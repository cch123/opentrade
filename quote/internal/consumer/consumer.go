// Package consumer drives the trade-event → engine → market-data pipeline.
//
// Quote keeps all per-symbol state (orderbook, kline bars) in memory. To make
// restarts deterministic we configure the consumer to rewind to the earliest
// trade-event offset and never commit offsets — every restart re-derives
// state from the full topic history. ADR-0025 captures this constraint and
// the future move to snapshots.
package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// Handler turns a trade-event into the market-data events to publish.
type Handler func(*eventpb.TradeEvent) []*eventpb.MarketDataEvent

// Publisher is the market-data producer contract (kept narrow for tests).
type Publisher interface {
	PublishBatch(ctx context.Context, evts []*eventpb.MarketDataEvent) error
}

// Config configures the trade-event consumer.
type Config struct {
	Brokers  []string
	ClientID string
	GroupID  string // used only for metadata; Quote does not commit offsets
	Topic    string // default "trade-event"
}

// TradeEventConsumer reads trade-event from the beginning on every start,
// dispatches to Handler, and publishes the result to Publisher.
type TradeEventConsumer struct {
	cli     *kgo.Client
	handler Handler
	pub     Publisher
	logger  *zap.Logger
	topic   string
}

// New constructs a consumer bound to group with its offset reset at the
// start of the topic. Quote does not commit offsets — see package doc.
func New(cfg Config, handler Handler, pub Publisher, logger *zap.Logger) (*TradeEventConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("consumer: at least one broker required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("consumer: GroupID required")
	}
	if handler == nil {
		return nil, errors.New("consumer: handler required")
	}
	if pub == nil {
		return nil, errors.New("consumer: publisher required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "trade-event"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &TradeEventConsumer{
		cli:     cli,
		handler: handler,
		pub:     pub,
		logger:  logger,
		topic:   cfg.Topic,
	}, nil
}

// Close closes the Kafka client.
func (c *TradeEventConsumer) Close() { c.cli.Close() }

// Run polls trade-event and publishes derived market-data until ctx is
// cancelled, the client is closed, or publish fails.
func (c *TradeEventConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})

		var records []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { records = append(records, rec) })
		if len(records) == 0 {
			continue
		}

		for _, rec := range records {
			var evt eventpb.TradeEvent
			if err := proto.Unmarshal(rec.Value, &evt); err != nil {
				c.logger.Error("decode trade-event",
					zap.String("topic", rec.Topic),
					zap.Int32("partition", rec.Partition),
					zap.Int64("offset", rec.Offset),
					zap.Error(err))
				continue
			}
			mdEvts := c.handler(&evt)
			if len(mdEvts) == 0 {
				continue
			}
			if err := c.pub.PublishBatch(ctx, mdEvts); err != nil {
				return fmt.Errorf("publish market-data: %w", err)
			}
		}
	}
}
