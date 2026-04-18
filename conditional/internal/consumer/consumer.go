// Package consumer drives the market-data → engine pipeline for
// conditional orders. It uses the same group + AdjustFetchOffsetsFn
// pattern as Quote (ADR-0036): on restart the engine's saved offsets
// become the starting point, otherwise the consumer starts at topic
// end (we never need history to match new trades).
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

// RecordHandler applies one market-data event at a specific partition /
// offset. The implementation (engine.HandleRecord) is responsible for
// advancing its offset bookkeeping under its own lock so that Capture
// sees a state/offset pair that are consistent with each other.
type RecordHandler func(ctx context.Context, evt *eventpb.MarketDataEvent, partition int32, offset int64)

// Config wires the consumer.
type Config struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "market-data"

	// InitialOffsets seeds AdjustFetchOffsetsFn so the consumer resumes
	// from the engine's snapshot offsets. Nil → AtEnd (fresh start).
	InitialOffsets map[int32]int64
}

// Consumer polls market-data and invokes handler for each record.
type Consumer struct {
	cli     *kgo.Client
	handler RecordHandler
	logger  *zap.Logger
	topic   string
}

// New constructs a consumer. An empty InitialOffsets map is treated as
// "cold start" → consume from topic end (we don't care about historical
// prints for new triggers).
func New(cfg Config, handler RecordHandler, logger *zap.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("consumer: brokers required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("consumer: group id required")
	}
	if handler == nil {
		return nil, errors.New("consumer: handler required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "market-data"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	}
	if len(cfg.InitialOffsets) > 0 {
		saved := cfg.InitialOffsets
		opts = append(opts, kgo.AdjustFetchOffsetsFn(func(_ context.Context, current map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			out := make(map[string]map[int32]kgo.Offset, len(current))
			for topic, parts := range current {
				outParts := make(map[int32]kgo.Offset, len(parts))
				for p := range parts {
					if off, ok := saved[p]; ok {
						outParts[p] = kgo.NewOffset().At(off)
					} else {
						outParts[p] = kgo.NewOffset().AtEnd()
					}
				}
				out[topic] = outParts
			}
			return out, nil
		}))
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Consumer{cli: cli, handler: handler, logger: logger, topic: cfg.Topic}, nil
}

// Close shuts down the underlying Kafka client.
func (c *Consumer) Close() { c.cli.Close() }

// Run polls market-data until ctx is cancelled or the client is closed.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("conditional market-data fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			var evt eventpb.MarketDataEvent
			if err := proto.Unmarshal(rec.Value, &evt); err != nil {
				c.logger.Error("conditional decode market-data",
					zap.Int32("partition", rec.Partition),
					zap.Int64("offset", rec.Offset),
					zap.Error(err))
				return
			}
			c.handler(ctx, &evt, rec.Partition, rec.Offset)
		})
	}
}
