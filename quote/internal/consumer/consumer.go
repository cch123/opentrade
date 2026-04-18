// Package consumer drives the trade-event → engine → market-data pipeline.
//
// Quote keeps all per-symbol state (orderbook, kline bars) in memory.
// ADR-0036 pairs that state with a local snapshot file: on restart we load
// the snapshot, start the consumer from the saved per-partition offsets,
// and resume. When no snapshot is present (cold start) we fall back to the
// old "rescan from earliest" behaviour (ADR-0025).
//
// Offset bookkeeping: the engine tracks the next-to-consume offset per
// partition atomically with state mutation (engine.HandleRecord). The
// consumer does NOT commit to Kafka — the snapshot file is the source of
// truth for "how far we got."
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

// RecordHandler applies one trade-event at the given partition+offset and
// returns the market-data events to publish. The implementation (the
// engine) is responsible for serialising state and offset advance under
// its own lock.
type RecordHandler func(evt *eventpb.TradeEvent, partition int32, offset int64) []*eventpb.MarketDataEvent

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

	// InitialOffsets sets the per-partition starting offset (next-to-consume)
	// when restoring from a snapshot. Partitions missing from the map default
	// to AtStart so newly-added partitions are backfilled. Nil means cold
	// start: every partition begins at AtStart.
	InitialOffsets map[int32]int64
}

// TradeEventConsumer reads trade-event (from saved offsets or from the
// beginning), dispatches to the engine handler, and publishes the result.
type TradeEventConsumer struct {
	cli     *kgo.Client
	handler RecordHandler
	pub     Publisher
	logger  *zap.Logger
	topic   string
}

// New constructs a consumer bound to a group. When InitialOffsets is
// non-empty the consumer uses AdjustFetchOffsetsFn to start each partition
// at the saved offset, falling back to AtStart for any partition not
// present in the map (e.g. added after the snapshot was taken).
func New(cfg Config, handler RecordHandler, pub Publisher, logger *zap.Logger) (*TradeEventConsumer, error) {
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
						outParts[p] = kgo.NewOffset().AtStart()
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
			mdEvts := c.handler(&evt, rec.Partition, rec.Offset)
			if len(mdEvts) == 0 {
				continue
			}
			if err := c.pub.PublishBatch(ctx, mdEvts); err != nil {
				return fmt.Errorf("publish market-data: %w", err)
			}
		}
	}
}
