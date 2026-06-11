package consumer

// perpprice.go is the perp-price topic consumer (ADR-0078 §6): the second
// stateful price feed, carrying MarkTicks perp position-bound triggers fire
// off. Same group + AdjustFetchOffsetsFn pattern as the market-data
// consumer; the engine's snapshot perp offsets seed the resume position
// (ADR-0048), cold start consumes from topic end.

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// PerpPriceHandler applies one perp-price event at a partition / offset
// (engine.HandlePerpPriceRecord — it advances the engine's perp offset map
// under its own lock).
type PerpPriceHandler func(ctx context.Context, evt *eventpb.PerpPriceEvent, partition int32, offset int64)

// PerpPriceConsumer polls perp-price and invokes handler for each record.
type PerpPriceConsumer struct {
	cli     *kgo.Client
	handler PerpPriceHandler
	logger  *zap.Logger
	topic   string
}

// NewPerpPrice constructs the perp-price consumer. Config.Topic defaults to
// "perp-price"; empty InitialOffsets → AtEnd (cold start, mark is a level
// stream — history before the snapshot is already folded into state).
func NewPerpPrice(cfg Config, handler PerpPriceHandler, logger *zap.Logger) (*PerpPriceConsumer, error) {
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
		cfg.Topic = "perp-price"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	}
	if len(cfg.InitialOffsets) > 0 {
		opts = append(opts, adjustFetchOffsets(cfg.InitialOffsets))
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &PerpPriceConsumer{cli: cli, handler: handler, logger: logger, topic: cfg.Topic}, nil
}

// Close shuts down the underlying Kafka client.
func (c *PerpPriceConsumer) Close() { c.cli.Close() }

// Run polls perp-price until ctx is cancelled or the client is closed.
func (c *PerpPriceConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("trigger perp-price fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			var evt eventpb.PerpPriceEvent
			if err := proto.Unmarshal(rec.Value, &evt); err != nil {
				c.logger.Error("trigger decode perp-price",
					zap.Int32("partition", rec.Partition),
					zap.Int64("offset", rec.Offset),
					zap.Error(err))
				return
			}
			c.handler(ctx, &evt, rec.Partition, rec.Offset)
		})
	}
}

// adjustFetchOffsets seeds the group's fetch positions from saved offsets,
// falling back to AtEnd for unseen partitions (shared by both consumers).
func adjustFetchOffsets(saved map[int32]int64) kgo.Opt {
	return kgo.AdjustFetchOffsetsFn(func(_ context.Context, current map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
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
	})
}
