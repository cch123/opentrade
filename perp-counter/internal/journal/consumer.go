package journal

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// TradeHandler is the service-side sink for each decoded perp-trade-event
// record. The service's HandleTradeEvent satisfies it structurally.
type TradeHandler interface {
	HandleTradeEvent(evt *eventpb.TradeEvent, partition int32, offset int64)
}

// TradeConsumerConfig configures the perp-trade-event consumer. Match's perp
// deployment publishes fills/lifecycle events here (separate from the spot
// `trade-event` topic — perp is physically isolated, ADR-0068 §0).
type TradeConsumerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "perp-trade-event"

	// InitialOffsets seeds the per-partition resume position from a snapshot
	// (ADR-0048). Nil → cold start (every partition AtStart; the position
	// match_seq watermark + terminal-order eviction make replay idempotent).
	InitialOffsets map[int32]int64
}

// TradeConsumer reads perp-trade-event and drives the service's settlement +
// order-lifecycle handlers. Like counter (ADR-0048), the snapshot is the
// authoritative position, so offsets are never committed back to the broker —
// the consumer group is used only for partition assignment.
type TradeConsumer struct {
	cli     *kgo.Client
	handler TradeHandler
	logger  *zap.Logger
	topic   string
}

// NewTradeConsumer builds a ReadCommitted consumer-group client. A single
// perp-counter MVP instance joins the group and is assigned every partition, so
// all perp users settle locally; multi-instance user sharding is the HA
// milestone (ADR-0068).
func NewTradeConsumer(cfg TradeConsumerConfig, handler TradeHandler, logger *zap.Logger) (*TradeConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("journal: GroupID required")
	}
	if handler == nil {
		return nil, errors.New("journal: handler required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "perp-trade-event"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
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
				for part, cur := range parts {
					if off, ok := saved[part]; ok {
						outParts[part] = kgo.NewOffset().At(off)
					} else {
						outParts[part] = cur
					}
				}
				out[topic] = outParts
			}
			return out, nil
		}))
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &TradeConsumer{cli: cli, handler: handler, logger: logger, topic: cfg.Topic}, nil
}

// Run polls and dispatches until ctx is cancelled or the client is closed.
func (c *TradeConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(t string, part int32, err error) {
			c.logger.Warn("fetch error",
				zap.String("topic", t), zap.Int32("partition", part), zap.Error(err))
		})
		fetches.EachRecord(c.handleRecord)
	}
}

// Close shuts down the underlying client.
func (c *TradeConsumer) Close() { c.cli.Close() }

func (c *TradeConsumer) handleRecord(rec *kgo.Record) {
	var pb eventpb.TradeEvent
	if err := proto.Unmarshal(rec.Value, &pb); err != nil {
		c.logger.Error("decode perp trade-event",
			zap.String("topic", rec.Topic), zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	c.handler.HandleTradeEvent(&pb, rec.Partition, rec.Offset)
}
