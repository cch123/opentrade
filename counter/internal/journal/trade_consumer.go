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

// TradeHandler is the Service-side callback invoked for each trade-event
// received from Kafka. HandleTradeRecord is the offset-aware variant used
// by the consumer; HandleTradeEvent is kept for in-process tests that
// don't care about per-partition offsets.
type TradeHandler interface {
	HandleTradeEvent(ctx context.Context, evt *eventpb.TradeEvent) error
	HandleTradeRecord(ctx context.Context, evt *eventpb.TradeEvent, partition int32, offset int64) error
}

// TradeConsumerConfig configures the trade-event Kafka consumer.
type TradeConsumerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "trade-event"

	// InitialOffsets seeds AdjustFetchOffsetsFn so the consumer resumes at
	// the snapshot's per-partition position (ADR-0048). Nil means cold
	// start: every partition begins AtStart.
	InitialOffsets map[int32]int64
}

// TradeConsumer reads Match's trade-event stream and dispatches each record
// to the Service for state mutation + counter-journal emission.
type TradeConsumer struct {
	cli     *kgo.Client
	handler TradeHandler
	logger  *zap.Logger
	topic   string
}

// NewTradeConsumer builds a consumer-group client in ReadCommitted mode. Per
// ADR-0048 the snapshot file is the authoritative consumer position, so we
// neither auto-commit nor manually commit back to the broker — Kafka's
// consumer group is used only for partition assignment.
func NewTradeConsumer(cfg TradeConsumerConfig, handler TradeHandler, logger *zap.Logger) (*TradeConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("journal: GroupID required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "trade-event"
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
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &TradeConsumer{cli: cli, handler: handler, logger: logger, topic: cfg.Topic}, nil
}

// Run polls Kafka and invokes the handler for each record until ctx is
// cancelled or the client is closed.
func (c *TradeConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			c.handleRecord(ctx, rec)
		})
	}
}

// Close shuts down the underlying client.
func (c *TradeConsumer) Close() { c.cli.Close() }

func (c *TradeConsumer) handleRecord(ctx context.Context, rec *kgo.Record) {
	var pb eventpb.TradeEvent
	if err := proto.Unmarshal(rec.Value, &pb); err != nil {
		c.logger.Error("decode trade-event",
			zap.String("topic", rec.Topic), zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	if err := c.handler.HandleTradeRecord(ctx, &pb, rec.Partition, rec.Offset); err != nil {
		c.logger.Error("handle trade-event",
			zap.Int64("offset", rec.Offset), zap.Error(err))
	}
}
