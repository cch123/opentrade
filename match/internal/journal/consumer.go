package journal

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/match/internal/sequencer"
)

// ConsumerConfig configures the order-event Kafka consumer.
type ConsumerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "order-event"

	// InitialOffsets sets the per-partition starting offset (next-to-consume)
	// when restoring from snapshots (ADR-0048). Partitions missing from the
	// map default to AtStart so newly-added partitions are backfilled. Nil
	// means cold start: every partition begins at AtStart.
	InitialOffsets map[int32]int64
}

// OrderConsumer consumes order-event Kafka messages and dispatches them to
// per-symbol SymbolWorkers via Dispatcher.
type OrderConsumer struct {
	client     *kgo.Client
	dispatcher *Dispatcher
	logger     *zap.Logger
	topic      string
}

// NewOrderConsumer constructs an OrderConsumer. The caller must start it with
// Run and Close it on shutdown.
func NewOrderConsumer(cfg ConsumerConfig, d *Dispatcher, logger *zap.Logger) (*OrderConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers configured")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("journal: GroupID required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "order-event"
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
	return &OrderConsumer{
		client:     cli,
		dispatcher: d,
		logger:     logger,
		topic:      cfg.Topic,
	}, nil
}

// Run polls Kafka and dispatches events until ctx is cancelled or the client
// is closed. Per ADR-0048 the snapshot file is the authoritative consumer
// position, so we do NOT commit offsets back to the broker — Kafka's
// consumer group metadata is only used for partition assignment.
func (c *OrderConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("kafka fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(c.handleRecord)
	}
}

// Close shuts down the underlying Kafka client.
func (c *OrderConsumer) Close() { c.client.Close() }

func (c *OrderConsumer) handleRecord(rec *kgo.Record) {
	var pb eventpb.OrderEvent
	if err := proto.Unmarshal(rec.Value, &pb); err != nil {
		c.logger.Error("decode OrderEvent",
			zap.String("topic", rec.Topic), zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	evt, err := OrderEventToInternal(&pb, sequencer.SourceMeta{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
	})
	if err != nil {
		c.logger.Error("convert OrderEvent",
			zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	if evt == nil {
		// unknown payload kind — forward-compat skip
		return
	}
	if err := c.dispatcher.Dispatch(evt); err != nil {
		if errors.Is(err, ErrUnknownSymbol) {
			// Silent skip: this Match instance does not own this symbol.
			return
		}
		c.logger.Error("dispatch event", zap.Error(err),
			zap.String("symbol", evt.Symbol), zap.Int64("offset", rec.Offset))
	}
}
