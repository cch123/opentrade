package tradeevent

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// Handler is the Counter-side callback invoked for each trade-event received
// from Kafka. The interface is owned by this inbound package because Match is
// the producer; Counter only ingests and applies the event stream.
type Handler interface {
	HandleTradeRecord(ctx context.Context, evt *eventpb.TradeEvent, partition int32, offset int64) error
}

// ConsumerConfig configures a consumer-group reader for Match's trade-event
// topic. The group mode is kept for legacy tests and tooling; production
// vshard workers use PartitionConsumerConfig for explicit ownership.
type ConsumerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "trade-event"

	// InitialOffsets seeds AdjustFetchOffsetsFn so the consumer resumes at
	// the snapshot's per-partition position (ADR-0048). Nil means cold start:
	// every assigned partition begins AtStart.
	InitialOffsets map[int32]int64
}

// Consumer reads Match's trade-event stream and dispatches each record to
// Counter's service layer for state mutation and counter-journal emission.
type Consumer struct {
	cli     *kgo.Client
	handler Handler
	logger  *zap.Logger
	topic   string
}

// NewConsumer builds a consumer-group client in ReadCommitted mode. The
// snapshot remains the authoritative consumer position, so this reader does
// not auto-commit or manually commit offsets back to Kafka.
func NewConsumer(cfg ConsumerConfig, handler Handler, logger *zap.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("tradeevent: no brokers")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("tradeevent: GroupID required")
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
	return &Consumer{cli: cli, handler: handler, logger: logger, topic: cfg.Topic}, nil
}

// PartitionConsumerConfig configures a reader that directly consumes explicit
// trade-event partitions without joining a Kafka consumer group.
type PartitionConsumerConfig struct {
	Brokers  []string
	ClientID string
	Topic    string // default "trade-event"

	// Partitions is the set this consumer will subscribe to. Each resolves
	// its starting offset from Offsets; absent partitions fall back to AtStart
	// on cold paths, where Counter's idempotency guards still apply.
	Partitions []int32

	// Offsets maps partition to the next-to-consume offset persisted in the
	// vshard snapshot.
	Offsets map[int32]int64
}

// NewPartitionConsumer builds the ADR-0058 vshard reader. Each spot Counter
// vshard owns exactly one trade-event partition, so direct assignment makes
// ownership explicit and keeps Kafka groups out of the fencing story.
func NewPartitionConsumer(cfg PartitionConsumerConfig, handler Handler, logger *zap.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("tradeevent: no brokers")
	}
	if len(cfg.Partitions) == 0 {
		return nil, errors.New("tradeevent: no partitions to assign")
	}
	if cfg.Topic == "" {
		cfg.Topic = "trade-event"
	}
	partitionOffsets := make(map[int32]kgo.Offset, len(cfg.Partitions))
	for _, p := range cfg.Partitions {
		if off, ok := cfg.Offsets[p]; ok {
			partitionOffsets[p] = kgo.NewOffset().At(off)
		} else {
			partitionOffsets[p] = kgo.NewOffset().AtStart()
		}
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			cfg.Topic: partitionOffsets,
		}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Consumer{cli: cli, handler: handler, logger: logger, topic: cfg.Topic}, nil
}

// Run polls Kafka and invokes the handler for each record until ctx is
// cancelled or the client is closed.
func (c *Consumer) Run(ctx context.Context) error {
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
func (c *Consumer) Close() { c.cli.Close() }

func (c *Consumer) handleRecord(ctx context.Context, rec *kgo.Record) {
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
