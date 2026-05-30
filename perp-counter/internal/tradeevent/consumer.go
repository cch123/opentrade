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

// Handler is the perp-counter service sink for each decoded perp-trade-event
// record. Match owns this inbound stream; perp-counter consumes it to settle
// positions and advance order lifecycle state.
type Handler interface {
	HandleTradeEvent(evt *eventpb.TradeEvent, partition int32, offset int64)
}

// ConsumerConfig configures the perp-trade-event consumer. Match's perp
// deployment publishes fills/lifecycle events here, physically isolated from
// spot's trade-event topic (ADR-0068 §0).
type ConsumerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "perp-trade-event"

	// InitialOffsets seeds the per-partition resume position from a snapshot
	// (ADR-0048). Nil means cold start: every partition begins AtStart, with
	// position match_seq and terminal-order eviction absorbing replay.
	InitialOffsets map[int32]int64
}

// Consumer reads perp-trade-event and drives the service's settlement and
// order-lifecycle handlers. The snapshot is the authoritative position, so
// offsets are not committed back to the broker; the group is only used for
// partition assignment.
type Consumer struct {
	cli     *kgo.Client
	handler Handler
	logger  *zap.Logger
	topic   string
}

// NewConsumer builds a ReadCommitted consumer-group client. The single-instance
// MVP joins the group and receives every partition; later HA/sharding keeps the
// same API while changing assignment topology.
func NewConsumer(cfg ConsumerConfig, handler Handler, logger *zap.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("tradeevent: no brokers")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("tradeevent: GroupID required")
	}
	if handler == nil {
		return nil, errors.New("tradeevent: handler required")
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
	return &Consumer{cli: cli, handler: handler, logger: logger, topic: cfg.Topic}, nil
}

// Run polls and dispatches until ctx is cancelled or the client is closed.
func (c *Consumer) Run(ctx context.Context) error {
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
func (c *Consumer) Close() { c.cli.Close() }

func (c *Consumer) handleRecord(rec *kgo.Record) {
	var pb eventpb.TradeEvent
	if err := proto.Unmarshal(rec.Value, &pb); err != nil {
		c.logger.Error("decode perp trade-event",
			zap.String("topic", rec.Topic), zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	c.handler.HandleTradeEvent(&pb, rec.Partition, rec.Offset)
}
