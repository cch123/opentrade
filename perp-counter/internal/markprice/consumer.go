package markprice

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// Handler is the perp-counter service sink for decoded perp-price records. The
// markprice service owns this stream; perp-counter consumes it for mark updates,
// funding, and liquidation scans.
type Handler interface {
	HandleMarkPriceEvent(evt *eventpb.MarkPriceEvent)
}

// ConsumerConfig configures the perp-price consumer (ADR-0068 §5).
type ConsumerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "perp-price"
}

// Consumer reads the perp-price stream and drives mark updates plus funding
// settlement. Unlike perp-trade-event, marks are not the source of snapshot
// replay state: marks are last-writer-wins and funding is guarded by
// funding_round_seen. This reader commits after processing so restart resumes
// from the broker's committed offset while at-least-once overlap remains safe.
type Consumer struct {
	cli     *kgo.Client
	handler Handler
	logger  *zap.Logger
	topic   string
}

// NewConsumer builds a ReadCommitted consumer-group client that resets to the
// newest offset on a cold join. A brand-new perp-counter has no positions, so
// there is no historical funding to settle.
func NewConsumer(cfg ConsumerConfig, handler Handler, logger *zap.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("markprice: no brokers")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("markprice: GroupID required")
	}
	if handler == nil {
		return nil, errors.New("markprice: handler required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "perp-price"
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Consumer{cli: cli, handler: handler, logger: logger, topic: cfg.Topic}, nil
}

// Run polls, processes, then commits each batch until ctx is cancelled or the
// client closes. Committing only after processing means a crash mid-batch
// redelivers the uncommitted tail and never skips a funding round.
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
			c.logger.Warn("perp-price fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(c.handleRecord)
		if err := c.cli.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.logger.Warn("commit perp-price offsets", zap.Error(err))
		}
	}
}

// Close shuts down the client.
func (c *Consumer) Close() { c.cli.Close() }

func (c *Consumer) handleRecord(rec *kgo.Record) {
	var pb eventpb.MarkPriceEvent
	if err := proto.Unmarshal(rec.Value, &pb); err != nil {
		c.logger.Error("decode perp-price",
			zap.String("topic", rec.Topic), zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	c.handler.HandleMarkPriceEvent(&pb)
}
