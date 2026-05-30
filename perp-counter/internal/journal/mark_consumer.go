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

// MarkPriceHandler is the service sink for decoded mark-price records. The
// service's HandleMarkPriceEvent satisfies it.
type MarkPriceHandler interface {
	HandleMarkPriceEvent(evt *eventpb.MarkPriceEvent)
}

// MarkPriceConsumerConfig configures the mark-price consumer (ADR-0068 §5).
type MarkPriceConsumerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "mark-price"
}

// MarkPriceConsumer reads the mark-price stream and drives mark updates +
// funding settlement. Unlike the trade-event consumer (snapshot-authoritative
// offset, ADR-0048), the mark stream carries no event-sourced state of its own:
// marks are last-writer-wins and funding is idempotent via funding_round_seen.
// So it uses a consumer group with commit-after-process — a restart resumes
// from the committed offset (no replay storm, no missed funding round) and the
// at-least-once overlap is absorbed by the funding watermark.
type MarkPriceConsumer struct {
	cli     *kgo.Client
	handler MarkPriceHandler
	logger  *zap.Logger
	topic   string
}

// NewMarkPriceConsumer builds a ReadCommitted consumer-group client that resets
// to the newest offset on a cold join (a brand-new perp-counter has no
// positions, so there is no past funding to settle).
func NewMarkPriceConsumer(cfg MarkPriceConsumerConfig, handler MarkPriceHandler, logger *zap.Logger) (*MarkPriceConsumer, error) {
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
		cfg.Topic = "mark-price"
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
	return &MarkPriceConsumer{cli: cli, handler: handler, logger: logger, topic: cfg.Topic}, nil
}

// Run polls, processes, then commits each batch until ctx is cancelled or the
// client closes. Committing only after processing means a crash mid-batch
// redelivers the uncommitted tail (idempotency-safe), never skipping a funding
// round.
func (c *MarkPriceConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("mark-price fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(c.handleRecord)
		if err := c.cli.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.logger.Warn("commit mark-price offsets", zap.Error(err))
		}
	}
}

// Close shuts down the client.
func (c *MarkPriceConsumer) Close() { c.cli.Close() }

func (c *MarkPriceConsumer) handleRecord(rec *kgo.Record) {
	var pb eventpb.MarkPriceEvent
	if err := proto.Unmarshal(rec.Value, &pb); err != nil {
		c.logger.Error("decode mark-price",
			zap.String("topic", rec.Topic), zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	c.handler.HandleMarkPriceEvent(&pb)
}
