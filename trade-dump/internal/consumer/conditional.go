package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/trade-dump/internal/writer"
)

// ConditionalWriter is the narrow view the conditional consumer needs
// against MySQL. Real impl: writer.MySQL. Tests substitute a fake.
type ConditionalWriter interface {
	ApplyConditionalBatch(ctx context.Context, batch writer.ConditionalBatch) error
}

// ConditionalConfig configures the conditional-event consumer (ADR-0047).
type ConditionalConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "conditional-event"
}

// ConditionalConsumer projects ConditionalUpdate events into MySQL.
// Same commit-order discipline as JournalConsumer: MySQL first, then
// commit Kafka offsets (ADR-0023).
type ConditionalConsumer struct {
	cli    *kgo.Client
	w      ConditionalWriter
	logger *zap.Logger
	topic  string
}

// NewConditional builds a conditional-event consumer.
func NewConditional(cfg ConditionalConfig, w ConditionalWriter, logger *zap.Logger) (*ConditionalConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("consumer: brokers required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("consumer: group id required")
	}
	if w == nil {
		return nil, errors.New("consumer: writer required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "conditional-event"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &ConditionalConsumer{cli: cli, w: w, logger: logger, topic: cfg.Topic}, nil
}

// Close releases the Kafka client.
func (c *ConditionalConsumer) Close() { c.cli.Close() }

// Run polls conditional-event, projects into MySQL, and commits offsets.
func (c *ConditionalConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("conditional-event fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})

		var records []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { records = append(records, rec) })
		if len(records) == 0 {
			continue
		}

		events := decodeConditionalBatch(records, c.logger)
		if len(events) > 0 {
			batch := writer.BuildConditionalBatch(events)
			if !batch.IsEmpty() {
				if err := c.w.ApplyConditionalBatch(ctx, batch); err != nil {
					return fmt.Errorf("apply conditional batch: %w", err)
				}
			}
		}
		if err := c.cli.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}

func decodeConditionalBatch(records []*kgo.Record, logger *zap.Logger) []*eventpb.ConditionalUpdate {
	out := make([]*eventpb.ConditionalUpdate, 0, len(records))
	for _, rec := range records {
		var evt eventpb.ConditionalUpdate
		if err := proto.Unmarshal(rec.Value, &evt); err != nil {
			logger.Error("decode conditional-event",
				zap.String("topic", rec.Topic),
				zap.Int32("partition", rec.Partition),
				zap.Int64("offset", rec.Offset),
				zap.Error(err))
			continue
		}
		out = append(out, &evt)
	}
	return out
}
