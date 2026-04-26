package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/trade-dump/internal/mysqlsink/writer"
)

// TriggerWriter is the narrow view the trigger consumer needs
// against MySQL. Real impl: writer.MySQL. Tests substitute a fake.
type TriggerWriter interface {
	ApplyTriggerBatch(ctx context.Context, batch writer.TriggerBatch) error
}

// TriggerConfig configures the trigger-event consumer (ADR-0047).
type TriggerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "trigger-event"
}

// TriggerConsumer projects TriggerUpdate events into MySQL.
// Same commit-order discipline as JournalConsumer: MySQL first, then
// commit Kafka offsets (ADR-0023).
type TriggerConsumer struct {
	cli    *kgo.Client
	w      TriggerWriter
	logger *zap.Logger
	topic  string
}

// NewTrigger builds a trigger-event consumer.
func NewTrigger(cfg TriggerConfig, w TriggerWriter, logger *zap.Logger) (*TriggerConsumer, error) {
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
		cfg.Topic = "trigger-event"
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
	return &TriggerConsumer{cli: cli, w: w, logger: logger, topic: cfg.Topic}, nil
}

// Close releases the Kafka client.
func (c *TriggerConsumer) Close() { c.cli.Close() }

// Run polls trigger-event, projects into MySQL, and commits offsets.
func (c *TriggerConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("trigger-event fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})

		var records []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { records = append(records, rec) })
		if len(records) == 0 {
			continue
		}

		events := decodeTriggerBatch(records, c.logger)
		if len(events) > 0 {
			batch := writer.BuildTriggerBatch(events)
			if !batch.IsEmpty() {
				if err := c.w.ApplyTriggerBatch(ctx, batch); err != nil {
					return fmt.Errorf("apply trigger batch: %w", err)
				}
			}
		}
		if err := c.cli.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}

func decodeTriggerBatch(records []*kgo.Record, logger *zap.Logger) []*eventpb.TriggerUpdate {
	out := make([]*eventpb.TriggerUpdate, 0, len(records))
	for _, rec := range records {
		var envelope eventpb.TriggerEvent
		if err := proto.Unmarshal(rec.Value, &envelope); err != nil {
			logger.Error("decode trigger-event envelope",
				zap.String("topic", rec.Topic),
				zap.Int32("partition", rec.Partition),
				zap.Int64("offset", rec.Offset),
				zap.Error(err))
			continue
		}
		// MySQL projection only cares about TriggerUpdate; checkpoint
		// events are ADR-0067 metadata for trade-dump's shadow
		// pipeline, not for this consumer.
		u := envelope.GetUpdate()
		if u == nil {
			continue
		}
		out = append(out, u)
	}
	return out
}
