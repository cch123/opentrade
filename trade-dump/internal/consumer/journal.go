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

// JournalWriter is the narrow view the consumer needs against MySQL. Real
// impl: writer.MySQL. Tests can plug in a fake.
type JournalWriter interface {
	ApplyJournalBatch(ctx context.Context, batch writer.JournalBatch) error
}

// JournalConfig configures the counter-journal consumer.
type JournalConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "counter-journal"
}

// JournalConsumer projects CounterJournalEvents into MySQL rows.
//
// Delivery model matches TradeConsumer (ADR-0023): write MySQL first, then
// commit Kafka offsets. Errors return from Run so the supervisor restarts
// us from the last committed offset; idempotent upserts absorb the replay.
type JournalConsumer struct {
	cli    *kgo.Client
	w      JournalWriter
	logger *zap.Logger
	topic  string
}

// NewJournal builds a counter-journal consumer.
func NewJournal(cfg JournalConfig, w JournalWriter, logger *zap.Logger) (*JournalConsumer, error) {
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
		cfg.Topic = "counter-journal"
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
	return &JournalConsumer{cli: cli, w: w, logger: logger, topic: cfg.Topic}, nil
}

// Close releases the Kafka client.
func (c *JournalConsumer) Close() { c.cli.Close() }

// Run polls counter-journal, projects into MySQL, and commits offsets. See
// ADR-0023 for the commit ordering rationale.
func (c *JournalConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("counter-journal fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})

		var records []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { records = append(records, rec) })
		if len(records) == 0 {
			continue
		}

		events := decodeJournalBatch(records, c.logger)
		if len(events) > 0 {
			batch := writer.BuildJournalBatch(events)
			if !batch.IsEmpty() {
				if err := c.w.ApplyJournalBatch(ctx, batch); err != nil {
					return fmt.Errorf("apply journal batch: %w", err)
				}
			}
		}
		if err := c.cli.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}

// decodeJournalBatch unmarshals records, logging malformed ones. Their
// offsets still get committed — we'd rather lose one bad event than stall
// the whole pipeline.
func decodeJournalBatch(records []*kgo.Record, logger *zap.Logger) []*eventpb.CounterJournalEvent {
	out := make([]*eventpb.CounterJournalEvent, 0, len(records))
	for _, rec := range records {
		var evt eventpb.CounterJournalEvent
		if err := proto.Unmarshal(rec.Value, &evt); err != nil {
			logger.Error("decode counter-journal",
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
