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

// PerpJournalWriter is the narrow MySQL view the perp consumer needs. Real impl:
// writer.MySQL.ApplyPerpBatch. Tests fake it.
type PerpJournalWriter interface {
	ApplyPerpBatch(ctx context.Context, batch writer.PerpBatch) error
}

// PerpJournalConfig configures the perp-journal consumer (ADR-0068 M7).
type PerpJournalConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "perp-journal"
}

// PerpJournalConsumer projects PerpJournalEvents into the perp MySQL tables.
// Same delivery model as JournalConsumer (ADR-0023): write MySQL, then commit
// offsets; idempotent upserts absorb replays after a restart.
type PerpJournalConsumer struct {
	cli    *kgo.Client
	w      PerpJournalWriter
	logger *zap.Logger
	topic  string
}

// NewPerpJournal builds a perp-journal consumer.
func NewPerpJournal(cfg PerpJournalConfig, w PerpJournalWriter, logger *zap.Logger) (*PerpJournalConsumer, error) {
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
		cfg.Topic = "perp-journal"
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
	return &PerpJournalConsumer{cli: cli, w: w, logger: logger, topic: cfg.Topic}, nil
}

// Close releases the Kafka client.
func (c *PerpJournalConsumer) Close() { c.cli.Close() }

// Run polls perp-journal, projects into MySQL, and commits offsets.
func (c *PerpJournalConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("perp-journal fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})

		var events []*eventpb.PerpJournalEvent
		fetches.EachRecord(func(rec *kgo.Record) {
			var evt eventpb.PerpJournalEvent
			if err := proto.Unmarshal(rec.Value, &evt); err != nil {
				c.logger.Error("decode perp-journal",
					zap.Int32("partition", rec.Partition), zap.Int64("offset", rec.Offset), zap.Error(err))
				return
			}
			events = append(events, &evt)
		})

		if len(events) > 0 {
			batch := writer.BuildPerpBatch(events)
			if !batch.IsEmpty() {
				if err := c.w.ApplyPerpBatch(ctx, batch); err != nil {
					return fmt.Errorf("apply perp batch: %w", err)
				}
			}
		}
		if err := c.cli.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}
