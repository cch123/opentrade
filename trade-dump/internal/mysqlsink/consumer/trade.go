// Package consumer wires the trade-event Kafka topic to the MySQL writer.
//
// Delivery model (ADR-0008):
//  1. Poll a batch from Kafka.
//  2. Decode each record; keep Trade payloads, skip others.
//  3. Write the batch to MySQL in a single transaction (idempotent via
//     ON DUPLICATE KEY UPDATE).
//  4. Only after commit succeeds, commit Kafka offsets.
//
// If any step after (1) fails, Run returns — the supervisor restarts us and we
// resume from the last committed offset. Rewrites are safe because of the
// idempotent upsert.
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

// Config configures the trade-event consumer.
type Config struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "trade-event"
}

// TradeConsumer reads trade-event and projects Trade payloads to MySQL.
type TradeConsumer struct {
	cli    *kgo.Client
	w      writer.TradeWriter
	logger *zap.Logger
	topic  string
}

// New constructs a consumer-group client in ReadCommitted + manual commit
// mode. ReadCommitted matches ADR-0005 — we only want committed writes from
// Match's transactional producer (future EOS upgrade).
func New(cfg Config, w writer.TradeWriter, logger *zap.Logger) (*TradeConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("consumer: at least one broker required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("consumer: GroupID required")
	}
	if w == nil {
		return nil, errors.New("consumer: writer required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "trade-event"
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
	return &TradeConsumer{cli: cli, w: w, logger: logger, topic: cfg.Topic}, nil
}

// Close closes the Kafka client.
func (c *TradeConsumer) Close() { c.cli.Close() }

// Run drives the consume → write → commit loop until ctx is cancelled, the
// client is closed, or a write / commit error is encountered. Returns nil on
// clean shutdown.
func (c *TradeConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})

		var records []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { records = append(records, rec) })
		if len(records) == 0 {
			continue
		}

		rows := decodeBatch(records, c.logger)
		if len(rows) > 0 {
			if err := c.w.InsertTrades(ctx, rows); err != nil {
				return fmt.Errorf("insert trades: %w", err)
			}
		}
		if err := c.cli.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}

// decodeBatch unmarshals records and keeps only Trade payloads. Malformed or
// non-Trade records are skipped — their Kafka offsets will still be committed
// so we don't replay them forever.
func decodeBatch(records []*kgo.Record, logger *zap.Logger) []writer.TradeRow {
	rows := make([]writer.TradeRow, 0, len(records))
	for _, rec := range records {
		var evt eventpb.TradeEvent
		if err := proto.Unmarshal(rec.Value, &evt); err != nil {
			logger.Error("decode trade-event",
				zap.String("topic", rec.Topic),
				zap.Int32("partition", rec.Partition),
				zap.Int64("offset", rec.Offset),
				zap.Error(err))
			continue
		}
		row, ok := writer.TradeRowFromEvent(&evt)
		if !ok {
			continue
		}
		rows = append(rows, row)
	}
	return rows
}
