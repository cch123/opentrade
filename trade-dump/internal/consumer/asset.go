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

// AssetWriter is the narrow view the consumer needs. Real impl:
// writer.MySQL. Tests plug in a fake.
type AssetWriter interface {
	ApplyAssetBatch(ctx context.Context, batch writer.AssetBatch) error
}

// AssetConfig configures the asset-journal consumer (ADR-0057 M5).
type AssetConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "asset-journal"
}

// AssetConsumer projects AssetJournalEvent → MySQL rows via
// writer.AssetBatch. Delivery model mirrors JournalConsumer: write
// MySQL first, then commit Kafka offsets (ADR-0023). Idempotent upserts
// absorb replays on resume.
type AssetConsumer struct {
	cli    *kgo.Client
	w      AssetWriter
	logger *zap.Logger
	topic  string
}

// NewAsset builds an asset-journal consumer.
func NewAsset(cfg AssetConfig, w AssetWriter, logger *zap.Logger) (*AssetConsumer, error) {
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
		cfg.Topic = "asset-journal"
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
	return &AssetConsumer{cli: cli, w: w, logger: logger, topic: cfg.Topic}, nil
}

// Close releases the Kafka client.
func (c *AssetConsumer) Close() { c.cli.Close() }

// Run polls asset-journal, projects into MySQL, commits offsets. Same
// shape as JournalConsumer.Run.
func (c *AssetConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("asset-journal fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})

		var records []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { records = append(records, rec) })
		if len(records) == 0 {
			continue
		}

		events := decodeAssetBatch(records, c.logger)
		if len(events) > 0 {
			batch := writer.BuildAssetBatch(events)
			if !batch.IsEmpty() {
				if err := c.w.ApplyAssetBatch(ctx, batch); err != nil {
					return fmt.Errorf("apply asset batch: %w", err)
				}
			}
		}
		if err := c.cli.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}

// decodeAssetBatch unmarshals records, logging malformed ones. Their
// offsets still get committed — the pipeline never stalls on a single
// bad event.
func decodeAssetBatch(records []*kgo.Record, logger *zap.Logger) []*eventpb.AssetJournalEvent {
	out := make([]*eventpb.AssetJournalEvent, 0, len(records))
	for _, rec := range records {
		var evt eventpb.AssetJournalEvent
		if err := proto.Unmarshal(rec.Value, &evt); err != nil {
			logger.Error("decode asset-journal",
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
