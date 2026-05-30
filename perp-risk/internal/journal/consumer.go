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

type Handler interface {
	ApplyJournalEventAt(evt *eventpb.PerpJournalEvent, partition int32, offset int64) (bool, error)
}

type ConsumerConfig struct {
	Brokers        []string
	ClientID       string
	GroupID        string
	Topic          string
	InitialOffsets map[int32]int64
}

// Consumer folds perp-journal into the global risk coordinator. Broker commits
// are deliberately disabled: the coordinator snapshot, not Kafka committed
// offsets, is the recovery boundary, so a crash before snapshot replay starts
// from the last snapshotted offset and refolds the lost tail exactly once.
type Consumer struct {
	cli     *kgo.Client
	handler Handler
	logger  *zap.Logger
}

func NewConsumer(cfg ConsumerConfig, h Handler, logger *zap.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("perp-risk journal: brokers required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("perp-risk journal: group id required")
	}
	if h == nil {
		return nil, errors.New("perp-risk journal: handler required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "perp-journal"
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
	return &Consumer{cli: cli, handler: h, logger: logger}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var fetchErr error
		fetches.EachError(func(topic string, part int32, err error) {
			c.logger.Warn("perp-risk fetch error", zap.String("topic", topic), zap.Int32("partition", part), zap.Error(err))
			if fetchErr == nil {
				fetchErr = err
			}
		})
		if fetchErr != nil {
			return fetchErr
		}
		var handleErr error
		fetches.EachRecord(func(rec *kgo.Record) {
			if handleErr != nil {
				return
			}
			var evt eventpb.PerpJournalEvent
			if err := proto.Unmarshal(rec.Value, &evt); err != nil {
				handleErr = fmt.Errorf("decode perp-journal %s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
				return
			}
			if _, err := c.handler.ApplyJournalEventAt(&evt, rec.Partition, rec.Offset); err != nil {
				handleErr = fmt.Errorf("fold perp-journal %s[%d]@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
			}
		})
		if handleErr != nil {
			return handleErr
		}
	}
}

func (c *Consumer) Close() { c.cli.Close() }
