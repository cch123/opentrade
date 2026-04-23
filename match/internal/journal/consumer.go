package journal

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/match/internal/sequencer"
)

// ConsumerConfig configures the order-event Kafka consumer.
//
// ADR-0050: production subscribes to per-symbol topics (`order-event-<sym>`)
// via TopicRegex; legacy single-topic mode is still supported by leaving
// TopicRegex empty and setting Topic.
type ConsumerConfig struct {
	Brokers           []string
	ClientID          string
	GroupID           string
	Topic             string   // legacy single topic (used when TopicRegex and Topics are empty)
	TopicRegex        string   // ADR-0050 fallback; e.g. `^order-event-.+$`
	Topics            []string // explicit owned topic set; takes precedence over TopicRegex/Topic
	UseExplicitTopics bool     // true allows Topics to be empty at startup

	// InitialOffsets sets the per-(topic, partition) starting offset when
	// restoring from snapshots (ADR-0048 + ADR-0050). Missing entries
	// keep Kafka's current assignment offset; on cold start that falls back to
	// ConsumeResetOffset(AtStart). Nil means cold start.
	InitialOffsets map[string]map[int32]int64
}

type offsetStore struct {
	mu      sync.RWMutex
	offsets map[string]map[int32]int64
}

func newOffsetStore(initial map[string]map[int32]int64) *offsetStore {
	return &offsetStore{offsets: cloneInitialOffsets(initial)}
}

func cloneInitialOffsets(initial map[string]map[int32]int64) map[string]map[int32]int64 {
	if len(initial) == 0 {
		return nil
	}
	out := make(map[string]map[int32]int64, len(initial))
	for topic, parts := range initial {
		if len(parts) == 0 {
			continue
		}
		outParts := make(map[int32]int64, len(parts))
		for p, off := range parts {
			outParts[p] = off
		}
		out[topic] = outParts
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *offsetStore) merge(offsets map[string]map[int32]int64) {
	if len(offsets) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.offsets == nil {
		s.offsets = make(map[string]map[int32]int64, len(offsets))
	}
	for topic, parts := range offsets {
		if len(parts) == 0 {
			continue
		}
		dst := s.offsets[topic]
		if dst == nil {
			dst = make(map[int32]int64, len(parts))
			s.offsets[topic] = dst
		}
		for p, off := range parts {
			dst[p] = off
		}
	}
}

func (s *offsetStore) adjust(current map[string]map[int32]kgo.Offset) map[string]map[int32]kgo.Offset {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]map[int32]kgo.Offset, len(current))
	for topic, parts := range current {
		outParts := make(map[int32]kgo.Offset, len(parts))
		topicSaved := s.offsets[topic]
		for p, cur := range parts {
			if off, ok := topicSaved[p]; ok {
				outParts[p] = kgo.NewOffset().At(off)
			} else {
				outParts[p] = cur
			}
		}
		out[topic] = outParts
	}
	return out
}

// OrderConsumer consumes order-event Kafka messages and dispatches them to
// per-symbol SymbolWorkers via Dispatcher.
type OrderConsumer struct {
	client     *kgo.Client
	dispatcher *Dispatcher
	logger     *zap.Logger
	// topic / topicRegex recorded for diagnostic logging only.
	topic      string
	topicRegex string
	topics     []string
	offsets    *offsetStore
}

// NewOrderConsumer constructs an OrderConsumer. The caller must start it with
// Run and Close it on shutdown.
func NewOrderConsumer(cfg ConsumerConfig, d *Dispatcher, logger *zap.Logger) (*OrderConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers configured")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("journal: GroupID required")
	}
	if cfg.Topic == "" && cfg.TopicRegex == "" && len(cfg.Topics) == 0 && !cfg.UseExplicitTopics {
		cfg.Topic = "order-event"
	}
	offsets := newOffsetStore(cfg.InitialOffsets)
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	switch {
	case cfg.UseExplicitTopics:
		if len(cfg.Topics) > 0 {
			opts = append(opts, kgo.ConsumeTopics(cfg.Topics...))
		}
	case len(cfg.Topics) > 0:
		opts = append(opts, kgo.ConsumeTopics(cfg.Topics...))
	case cfg.TopicRegex != "":
		// ADR-0050: subscribe by regex to pick up every per-symbol topic
		// including ones created after the consumer starts.
		opts = append(opts,
			kgo.ConsumeTopics(cfg.TopicRegex),
			kgo.ConsumeRegex(),
		)
	case cfg.Topic != "":
		opts = append(opts, kgo.ConsumeTopics(cfg.Topic))
	}
	opts = append(opts, kgo.AdjustFetchOffsetsFn(func(_ context.Context, current map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
		return offsets.adjust(current), nil
	}))
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &OrderConsumer{
		client:     cli,
		dispatcher: d,
		logger:     logger,
		topic:      cfg.Topic,
		topicRegex: cfg.TopicRegex,
		topics:     append([]string(nil), cfg.Topics...),
		offsets:    offsets,
	}, nil
}

// Run polls Kafka and dispatches events until ctx is cancelled or the client
// is closed. Per ADR-0048 the snapshot file is the authoritative consumer
// position, so we do NOT commit offsets back to the broker — Kafka's
// consumer group metadata is only used for partition assignment.
func (c *OrderConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("kafka fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(c.handleRecord)
	}
}

// Close shuts down the underlying Kafka client.
func (c *OrderConsumer) Close() { c.client.Close() }

// AddTopics adds owned per-symbol topics at runtime. No-op in regex mode.
func (c *OrderConsumer) AddTopics(topics ...string) {
	if c.topicRegex != "" || len(topics) == 0 {
		return
	}
	c.client.AddConsumeTopics(topics...)
}

// AddTopicsWithOffsets adds topics after first teaching the next group rebalance
// where to start them. This is used when etcd migrates a symbol in after its
// worker has restored from a snapshot.
func (c *OrderConsumer) AddTopicsWithOffsets(offsets map[string]map[int32]int64, topics ...string) {
	if c.topicRegex != "" || len(topics) == 0 {
		return
	}
	c.offsets.merge(offsets)
	c.client.AddConsumeTopics(topics...)
}

// RemoveTopics stops consuming topics that this instance no longer owns. No-op
// in regex mode.
func (c *OrderConsumer) RemoveTopics(topics ...string) {
	if c.topicRegex != "" || len(topics) == 0 {
		return
	}
	c.client.PurgeTopicsFromClient(topics...)
}

func (c *OrderConsumer) handleRecord(rec *kgo.Record) {
	var pb eventpb.OrderEvent
	if err := proto.Unmarshal(rec.Value, &pb); err != nil {
		c.logger.Error("decode OrderEvent",
			zap.String("topic", rec.Topic), zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	evt, err := OrderEventToInternal(&pb, sequencer.SourceMeta{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
	})
	if err != nil {
		c.logger.Error("convert OrderEvent",
			zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	if evt == nil {
		// unknown payload kind — forward-compat skip
		return
	}
	if err := c.dispatcher.Dispatch(evt); err != nil {
		if errors.Is(err, ErrUnknownSymbol) {
			c.logger.Warn("order-event for unowned symbol",
				zap.String("topic", rec.Topic),
				zap.Int32("partition", rec.Partition),
				zap.Int64("offset", rec.Offset),
				zap.String("symbol", evt.Symbol))
			return
		}
		c.logger.Error("dispatch event", zap.Error(err),
			zap.String("symbol", evt.Symbol), zap.Int64("offset", rec.Offset))
	}
}
