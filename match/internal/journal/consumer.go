package journal

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/match/internal/sequencer"
)

// ConsumerConfig configures the order-event Kafka consumer.
type ConsumerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "order-event"
}

// OrderConsumer consumes order-event Kafka messages and dispatches them to
// per-symbol SymbolWorkers via Dispatcher.
type OrderConsumer struct {
	client     *kgo.Client
	dispatcher *Dispatcher
	logger     *zap.Logger
	topic      string
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
	if cfg.Topic == "" {
		cfg.Topic = "order-event"
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &OrderConsumer{
		client:     cli,
		dispatcher: d,
		logger:     logger,
		topic:      cfg.Topic,
	}, nil
}

// Run polls Kafka and dispatches events until ctx is cancelled or the client
// is closed.
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
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.logger.Error("commit offsets", zap.Error(err))
		}
	}
}

// Close shuts down the underlying Kafka client.
func (c *OrderConsumer) Close() { c.client.Close() }

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
			// Silent skip: this Match instance does not own this symbol.
			return
		}
		c.logger.Error("dispatch event", zap.Error(err),
			zap.String("symbol", evt.Symbol), zap.Int64("offset", rec.Offset))
	}
}
