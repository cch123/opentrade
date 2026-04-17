package journal

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/xargin/opentrade/match/internal/sequencer"
)

// ProducerConfig configures the trade-event Kafka producer.
type ProducerConfig struct {
	Brokers    []string
	ClientID   string
	ProducerID string // stamped onto EventMeta.producer_id
	Topic      string // default "trade-event"
}

// TradeProducer publishes sequencer.Output emissions to the trade-event topic.
type TradeProducer struct {
	client *kgo.Client
	cfg    ProducerConfig
	logger *zap.Logger
}

// NewTradeProducer constructs a TradeProducer (idempotent, acks=all).
func NewTradeProducer(cfg ProducerConfig, logger *zap.Logger) (*TradeProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if cfg.ProducerID == "" {
		return nil, errors.New("journal: ProducerID required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "trade-event"
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ProducerLinger(0),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &TradeProducer{client: cli, cfg: cfg, logger: logger}, nil
}

// Publish serializes out and sends it synchronously. Returns the first error.
func (p *TradeProducer) Publish(ctx context.Context, out *sequencer.Output) error {
	te, err := OutputToTradeEvent(out, p.cfg.ProducerID)
	if err != nil {
		return fmt.Errorf("convert output: %w", err)
	}
	payload, err := proto.Marshal(te)
	if err != nil {
		return fmt.Errorf("marshal TradeEvent: %w", err)
	}
	rec := &kgo.Record{
		Topic: p.cfg.Topic,
		Key:   []byte(out.Symbol),
		Value: payload,
	}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

// Pump drains outbox and publishes each Output until ctx is cancelled or
// outbox is closed. Logs but does not return on individual publish errors
// (idempotent producer will retry via franz-go).
func (p *TradeProducer) Pump(ctx context.Context, outbox <-chan *sequencer.Output) {
	for {
		select {
		case <-ctx.Done():
			return
		case out, ok := <-outbox:
			if !ok {
				return
			}
			if err := p.Publish(ctx, out); err != nil {
				p.logger.Error("publish trade-event",
					zap.String("symbol", out.Symbol),
					zap.Uint64("seq_id", out.SeqID),
					zap.Uint8("kind", uint8(out.Kind)),
					zap.Error(err))
			}
		}
	}
}

// Close flushes and closes the underlying client.
func (p *TradeProducer) Close() { p.client.Close() }
