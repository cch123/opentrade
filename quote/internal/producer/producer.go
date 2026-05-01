// Package producer publishes MarketDataEvent messages to the market-data
// Kafka topic. Partition key is MarketDataEvent.symbol (aligned with
// trade-event so a given symbol stays on a single partition for ordering).
package producer

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// Config configures the market-data producer.
type Config struct {
	Brokers  []string
	ClientID string
	Topic    string // default "market-data"
}

// MarketDataProducer is a thin idempotent producer over the market-data topic.
type MarketDataProducer struct {
	client *kgo.Client
	topic  string
	logger *zap.Logger
}

// New constructs a MarketDataProducer.
func New(cfg Config, logger *zap.Logger) (*MarketDataProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("producer: at least one broker required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "market-data"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(0),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &MarketDataProducer{client: cli, topic: cfg.Topic, logger: logger}, nil
}

// Close flushes and closes the underlying client.
func (p *MarketDataProducer) Close() { p.client.Close() }

// Publish synchronously sends one MarketDataEvent. Returns the first error.
func (p *MarketDataProducer) Publish(ctx context.Context, evt *eventpb.MarketDataEvent) error {
	return p.PublishBatch(ctx, []*eventpb.MarketDataEvent{evt})
}

func (p *MarketDataProducer) encodeRecord(evt *eventpb.MarketDataEvent) (*kgo.Record, error) {
	if evt == nil {
		return nil, errors.New("producer: nil event")
	}
	payload, err := proto.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal market-data: %w", err)
	}
	return &kgo.Record{
		Topic: p.topic,
		Key:   []byte(evt.Symbol),
		Value: payload,
	}, nil
}

// PublishBatch sends the whole batch through one ProduceSync call. The events
// remain independent Kafka records, but a single sync point lets franz-go fill
// its produce request more efficiently than one ack wait per market-data frame.
// Callers that need strict all-or-nothing semantics should use a Kafka
// transaction (Quote's market-data projection does not require one).
func (p *MarketDataProducer) PublishBatch(ctx context.Context, evts []*eventpb.MarketDataEvent) error {
	if len(evts) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(evts))
	for _, evt := range evts {
		rec, err := p.encodeRecord(evt)
		if err != nil {
			return err
		}
		recs = append(recs, rec)
	}
	if err := p.client.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("produce market-data: %w", err)
	}
	return nil
}
