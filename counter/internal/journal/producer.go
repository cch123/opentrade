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

// ProducerConfig configures the counter-journal Kafka producer.
type ProducerConfig struct {
	Brokers    []string
	ClientID   string
	ProducerID string // stamped onto EventMeta.producer_id
	Topic      string // default "counter-journal"
}

// JournalProducer serializes CounterJournalEvents and publishes them
// synchronously to Kafka with user_id as the partition key (ADR-0004).
type JournalProducer struct {
	client *kgo.Client
	cfg    ProducerConfig
	logger *zap.Logger
}

// NewJournalProducer builds the Kafka client in idempotent-producer mode and
// acks=all (ADR-0001).
func NewJournalProducer(cfg ProducerConfig, logger *zap.Logger) (*JournalProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if cfg.ProducerID == "" {
		return nil, errors.New("journal: ProducerID required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "counter-journal"
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
	return &JournalProducer{client: cli, cfg: cfg, logger: logger}, nil
}

// Publish serializes evt and sends it synchronously. partitionKey is the
// user_id so all events for a given user go to the same partition (ADR-0004).
func (p *JournalProducer) Publish(ctx context.Context, partitionKey string, evt *eventpb.CounterJournalEvent) error {
	payload, err := proto.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	rec := &kgo.Record{
		Topic: p.cfg.Topic,
		Key:   []byte(partitionKey),
		Value: payload,
	}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

// Close flushes and closes the underlying client.
func (p *JournalProducer) Close() { p.client.Close() }
