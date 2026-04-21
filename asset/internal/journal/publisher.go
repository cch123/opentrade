package journal

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// DefaultTopic is the asset-journal Kafka topic (ADR-0057). A CLI flag
// in main surfaces it so staging can prefix/suffix.
const DefaultTopic = "asset-journal"

// Publisher is the contract the service layer sees. It intentionally
// does NOT expose Kafka types — implementations may be in-memory fakes
// (tests), async Kafka producers (production), or future transactional
// producers.
type Publisher interface {
	// Publish appends one envelope for the given user_id partition key.
	// Returns nil on successful enqueue; actual Kafka acks are async
	// inside the drain goroutine (MVP fire-and-forget, mirroring
	// conditional/journal).
	Publish(ctx context.Context, userID string, evt *eventpb.AssetJournalEvent) error

	// NextSeq returns the next monotonic asset_seq_id. Callers pass it
	// into Build so the envelope carries a stable, typed sequence
	// (ADR-0051).
	NextSeq() uint64

	// Close drains in-flight records. Tests may no-op.
	Close(ctx context.Context) error
}

// KafkaConfig wires the async Kafka producer.
type KafkaConfig struct {
	Brokers    []string
	Topic      string
	ClientID   string
	ProducerID string // populated on BuildInput.ProducerID by the service
	QueueSize  int    // 0 → 4096
	Logger     *zap.Logger
}

// KafkaPublisher is the production Publisher: a franz-go idempotent
// producer with an internal buffered channel. The producer is
// non-transactional for MVP (ADR-0057 §6 "DB-first + Kafka retry");
// upgrading to a transactional producer later is additive and doesn't
// change the Publisher contract.
type KafkaPublisher struct {
	kc     *kgo.Client
	topic  string
	logger *zap.Logger

	seq   atomic.Uint64
	queue chan pendingRecord
	done  chan struct{}
}

type pendingRecord struct {
	partitionKey string
	evt          *eventpb.AssetJournalEvent
}

// NewKafkaPublisher opens the client and starts the drain goroutine.
func NewKafkaPublisher(cfg KafkaConfig) (*KafkaPublisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	topic := cfg.Topic
	if topic == "" {
		topic = DefaultTopic
	}
	qs := cfg.QueueSize
	if qs <= 0 {
		qs = 4096
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	kc, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	p := &KafkaPublisher{
		kc:     kc,
		topic:  topic,
		logger: logger,
		queue:  make(chan pendingRecord, qs),
		done:   make(chan struct{}),
	}
	go p.drain()
	return p, nil
}

// Publish enqueues the record for async publication. The channel is
// buffered; when full, Publish blocks until ctx is done or a slot frees.
// We DO NOT silently drop here (vs conditional/journal) because asset-
// journal is the system of record for saga projections — a dropped
// event means a divergent MySQL projection downstream.
func (p *KafkaPublisher) Publish(ctx context.Context, userID string, evt *eventpb.AssetJournalEvent) error {
	select {
	case p.queue <- pendingRecord{partitionKey: userID, evt: evt}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NextSeq returns the next asset_seq_id.
func (p *KafkaPublisher) NextSeq() uint64 {
	return p.seq.Add(1)
}

// Close drains and shuts down the Kafka client.
func (p *KafkaPublisher) Close(ctx context.Context) error {
	close(p.queue)
	select {
	case <-p.done:
	case <-ctx.Done():
	}
	p.kc.Close()
	return nil
}

func (p *KafkaPublisher) drain() {
	defer close(p.done)
	for rec := range p.queue {
		data, err := proto.Marshal(rec.evt)
		if err != nil {
			p.logger.Warn("marshal asset-journal event", zap.Error(err))
			continue
		}
		kr := &kgo.Record{
			Topic: p.topic,
			Key:   []byte(rec.partitionKey),
			Value: data,
		}
		p.kc.Produce(context.Background(), kr, func(_ *kgo.Record, err error) {
			if err != nil {
				p.logger.Warn("asset-journal produce failed",
					zap.Uint64("asset_seq_id", rec.evt.AssetSeqId),
					zap.Error(err))
			}
		})
	}
}
