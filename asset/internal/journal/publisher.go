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
// (tests), synchronous Kafka producers (production), or future transactional
// producers.
type Publisher interface {
	// Publish appends one envelope for the given user_id partition key.
	// Returns only after Kafka acks the record. The funding engine commits
	// balance mutations after this call succeeds, so fire-and-forget would
	// make the in-memory wallet diverge from asset-journal on producer
	// failure.
	Publish(ctx context.Context, userID string, evt *eventpb.AssetJournalEvent) error

	// NextSeq returns the next monotonic asset_seq_id. Callers pass it
	// into Build so the envelope carries a stable, typed sequence
	// (ADR-0051).
	NextSeq() uint64

	// Close drains in-flight records. Tests may no-op.
	Close(ctx context.Context) error
}

// KafkaConfig wires the Kafka producer.
type KafkaConfig struct {
	Brokers    []string
	Topic      string
	ClientID   string
	ProducerID string // populated on BuildInput.ProducerID by the service
	QueueSize  int    // deprecated: Publish is synchronous; retained for flag compatibility
	Logger     *zap.Logger
}

// KafkaPublisher is the production Publisher: a franz-go idempotent producer.
// Publish uses ProduceSync so the service can treat asset-journal as the
// durable funding-wallet commit point. The producer is non-transactional for
// MVP; upgrading to a transactional producer later is additive and doesn't
// change the Publisher contract.
type KafkaPublisher struct {
	kc     *kgo.Client
	topic  string
	logger *zap.Logger

	seq atomic.Uint64
}

// NewKafkaPublisher opens the client.
func NewKafkaPublisher(cfg KafkaConfig) (*KafkaPublisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	topic := cfg.Topic
	if topic == "" {
		topic = DefaultTopic
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
	return &KafkaPublisher{
		kc:     kc,
		topic:  topic,
		logger: logger,
	}, nil
}

// Publish writes the record synchronously. We do not silently drop or merely
// enqueue here because asset-journal is the commit log used to recover the
// funding wallet on restart.
func (p *KafkaPublisher) Publish(ctx context.Context, userID string, evt *eventpb.AssetJournalEvent) error {
	data, err := proto.Marshal(evt)
	if err != nil {
		return err
	}
	kr := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(userID),
		Value: data,
	}
	if err := p.kc.ProduceSync(ctx, kr).FirstErr(); err != nil {
		p.logger.Warn("asset-journal produce failed",
			zap.Uint64("asset_seq_id", evt.AssetSeqId),
			zap.Error(err))
		return err
	}
	return nil
}

// NextSeq returns the next asset_seq_id.
func (p *KafkaPublisher) NextSeq() uint64 {
	return p.seq.Add(1)
}

// SetSeqAtLeast raises the local asset_seq_id allocator so the next NextSeq
// returns at least seq+1. Used after startup replay of asset-journal.
func (p *KafkaPublisher) SetSeqAtLeast(seq uint64) {
	for {
		cur := p.seq.Load()
		if cur >= seq {
			return
		}
		if p.seq.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// Close drains and shuts down the Kafka client.
func (p *KafkaPublisher) Close(ctx context.Context) error {
	_ = ctx
	p.kc.Close()
	return nil
}
