package journal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// ProducerConfig configures perp-counter's Kafka producer. One client serves
// two streams: order-event-<symbol> (to Match) and perp-journal (the WAL).
type ProducerConfig struct {
	Brokers  []string
	ClientID string

	// OrderEventTopicPrefix yields the per-symbol Match input topic
	// `<prefix>-<symbol>` (ADR-0050). Default "order-event".
	OrderEventTopicPrefix string
	// JournalTopic is the perp-counter WAL topic. Default "perp-journal".
	JournalTopic string

	// TransactionalID, when set, opens the client in transactional mode so a
	// new owner under the same stable id fences a stale primary's in-flight
	// writes (ADR-0032). Empty keeps the legacy idempotent producer for dev /
	// single-node (perp-counter HA is a later milestone, ADR-0068).
	TransactionalID string

	// ProduceTimeout bounds a single produce/commit. Zero → 5s. The PlaceOrder
	// RPC blocks on this (an order is not "accepted" until durably in Kafka).
	ProduceTimeout time.Duration
}

// Producer publishes order-event + perp-journal records. It satisfies the
// service's Dispatcher (DispatchOrder/DispatchCancel) and Journal (Emit)
// interfaces structurally, so the service never imports this package.
type Producer struct {
	cli           *kgo.Client
	cfg           ProducerConfig
	logger        *zap.Logger
	transactional bool

	mu sync.Mutex // serializes BeginTransaction … EndTransaction in txn mode
}

// NewProducer constructs the client. In transactional mode franz-go issues
// InitProducerID on the first BeginTransaction, fencing any prior owner of the
// same TransactionalID (ADR-0032).
func NewProducer(cfg ProducerConfig, logger *zap.Logger) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if cfg.OrderEventTopicPrefix == "" {
		cfg.OrderEventTopicPrefix = "order-event"
	}
	if cfg.JournalTopic == "" {
		cfg.JournalTopic = "perp-journal"
	}
	if cfg.ProduceTimeout <= 0 {
		cfg.ProduceTimeout = 5 * time.Second
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ProducerLinger(0),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Order-event keyed by symbol, perp-journal keyed by user_id — both
		// hash by record key via the default StickyKeyPartitioner.
	}
	transactional := cfg.TransactionalID != ""
	if transactional {
		opts = append(opts,
			kgo.TransactionalID(cfg.TransactionalID),
			kgo.TransactionTimeout(10*time.Second),
		)
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Producer{cli: cli, cfg: cfg, logger: logger, transactional: transactional}, nil
}

// DispatchOrder publishes a new/cancel order-event to the symbol's Match input
// topic (order-event-<symbol>). Returns an error so PlaceOrder can release the
// reserved margin and reject when the write fails (ADR-0068 §4).
func (p *Producer) DispatchOrder(symbol string, evt *eventpb.OrderEvent) error {
	return p.publish(p.orderTopic(symbol), orderEventKey(symbol), evt)
}

// DispatchCancel publishes a cancel order-event. Same topic/keying as
// DispatchOrder (the payload oneof differs).
func (p *Producer) DispatchCancel(symbol string, evt *eventpb.OrderEvent) error {
	return p.publish(p.orderTopic(symbol), orderEventKey(symbol), evt)
}

// Emit publishes a perp-journal record (keyed by user_id). The service's
// Journal interface is fire-and-forget (no error return), matching the spot
// counter's best-effort settlement publish: a transient failure is logged and
// the record is re-derivable on consumer replay (idempotency watermarks make
// reprocessing safe). A durable WAL pipeline with retry-then-handoff is a later
// milestone (ADR-0068 snapshot/HA).
func (p *Producer) Emit(evt *eventpb.PerpJournalEvent) {
	if err := p.publish(p.cfg.JournalTopic, journalPartitionKey(evt), evt); err != nil {
		p.logger.Error("emit perp-journal",
			zap.String("key", journalPartitionKey(evt)), zap.Error(err))
	}
}

func (p *Producer) orderTopic(symbol string) string {
	return orderEventTopicFor(p.cfg.OrderEventTopicPrefix, symbol)
}

// publish marshals pb and produces one record, wrapped in its own Kafka
// transaction when transactional (idempotent ProduceSync otherwise).
func (p *Producer) publish(topic, key string, pb proto.Message) error {
	payload, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}

	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.ProduceTimeout)
	defer cancel()

	if !p.transactional {
		return p.cli.ProduceSync(ctx, rec).FirstErr()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.cli.BeginTransaction(); err != nil {
		return fmt.Errorf("begin txn: %w", err)
	}
	if err := p.cli.ProduceSync(ctx, rec).FirstErr(); err != nil {
		_ = p.cli.EndTransaction(ctx, kgo.TryAbort)
		return fmt.Errorf("produce %s: %w", topic, err)
	}
	if err := p.cli.Flush(ctx); err != nil {
		_ = p.cli.EndTransaction(ctx, kgo.TryAbort)
		return fmt.Errorf("flush: %w", err)
	}
	if err := p.cli.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("commit txn: %w", err)
	}
	return nil
}

// Flush blocks until buffered records are acked — the ADR-0048 output barrier a
// future snapshot takes before binding offsets.
func (p *Producer) Flush(ctx context.Context) error { return p.cli.Flush(ctx) }

// Close flushes and closes the client.
func (p *Producer) Close() { p.cli.Close() }
