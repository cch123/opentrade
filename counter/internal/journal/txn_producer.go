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
)

// TxnProducerConfig configures Counter's transactional Kafka producer used by
// PlaceOrder / CancelOrder to atomically write counter-journal + order-event
// (ADR-0005).
type TxnProducerConfig struct {
	Brokers        []string
	ClientID       string
	TransactionalID string // e.g. "counter-shard-0-main" (ADR-0017)
	JournalTopic   string  // default "counter-journal"
	OrderEventTopic string // default "order-event"
}

// TxnProducer wraps franz-go's transactional producer. Each BeginCommit cycle
// is guarded by a mutex so a single shard process cannot accidentally start
// two overlapping transactions.
type TxnProducer struct {
	cli    *kgo.Client
	cfg    TxnProducerConfig
	logger *zap.Logger

	mu sync.Mutex // serializes BeginTxn ... EndTransaction
}

// NewTxnProducer constructs the client and initializes transactions (fences
// any dangling session from a previous process using the same
// transactional_id, ADR-0017).
func NewTxnProducer(ctx context.Context, cfg TxnProducerConfig, logger *zap.Logger) (*TxnProducer, error) {
	if cfg.TransactionalID == "" {
		return nil, errors.New("journal: TransactionalID required")
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if cfg.JournalTopic == "" {
		cfg.JournalTopic = "counter-journal"
	}
	if cfg.OrderEventTopic == "" {
		cfg.OrderEventTopic = "order-event"
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.TransactionalID(cfg.TransactionalID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
		kgo.TransactionTimeout(10e9), // 10s, ns
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &TxnProducer{cli: cli, cfg: cfg, logger: logger}, nil
}

// PublishOrderPlacement atomically writes (1) a counter-journal event and
// (2) an order-event within a single Kafka transaction. Partition keys:
// user_id for the journal event, symbol for the order event (ADR-0004).
func (p *TxnProducer) PublishOrderPlacement(
	ctx context.Context,
	journalEvt *eventpb.CounterJournalEvent,
	orderEvt *eventpb.OrderEvent,
	journalKey string,
	orderKey string,
) error {
	return p.runTxn(ctx, func() error {
		if err := p.produce(ctx, p.cfg.JournalTopic, journalKey, journalEvt); err != nil {
			return fmt.Errorf("produce journal: %w", err)
		}
		if err := p.produce(ctx, p.cfg.OrderEventTopic, orderKey, orderEvt); err != nil {
			return fmt.Errorf("produce order-event: %w", err)
		}
		return nil
	})
}

// Publish writes a single CounterJournalEvent inside its own Kafka
// transaction. Used for every single-topic counter-journal write
// (Transfer, settlement, order status) so they benefit from the same
// transactional.id fencing as PlaceOrder / CancelOrder (ADR-0017).
// Signature matches service.Publisher so Service can depend on one
// interface for both dual-write and single-write paths.
func (p *TxnProducer) Publish(
	ctx context.Context,
	partitionKey string,
	evt *eventpb.CounterJournalEvent,
) error {
	return p.runTxn(ctx, func() error {
		return p.produce(ctx, p.cfg.JournalTopic, partitionKey, evt)
	})
}

// Close flushes and closes the underlying kgo client.
func (p *TxnProducer) Close() { p.cli.Close() }

// runTxn serializes Begin → user op → End for one transaction. On any error
// inside the callback the transaction is aborted.
func (p *TxnProducer) runTxn(ctx context.Context, fn func() error) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.cli.BeginTransaction(); err != nil {
		return fmt.Errorf("begin txn: %w", err)
	}
	if err := fn(); err != nil {
		if abortErr := p.cli.EndTransaction(ctx, kgo.TryAbort); abortErr != nil {
			p.logger.Error("abort txn", zap.Error(abortErr))
		}
		return err
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

// produce sends one record synchronously (via ProduceSync for simplicity in
// MVP-3 — franz-go handles the in-txn accounting).
func (p *TxnProducer) produce(ctx context.Context, topic, key string, pb proto.Message) error {
	payload, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	return p.cli.ProduceSync(ctx, rec).FirstErr()
}
