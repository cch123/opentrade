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
	"github.com/xargin/opentrade/pkg/shard"
)

// TxnProducerConfig configures Counter's transactional Kafka producer used by
// PlaceOrder / CancelOrder to atomically write counter-journal + order-event
// (ADR-0005).
//
// OrderEventTopicPrefix (ADR-0050) enables per-symbol topics: the producer
// writes to `<prefix>-<symbol>` instead of a single shared topic. When
// non-empty it takes precedence; when empty the legacy single-topic
// OrderEventTopic is used (kept for in-process tests / legacy deployments
// that still want one shared topic).
type TxnProducerConfig struct {
	Brokers               []string
	ClientID              string
	TransactionalID       string // e.g. "counter-vshard-042-ep-7" (ADR-0017, ADR-0058)
	JournalTopic          string // default "counter-journal"
	OrderEventTopic       string // legacy single topic; default "order-event"
	OrderEventTopicPrefix string // ADR-0050; default "order-event" → `order-event-<symbol>`

	// VShardCount drives counter-journal partitioning: every journal
	// record's Partition is set to shard.Index(user_id, VShardCount),
	// identical to what Match uses for trade-event (ADR-0058 §2a). Must
	// be >0 and must match the rest of the cluster.
	VShardCount int
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
	if cfg.VShardCount <= 0 {
		return nil, errors.New("journal: VShardCount required (ADR-0058)")
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.TransactionalID(cfg.TransactionalID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
		kgo.TransactionTimeout(10e9), // 10s, ns
		// ADR-0058 §2a: counter-journal records carry an explicit
		// Partition; order-event records (per-symbol topics, ADR-0050)
		// fall back to StickyKeyPartitioner by symbol.
		kgo.RecordPartitioner(newJournalAwarePartitioner(cfg.JournalTopic)),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &TxnProducer{cli: cli, cfg: cfg, logger: logger}, nil
}

// PublishOrderPlacement atomically writes (1) a counter-journal event and
// (2) an order-event within a single Kafka transaction. Partition keys:
// user_id for the journal event, symbol for the order event (ADR-0004).
// Target topic for the order-event is derived from orderKey (= symbol) per
// ADR-0050 — `OrderEventTopicPrefix + "-" + orderKey`. Empty prefix keeps
// the legacy single-topic behaviour.
func (p *TxnProducer) PublishOrderPlacement(
	ctx context.Context,
	journalEvt *eventpb.CounterJournalEvent,
	orderEvt *eventpb.OrderEvent,
	journalKey string,
	orderKey string,
) error {
	orderTopic := p.orderEventTopicFor(orderKey)
	return p.runTxn(ctx, func() error {
		if err := p.produce(ctx, p.cfg.JournalTopic, journalKey, journalEvt); err != nil {
			return fmt.Errorf("produce journal: %w", err)
		}
		if err := p.produce(ctx, orderTopic, orderKey, orderEvt); err != nil {
			return fmt.Errorf("produce order-event: %w", err)
		}
		return nil
	})
}

// orderEventTopicFor returns the order-event topic name for a given symbol.
// ADR-0050: returns `OrderEventTopicPrefix + "-" + symbol` when prefix is
// non-empty; otherwise falls back to the legacy single-topic value.
func (p *TxnProducer) orderEventTopicFor(symbol string) string {
	if p.cfg.OrderEventTopicPrefix != "" && symbol != "" {
		return p.cfg.OrderEventTopicPrefix + "-" + symbol
	}
	return p.cfg.OrderEventTopic
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

// Flush blocks until any in-flight produce records are acknowledged by the
// broker. Every Publish / PublishOrderPlacement already runs its own sync
// EndTransaction, so in steady state there is nothing buffered — Flush is
// the ADR-0048 "output barrier" that guarantees all transactions have been
// committed before a snapshot reads state + offsets. Returns the underlying
// kgo.Client.Flush error (typically ctx cancellation).
func (p *TxnProducer) Flush(ctx context.Context) error {
	return p.cli.Flush(ctx)
}

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
// MVP-3 — franz-go handles the in-txn accounting). Records destined for
// JournalTopic get their Partition pinned via shard.Index so that 1 vshard
// == 1 partition (ADR-0058 §2a). Other topics (per-symbol order-event)
// pass through with Partition=0 and let StickyKeyPartitioner hash by key.
func (p *TxnProducer) produce(ctx context.Context, topic, key string, pb proto.Message) error {
	payload, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	if topic == p.cfg.JournalTopic {
		// key == user_id for every counter-journal event.
		rec.Partition = int32(shard.Index(key, p.cfg.VShardCount))
	}
	return p.cli.ProduceSync(ctx, rec).FirstErr()
}
