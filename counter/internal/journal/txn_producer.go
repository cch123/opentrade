package journal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	TransactionalID       string // e.g. "counter-vshard-042" (ADR-0017, ADR-0058 — stable per vshard so Kafka's native fencing applies)
	JournalTopic          string // default "counter-journal"
	OrderEventTopic       string // legacy single topic; default "order-event"
	OrderEventTopicPrefix string // ADR-0050; default "order-event" → `order-event-<symbol>`

	// VShardCount drives counter-journal partitioning: every journal
	// record's Partition is set to shard.Index(user_id, VShardCount),
	// identical to what Match uses for trade-event (ADR-0058 §2a). Must
	// be >0 and must match the rest of the cluster.
	VShardCount int

	// Writer metadata (ADR-0058 §4): stamped onto every record's
	// Kafka headers so audit / reconciliation tools can tell which
	// node (and which assignment epoch) wrote a given event without
	// relying on the payload schema. Owner changes rotate these
	// values; transactional.id and EventMeta.producer_id stay stable.
	WriterNodeID string
	WriterEpoch  uint64

	// PublishRetryBudget caps how long ADR-0060 §3 retry-then-panic
	// keeps trying before escalating. Zero falls back to the package
	// default (5s). Tests inject sub-second values to exercise the
	// panic path deterministically.
	PublishRetryBudget time.Duration

	// PublishRetryBackoff is the sleep between retries. Zero falls
	// back to the package default (50ms).
	PublishRetryBackoff time.Duration

	// RetryMetrics is the optional emitter for per-op retry counters
	// (ADR-0060 M8). Pass nil to disable (tests + the synchronous
	// MVP path). Fires one Inc per retry attempt beyond the first so
	// operators can distinguish "healthy first-try success" from
	// "riding out transient errors".
	RetryMetrics RetryMetrics
}

// RetryMetrics is the nil-safe emission hook used by runTxnWithRetry.
// Implemented by counter/internal/metrics.Counter but kept as an
// interface here to avoid an import cycle (metrics -> journal would
// loop via worker). Tests can pass a fake.
type RetryMetrics interface {
	RecordPublishRetry(op string, attempt int)
}

// TxnProducer wraps franz-go's transactional producer. Each BeginCommit cycle
// is guarded by a mutex so a single shard process cannot accidentally start
// two overlapping transactions.
type TxnProducer struct {
	cli    *kgo.Client
	cfg    TxnProducerConfig
	logger *zap.Logger

	mu sync.Mutex // serializes BeginTxn ... EndTransaction

	// journalHighOffset tracks the highest committed record offset on
	// this producer's counter-journal partition (ADR-0060 §4.1). Updated
	// after every successful ProduceSync on JournalTopic; read by
	// VShardWorker.writeSnapshot to stamp snap.JournalOffset. Atomic
	// because Flush + snapshot read can race with in-flight produces
	// (even though produce itself is serialised by mu, the snapshot
	// path reads without holding mu).
	//
	// Value semantics: -1 means "never produced". Callers computing the
	// next-to-consume offset add 1; zero value is therefore indistinguishable
	// from "nothing ever published" which is the correct catch-up behaviour
	// on cold start (journal_offset=0 → consumer seeks to start, topic
	// is empty, nothing to apply).
	journalHighOffset atomic.Int64
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
	p := &TxnProducer{cli: cli, cfg: cfg, logger: logger}
	p.journalHighOffset.Store(-1)
	return p, nil
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
	return p.runTxnWithRetry(ctx, "PublishOrderPlacement", func() error {
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
//
// ADR-0060 §3: transient Kafka errors trigger bounded retry (up to
// PublishRetryBudget, default 5s); budget exhaustion panics. Callers
// treat a successful return as "event committed"; a panic as "vshard
// must be handed off" (ADR-0058 cold migration picks up).
func (p *TxnProducer) Publish(
	ctx context.Context,
	partitionKey string,
	evt *eventpb.CounterJournalEvent,
) error {
	return p.runTxnWithRetry(ctx, "Publish", func() error {
		return p.produce(ctx, p.cfg.JournalTopic, partitionKey, evt)
	})
}

// PublishToVShard writes evt to the counter-journal partition that
// owns vshardID, bypassing the usual shard.Index(key) hashing. Used
// by ADR-0060's advancer to emit TECheckpointEvent on the current
// vshard's partition — the event has no user-level identity and
// hashing by a synthetic key would not guarantee landing in the
// right partition.
//
// key is used only as the Kafka record key for audit / replay (no
// consumer reads it for routing). Callers pass a descriptive token
// like "vshard-042-checkpoint".
//
// Retry semantics (ADR-0060 §3): same as Publish — bounded retry,
// panic on budget exhaustion.
func (p *TxnProducer) PublishToVShard(
	ctx context.Context,
	vshardID int32,
	key string,
	evt *eventpb.CounterJournalEvent,
) error {
	return p.runTxnWithRetry(ctx, "PublishToVShard", func() error {
		return p.produceToPartition(ctx, p.cfg.JournalTopic, vshardID, key, evt)
	})
}

// ProduceFenceSentinel writes a single StartupFenceEvent record to
// this vshard's counter-journal partition inside a Kafka transaction.
// This is ADR-0064 §3 Phase 1 step ③ — the real-Produce primitive
// Counter invokes at startup to:
//
//  1. Force franz-go's lazy InitProducerID to fire on this
//     transactional.id, which causes the broker's Transaction
//     Coordinator to fence the prior owner and write abort markers
//     for any of their still-pending transactions to this partition.
//  2. Stabilise the partition LEO so trade-dump's on-demand
//     WaitAppliedTo has a well-defined target (the commit marker
//     this EndTransaction produces becomes the anchor LEO advances
//     past).
//
// The record itself is a strict no-op at apply time (shadow engine
// and Counter engine both treat StartupFenceEvent as no-op, pinned
// by tests in M1a). Its value is entirely in its physical presence
// — not in any state transition.
//
// Retry semantics (ADR-0060 §3): same as Publish — bounded retry
// with a panic on budget exhaustion (PublishRetryBudget, default
// 5s). **IMPORTANT startup-path nuance**: runTxnWithRetry checks
// ctx.Err() at the top of every iteration and returns ctx.Err()
// BEFORE the budget-exhaustion panic. Callers that need this to
// degrade to a fallback error rather than crash the process (e.g.
// ADR-0064 §3 Phase 1 on-demand startup — the `worker.loadOnDemand`
// path wraps this call in a bounded onDemandCtx) MUST pass a ctx
// whose deadline is shorter than PublishRetryBudget. Steady-state
// RPC callers keep the panic-on-exhaust semantics (ADR-0060 §3
// vshard failover) by passing the long-lived worker ctx.
//
// Notes:
//   - vshardID MUST match this producer's configured vshard; we
//     cross-check against WriterEpoch / TransactionalID-derived
//     identity in M2c when we have worker context to compare.
//   - On return the journalHighOffset mirror is bumped past the
//     sentinel's offset, so a subsequent snapshot picks a cursor
//     >= startOffset (inherits Publish's invariant).
//   - The sentinel's apply path must tolerate nodeID/epoch=0 zero
//     values too — tests inject those for determinism.
func (p *TxnProducer) ProduceFenceSentinel(ctx context.Context, vshardID int32) error {
	evt := buildFenceSentinelEvent(p.cfg.WriterNodeID, p.cfg.WriterEpoch, time.Now().UnixMilli())
	key := fenceSentinelKey(vshardID)
	return p.runTxnWithRetry(ctx, "ProduceFenceSentinel", func() error {
		return p.produceToPartition(ctx, p.cfg.JournalTopic, vshardID, key, evt)
	})
}

// buildFenceSentinelEvent is the pure-function shape of the sentinel
// envelope. Factored out so unit tests can assert the exact proto
// payload without needing a live kgo.Client.
func buildFenceSentinelEvent(nodeID string, epoch uint64, tsMs int64) *eventpb.CounterJournalEvent {
	return &eventpb.CounterJournalEvent{
		// Sentinels do not allocate a counter_seq (ADR-0064 §1.2).
		// Shadow engine / Counter engine do NOT advance counterSeq
		// on this payload — the zero here documents the contract.
		CounterSeqId: 0,
		Payload: &eventpb.CounterJournalEvent_StartupFence{
			StartupFence: &eventpb.StartupFenceEvent{
				NodeId: nodeID,
				Epoch:  epoch,
				TsMs:   tsMs,
			},
		},
	}
}

// fenceSentinelKey returns the Kafka record key stamped on the
// sentinel record. Purely for audit / log grep; consumers do not
// route by this key. Format is fixed-width zero-padded so sorting
// a raw journal dump keeps startup fences grouped by vshard.
func fenceSentinelKey(vshardID int32) string {
	return fmt.Sprintf("vshard-%03d-startup-fence", vshardID)
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

// PublishRetryBudget bounds how long runTxnWithRetry keeps retrying
// before escalating to panic. ADR-0060 §3 default = 5s: long enough to
// ride out Kafka leader elections + transient network blips, short
// enough that vshard failover (ADR-0058 cold handoff) kicks in before
// upstream pendingList / SubmitAsync queues grow unbounded.
//
// Exposed as a var (not const) so tests can inject a sub-second
// budget via TxnProducerConfig.PublishRetryBudget.
var PublishRetryBudget = 5 * time.Second

// PublishRetryBackoff is the sleep between retries. Fixed (not
// exponential) — the expected failure modes are leader elections
// that resolve in a few hundred ms; a slower backoff would burn
// the budget on idle sleeps.
var PublishRetryBackoff = 50 * time.Millisecond

// runTxnWithRetry is runTxn with ADR-0060 §3 retry semantics: retry
// on transient error until budget exhaustion, then panic. opName is
// a short identifier embedded in the panic / log so operators can
// see which call site tripped (Publish / PublishOrderPlacement /
// PublishToVShard).
//
// Context cancellation surfaces immediately — callers that cancel
// ctx get a ctx.Err() back without retry loops eating the signal.
//
// Panic semantics: the caller's deferred shutdown hooks still run
// (consumer.Close, periodicSnapshot shutdown), but the Run()
// goroutine exits. ADR-0058's clustering layer sees the vshard
// lease expire and migrates to another node.
func (p *TxnProducer) runTxnWithRetry(ctx context.Context, opName string, fn func() error) error {
	budget := p.cfg.PublishRetryBudget
	if budget <= 0 {
		budget = PublishRetryBudget
	}
	backoff := p.cfg.PublishRetryBackoff
	if backoff <= 0 {
		backoff = PublishRetryBackoff
	}
	deadline := time.Now().Add(budget)
	var lastErr error
	attempts := 0
	for {
		attempts++
		err := p.runTxn(ctx, fn)
		if err == nil {
			if attempts > 1 && p.cfg.RetryMetrics != nil {
				// Emit one final "retry success" so operators can
				// distinguish "we rode this out" from "every retry
				// ultimately panicked" (the latter has no success
				// emission).
				p.cfg.RetryMetrics.RecordPublishRetry(opName, attempts)
			}
			return nil
		}
		lastErr = err
		// Context cancelled — no retry.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			// Budget exhausted — escalate.
			p.logger.Error("journal publish budget exhausted, panicking",
				zap.String("op", opName),
				zap.Int("attempts", attempts),
				zap.Duration("budget", budget),
				zap.Error(lastErr))
			panic(fmt.Sprintf("journal: %s failed after %d attempts over %s: %v",
				opName, attempts, budget, lastErr))
		}
		p.logger.Warn("journal publish failed; retrying",
			zap.String("op", opName),
			zap.Int("attempt", attempts),
			zap.Error(err))
		if p.cfg.RetryMetrics != nil {
			p.cfg.RetryMetrics.RecordPublishRetry(opName, attempts)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
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
// Writer metadata (ADR-0058 §4) is attached as Kafka record headers so
// audit tooling can tell which node / assignment epoch produced each
// event without coupling to the payload schema.
//
// After a successful JournalTopic write, updates journalHighOffset so
// VShardWorker.writeSnapshot can stamp snap.JournalOffset for
// ADR-0060 catch-up replay on next restore.
func (p *TxnProducer) produce(ctx context.Context, topic, key string, pb proto.Message) error {
	payload, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	rec := &kgo.Record{
		Topic:   topic,
		Key:     []byte(key),
		Value:   payload,
		Headers: p.writerHeaders(),
	}
	if topic == p.cfg.JournalTopic {
		// key == user_id for every counter-journal event.
		rec.Partition = int32(shard.Index(key, p.cfg.VShardCount))
	}
	if err := p.cli.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return err
	}
	p.noteJournalOffset(topic, rec)
	return nil
}

// produceToPartition writes a record directly to a caller-specified
// partition, bypassing shard.Index hashing. Used by PublishToVShard
// for events that have no user-level identity.
func (p *TxnProducer) produceToPartition(ctx context.Context, topic string, partition int32, key string, pb proto.Message) error {
	payload, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	rec := &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Key:       []byte(key),
		Value:     payload,
		Headers:   p.writerHeaders(),
	}
	if err := p.cli.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return err
	}
	p.noteJournalOffset(topic, rec)
	return nil
}

// noteJournalOffset bumps journalHighOffset after a successful produce to
// the counter-journal topic. Ignores non-journal writes (order-event etc.
// — those have their own downstream watermarks). Uses atomic max so
// out-of-order callbacks (shouldn't happen under mu-serialised runTxn
// but cheap insurance) never rewind the value.
func (p *TxnProducer) noteJournalOffset(topic string, rec *kgo.Record) {
	if topic != p.cfg.JournalTopic {
		return
	}
	for {
		cur := p.journalHighOffset.Load()
		if rec.Offset <= cur {
			return
		}
		if p.journalHighOffset.CompareAndSwap(cur, rec.Offset) {
			return
		}
	}
}

// JournalOffsetNext returns the next-to-consume offset on this vshard's
// counter-journal partition: last successfully-committed record offset
// + 1, or 0 if nothing was ever produced. Callers use this to stamp
// snapshot.JournalOffset (ADR-0060 §4.1) so recovery's catch-up
// consumer lands at the right place.
//
// Safe to call concurrently with produce(): the underlying atomic
// write happens after ProduceSync returns success, and Flush
// guarantees all in-flight produces have settled before snapshot
// read (ADR-0048 output barrier still applies).
func (p *TxnProducer) JournalOffsetNext() int64 {
	hi := p.journalHighOffset.Load()
	if hi < 0 {
		return 0
	}
	return hi + 1
}

// NoteObservedJournalOffset bumps journalHighOffset to max(current,
// observed). Used by ADR-0060 catch-up: after replay reads the last
// journal record at offset K, seeding the producer's tracker with K
// guarantees the next writeSnapshot picks a journal_offset >=
// startOffset of this recovery. Prevents an oscillating regression
// where every clean restart walks the catch-up window from 0 because
// the first snapshot after restart predates any new publish.
//
// Callers must only use this in initialisation paths (Run before
// consumer opens). On the live path use produce() which bumps via
// noteJournalOffset.
func (p *TxnProducer) NoteObservedJournalOffset(observed int64) {
	if observed < 0 {
		return
	}
	for {
		cur := p.journalHighOffset.Load()
		if observed <= cur {
			return
		}
		if p.journalHighOffset.CompareAndSwap(cur, observed) {
			return
		}
	}
}

// writerHeaders returns the fixed audit headers for every record this
// producer emits. Values only change on worker restart (new epoch or
// new owner node); Kafka headers are the right place for them because
// they're out-of-band from the proto payload consumers already parse.
func (p *TxnProducer) writerHeaders() []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "writer-node", Value: []byte(p.cfg.WriterNodeID)},
		{Key: "writer-epoch", Value: []byte(strconv.FormatUint(p.cfg.WriterEpoch, 10))},
	}
}
