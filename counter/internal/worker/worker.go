// Package worker packages up everything needed to serve one virtual
// shard (ADR-0058 phase 3b). A VShardWorker owns the per-vshard state,
// sequencer, dedup table, transactional producer (with epoch-fenced
// transactional id), trade-event partition consumer, snapshot
// restore/save cycle, and the Service instance the gRPC dispatcher
// will route requests to.
//
// Lifecycle:
//
//	w, _ := worker.New(Config{...})
//	go w.Run(ctx)        // blocks until ctx done
//	<-w.Ready()          // safe to route traffic after this closes
//	w.Service().Transfer(...)
//
// Out of scope for 3b-2A: Manager that spawns workers per
// clustering.WatchAssignedVShards — that's phase 3b-2B. This file only
// delivers the single-vshard unit.
package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/clustering"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/counter/internal/metrics"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/counter/internal/snapshot"
	"github.com/xargin/opentrade/pkg/idgen"
)

const (
	// SnapshotKeyFormat is the BlobStore key stem for each vshard's
	// snapshot. Zero-padded so lexicographic listing matches numeric
	// order.
	SnapshotKeyFormat = "vshard-%03d"

	// TransactionalIDFormat is the Kafka transactional.id for a
	// vshard's producer. Per Kafka semantics it MUST be stable across
	// owner changes: the broker's TransactionCoordinator uses same-id
	// re-init to auto-fence the previous owner via producer_epoch
	// bump (KIP-98). The ADR-0058 epoch is NOT embedded here — it
	// lives in etcd assignment for application-level decisions
	// (Manager restart, CAS guard) and is re-surfaced into each
	// Kafka record's headers for downstream audit.
	TransactionalIDFormat = "counter-vshard-%03d"

	defaultSnapshotFlushTimeout = 3 * time.Second
	defaultShutdownFlushTimeout = 10 * time.Second

	// ADR-0062 evictor defaults. Chosen so steady-state trade-dump lag
	// (<1s typical, minutes in failure) is comfortably covered by the
	// retention window, while batch size bounds a single scan at well
	// under 100ms even on a ~200w entry byID map.
	defaultOrderEvictInterval  = 30 * time.Second
	defaultOrderEvictRetention = 1 * time.Hour
	defaultOrderEvictMaxBatch  = 500

	// ADR-0062 §5 circuit-breaker defaults. Snapshot staleness is
	// the longer bound (trade-dump pipeline downtime tolerance);
	// checkpoint staleness is the shorter bound (advancer liveness).
	defaultEvictSnapshotStaleAfter   = 2 * time.Hour
	defaultEvictCheckpointStaleAfter = 5 * time.Minute
)

// Config wires all per-vshard dependencies. Brokers / topics / store
// are shared across workers on the same node; VShardID / Epoch are
// unique per worker instance.
type Config struct {
	VShardID    clustering.VShardID
	Epoch       uint64
	NodeID      string // stamped into ClientID + ProducerID
	VShardCount int    // for Service.TotalShards — OwnsUser check

	// Kafka wiring
	Brokers               []string
	JournalTopic          string
	TradeEventTopic       string
	OrderEventTopic       string // legacy single topic (ADR-0050 fallback)
	OrderEventTopicPrefix string // per-symbol topics prefix (ADR-0050)

	// State plumbing
	Store            snapshot.BlobStore
	SnapshotFormat   snapshot.Format
	SnapshotInterval time.Duration
	DedupTTL         time.Duration

	// Behaviour
	DefaultMaxOpenLimitOrders uint32
	SymbolLookup              service.SymbolLookup // optional precision source

	// Timeouts — zero defaults below.
	SnapshotFlushTimeout time.Duration
	ShutdownFlushTimeout time.Duration

	// ADR-0062 terminal-order eviction.
	//
	// OrderEvictInterval is the scan cadence. 0 falls back to the
	// default (30s); explicitly negative disables the loop (tests).
	// OrderEvictRetention is the minimum age a terminal order must
	// reach before it becomes eligible. Default 1h.
	// OrderEvictMaxBatch caps the number of orders processed per round
	// so a single scan cannot monopolise the user sequencer. Default 500.
	OrderEvictInterval  time.Duration
	OrderEvictRetention time.Duration
	OrderEvictMaxBatch  int

	// EvictSnapshotStaleAfter is the age at which a missing snapshot
	// round trips this vshard's evictor into the circuit-breaker halt
	// state (ADR-0062 §5). Measured against VShardWorker's own last
	// writeSnapshot success — in the post-ADR-0061 world this will
	// be read from the authoritative snapshot store's metadata. 0
	// falls back to the default (2h).
	EvictSnapshotStaleAfter time.Duration

	// EvictCheckpointStaleAfter is the age at which a missing
	// TECheckpoint publish halts eviction — downstream (trade-dump)
	// stops advancing its te_watermark so newly evicted orders would
	// miss the snapshot pipeline. 0 falls back to the default (5m).
	EvictCheckpointStaleAfter time.Duration

	// Metrics is the optional ADR-0060 M8 + ADR-0062 M8 observability
	// hook. Nil disables emission (tests / legacy paths). Production
	// main.go constructs a single *metrics.Counter and shares it
	// across every VShardWorker + the shared /metrics HTTP handler.
	Metrics *metrics.Counter

	Logger *zap.Logger
}

// VShardWorker runs the lifecycle of a single virtual shard.
type VShardWorker struct {
	cfg Config

	// Stateful components; populated once Run has restored the
	// snapshot. Reads before Ready() closes are unsafe.
	state      *engine.ShardState
	seq        *sequencer.UserSequencer
	dedupTable *dedup.Table
	svc        *service.Service
	producer   *journal.TxnProducer
	// pending tracks in-flight trade-events across per-user drain
	// goroutines (ADR-0060 §1.3). Populated before the first consumer
	// record is dispatched; reads are safe once Ready() closes.
	pending *pendingList

	// lastCheckpointAtMs / lastSnapshotAtMs are liveness timestamps
	// sampled by the ADR-0062 §5 evict circuit breaker. Updated by
	// emitCheckpoint + writeSnapshot on success; read by
	// evictHealthCheck before each round.
	lastCheckpointAtMs atomic.Int64
	lastSnapshotAtMs   atomic.Int64

	ready chan struct{}
}

// New validates cfg. No I/O happens here — all state restore + broker
// connections are deferred to Run.
func New(cfg Config) (*VShardWorker, error) {
	if cfg.Store == nil {
		return nil, errors.New("worker: Config.Store required")
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("worker: Config.Brokers required")
	}
	if cfg.NodeID == "" {
		return nil, errors.New("worker: Config.NodeID required")
	}
	if cfg.VShardCount <= 0 {
		return nil, errors.New("worker: Config.VShardCount must be > 0")
	}
	if int(cfg.VShardID) >= cfg.VShardCount {
		return nil, fmt.Errorf("worker: VShardID %d out of range [0, %d)", cfg.VShardID, cfg.VShardCount)
	}
	if cfg.JournalTopic == "" {
		cfg.JournalTopic = "counter-journal"
	}
	if cfg.TradeEventTopic == "" {
		cfg.TradeEventTopic = "trade-event"
	}
	if cfg.OrderEventTopic == "" {
		cfg.OrderEventTopic = "order-event"
	}
	if cfg.DedupTTL <= 0 {
		cfg.DedupTTL = 24 * time.Hour
	}
	if cfg.SnapshotFlushTimeout <= 0 {
		cfg.SnapshotFlushTimeout = defaultSnapshotFlushTimeout
	}
	if cfg.ShutdownFlushTimeout <= 0 {
		cfg.ShutdownFlushTimeout = defaultShutdownFlushTimeout
	}
	// Negative -> explicit disable (tests). Zero -> default.
	if cfg.OrderEvictInterval == 0 {
		cfg.OrderEvictInterval = defaultOrderEvictInterval
	}
	if cfg.OrderEvictRetention <= 0 {
		cfg.OrderEvictRetention = defaultOrderEvictRetention
	}
	if cfg.OrderEvictMaxBatch <= 0 {
		cfg.OrderEvictMaxBatch = defaultOrderEvictMaxBatch
	}
	if cfg.EvictSnapshotStaleAfter <= 0 {
		cfg.EvictSnapshotStaleAfter = defaultEvictSnapshotStaleAfter
	}
	if cfg.EvictCheckpointStaleAfter <= 0 {
		cfg.EvictCheckpointStaleAfter = defaultEvictCheckpointStaleAfter
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &VShardWorker{cfg: cfg, ready: make(chan struct{})}, nil
}

// Run is the worker's main loop. It restores state from the shared
// snapshot, opens a transactional producer whose ID embeds the epoch
// (fencing any previous owner), starts the per-partition trade-event
// consumer and the periodic snapshot goroutine, then blocks until ctx
// is cancelled. On exit it drains the consumer, flushes the producer,
// and writes a final snapshot so the next owner can seek deterministically.
func (w *VShardWorker) Run(ctx context.Context) (rerr error) {
	logger := w.cfg.Logger.With(
		zap.Int("vshard", int(w.cfg.VShardID)),
		zap.Uint64("epoch", w.cfg.Epoch),
		zap.String("node", w.cfg.NodeID))

	state := engine.NewShardState(int(w.cfg.VShardID))
	// ADR-0060 §5 (M7): wire SnapshotMu as the sequencer's apply
	// barrier so every per-user fn runs under RLock; writeSnapshot
	// takes the Write lock briefly for cross-account consistency.
	seq := sequencer.New(sequencer.WithApplyBarrier(&state.SnapshotMu))
	dt := dedup.New(w.cfg.DedupTTL)

	key := fmt.Sprintf(SnapshotKeyFormat, w.cfg.VShardID)
	snap, err := snapshot.Load(ctx, w.cfg.Store, key)
	var offsets map[int32]int64
	var journalOffset int64
	switch {
	case err == nil:
		if err := snapshot.Restore(int(w.cfg.VShardID), state, seq, dt, snap); err != nil {
			return fmt.Errorf("snapshot restore: %w", err)
		}
		offsets = snapshot.OffsetsSliceToMap(snap.Offsets)
		journalOffset = snap.JournalOffset
		logger.Info("restored from snapshot",
			zap.Int("version", snap.Version),
			zap.Uint64("counter_seq", snap.CounterSeq),
			zap.Int("accounts", len(snap.Accounts)),
			zap.Int("orders", len(snap.Orders)),
			zap.Int("partitions", len(offsets)),
			zap.Int64("journal_offset", journalOffset))
	case errors.Is(err, os.ErrNotExist):
		logger.Info("no snapshot found, starting fresh")
	default:
		return fmt.Errorf("snapshot load: %w", err)
	}

	// Avoid Go's "non-nil interface holding a nil pointer" gotcha:
	// assign the concrete pointer to the interface only when it's
	// non-nil, so txn_producer's `!= nil` check reflects reality.
	var retryMetrics journal.RetryMetrics
	if w.cfg.Metrics != nil {
		retryMetrics = w.cfg.Metrics
	}
	txnID := fmt.Sprintf(TransactionalIDFormat, w.cfg.VShardID)
	producer, err := journal.NewTxnProducer(ctx, journal.TxnProducerConfig{
		Brokers:               w.cfg.Brokers,
		ClientID:              fmt.Sprintf("%s-vshard-%03d-txn", w.cfg.NodeID, w.cfg.VShardID),
		TransactionalID:       txnID,
		JournalTopic:          w.cfg.JournalTopic,
		OrderEventTopic:       w.cfg.OrderEventTopic,
		OrderEventTopicPrefix: w.cfg.OrderEventTopicPrefix,
		VShardCount:           w.cfg.VShardCount,
		WriterNodeID:          w.cfg.NodeID,
		WriterEpoch:           w.cfg.Epoch,
		RetryMetrics:          retryMetrics,
	}, logger)
	if err != nil {
		return fmt.Errorf("txn producer: %w", err)
	}
	defer producer.Close()

	idg, err := idgen.NewGenerator(int(w.cfg.VShardID))
	if err != nil {
		return fmt.Errorf("idgen: %w", err)
	}

	svc := service.New(service.Config{
		ShardID:                   int(w.cfg.VShardID),
		TotalShards:               w.cfg.VShardCount,
		ProducerID:                fmt.Sprintf(TransactionalIDFormat, w.cfg.VShardID),
		DefaultMaxOpenLimitOrders: w.cfg.DefaultMaxOpenLimitOrders,
	}, state, seq, dt, producer, logger)
	svc.SetOrderDeps(producer, idg)
	if w.cfg.SymbolLookup != nil {
		svc.SetSymbolLookup(w.cfg.SymbolLookup)
	}
	svc.SetOffsets(offsets)

	// Publish the live components BEFORE opening the consumer — Ready
	// signals "Service is safe to call" so a dispatcher may start
	// routing. The consumer needs svc too, so it's fine that it opens
	// right after.
	w.state = state
	w.seq = seq
	w.dedupTable = dt
	w.svc = svc
	w.producer = producer

	// ADR-0060 §4.2: catch-up journal replay happens BEFORE Ready
	// closes so that any RPC or trade-event dispatched to this
	// worker observes state that reflects every journal event
	// committed by the previous owner (including publishes that
	// raced the last snapshot). Skipped when the loaded snapshot
	// has journal_offset == 0 (cold start / pre-0060 snapshot).
	if journalOffset > 0 {
		if err := w.catchUpJournal(ctx, journalOffset); err != nil {
			return fmt.Errorf("catchup journal: %w", err)
		}
	}

	close(w.ready)

	// ADR-0060: consumer loop is fire-and-forget. Each trade-event
	// record is dispatched via Service.HandleTradeRecordAsync so the
	// consumer can poll the next record immediately; the ordered
	// advancer reads pendingList and advances the local offset map +
	// (M3: publishes TECheckpointEvent) as per-user fns complete.
	pending := newPendingList()
	// Buffered by 1: signals are coalescing, drop duplicates.
	advanceSignal := make(chan struct{}, 1)
	w.pending = pending
	asyncHandler := newAsyncTradeHandler(svc, pending, advanceSignal, logger)

	consumer, err := journal.NewTradePartitionConsumer(journal.TradePartitionConsumerConfig{
		Brokers:    w.cfg.Brokers,
		ClientID:   fmt.Sprintf("%s-vshard-%03d-trade", w.cfg.NodeID, w.cfg.VShardID),
		Topic:      w.cfg.TradeEventTopic,
		Partitions: []int32{int32(w.cfg.VShardID)},
		Offsets:    offsets,
	}, asyncHandler, logger)
	if err != nil {
		return fmt.Errorf("trade consumer: %w", err)
	}
	defer consumer.Close()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("trade consumer exited", zap.Error(err))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runAdvancer(ctx, pending, advanceSignal)
	}()

	// Periodic snapshot runs on a separate context so it can keep
	// draining Flush after ctx is cancelled, up to the shutdown path.
	snapCtx, cancelSnap := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.periodicSnapshot(snapCtx)
	}()

	// ADR-0062 evictor — age out terminal orders from byID after the
	// retention window and publish OrderEvictedEvent so trade-dump's
	// shadow engine + MySQL projection can shrink in lockstep.
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runOrderEvictor(ctx)
	}()

	<-ctx.Done()
	logger.Info("vshard worker shutting down")

	consumer.Close()
	cancelSnap()
	wg.Wait()

	// Time-bounded: if Flush / Put stall past this, we log + move on
	// so an unhealthy broker can't pin the process forever.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), w.cfg.ShutdownFlushTimeout)
	defer cancelShutdown()
	if err := w.writeSnapshot(shutdownCtx); err != nil {
		logger.Error("final snapshot failed", zap.Error(err))
	} else {
		logger.Info("final snapshot written")
	}
	return nil
}

// Ready returns a channel that closes once the Service is safe to call
// — that is, after snapshot restore has completed and the Service has
// been wired with its producer / idgen. Dispatchers must wait on this
// before routing requests to VShardWorker.Service().
func (w *VShardWorker) Ready() <-chan struct{} { return w.ready }

// Service returns the vshard's Service. Returns nil before Ready fires;
// callers that might race should select on Ready() first.
func (w *VShardWorker) Service() *service.Service { return w.svc }

// VShardID is the id this worker owns.
func (w *VShardWorker) VShardID() clustering.VShardID { return w.cfg.VShardID }

// Epoch is the ownership epoch this worker is running under. The
// coordinator increments it on every owner change; a new worker always
// carries a strictly larger epoch than the previous one, which is how
// Kafka transactional fencing knows who is authoritative.
func (w *VShardWorker) Epoch() uint64 { return w.cfg.Epoch }

// periodicSnapshot fires every SnapshotInterval until snapCtx is
// cancelled. SnapshotInterval <= 0 disables the loop entirely (only the
// final shutdown snapshot runs).
func (w *VShardWorker) periodicSnapshot(ctx context.Context) {
	if w.cfg.SnapshotInterval <= 0 {
		return
	}
	ticker := time.NewTicker(w.cfg.SnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flushCtx, cancel := context.WithTimeout(ctx, w.cfg.SnapshotFlushTimeout)
			err := w.writeSnapshot(flushCtx)
			cancel()
			if err != nil {
				w.cfg.Logger.Error("periodic snapshot",
					zap.Int("vshard", int(w.cfg.VShardID)), zap.Error(err))
			}
		}
	}
}

// checkpointPublisher is the narrow TxnProducer subset the advancer
// uses. Defined as an interface so unit tests can capture emitted
// checkpoint events without a Kafka connection.
type checkpointPublisher interface {
	PublishToVShard(ctx context.Context, vshardID int32, key string, evt *eventpb.CounterJournalEvent) error
}

// runAdvancer consumes pendingList signals, pops the consecutive-done
// prefix, publishes a TECheckpointEvent for the new watermark to
// counter-journal, and advances the local offset map via
// Service.AdvanceOffset so the next snapshot picks up the up-to-date
// te_watermark.
//
// ADR-0060 §1.4: this is the single goroutine responsible for offset
// advancement on the async path. The TECheckpointEvent publish fires
// BEFORE AdvanceOffset so trade-dump's shadow pipeline observes the
// checkpoint in counter-journal partition order (after every business
// event whose offset ≤ watermark) — the TxnProducer.mu lock
// serialises the publish against per-user drain goroutines' publishes.
//
// Publish failures log + skip AdvanceOffset for this round: next
// ticker we retry from the same pendingList head (entries are
// idempotent, PopConsecutiveDone isn't called again until the next
// signal).
//
// The synchronous HandleTradeRecord path (legacy / tests) still
// advances via Service.HandleTradeRecord directly and does not
// publish checkpoints (only the async path does — pre-ADR-0060
// consumers never expected them).
func (w *VShardWorker) runAdvancer(ctx context.Context, pending *pendingList, signal <-chan struct{}) {
	w.runAdvancerWithPublisher(ctx, pending, signal, w.producer)
}

// runAdvancerWithPublisher is runAdvancer with an injected publisher
// for test-time substitution.
func (w *VShardWorker) runAdvancerWithPublisher(
	ctx context.Context,
	pending *pendingList,
	signal <-chan struct{},
	pub checkpointPublisher,
) {
	logger := w.cfg.Logger.With(zap.Int("vshard", int(w.cfg.VShardID)))
	for {
		select {
		case <-ctx.Done():
			return
		case <-signal:
			for {
				partition, maxOffset, ok := pending.PopConsecutiveDone()
				if !ok {
					break
				}
				w.emitCheckpoint(ctx, logger, pub, pending, partition, maxOffset)
			}
		}
	}
}

// emitCheckpoint publishes a TECheckpointEvent for (partition,
// maxOffset) and, on success, advances the local offset map. On
// publish failure we log and return without advancing — the next
// pendingList pop will retry (pendingList entries are already
// removed; retry happens on new signal for the same or next offset).
func (w *VShardWorker) emitCheckpoint(
	ctx context.Context,
	logger *zap.Logger,
	pub checkpointPublisher,
	pending *pendingList,
	partition int32,
	maxOffset int64,
) {
	evt := journal.BuildTECheckpointEvent(journal.TECheckpointEventInput{
		CounterSeqID:   w.seq.CounterSeq(),
		ProducerID:     w.cfg.NodeID,
		AccountVersion: 0,
		TePartition:    partition,
		TeOffset:       maxOffset,
	})
	key := fmt.Sprintf("vshard-%03d-checkpoint", w.cfg.VShardID)
	if err := pub.PublishToVShard(ctx, int32(w.cfg.VShardID), key, evt); err != nil {
		logger.Error("publish te checkpoint",
			zap.Int32("te_partition", partition),
			zap.Int64("te_offset", maxOffset),
			zap.Error(err))
		w.cfg.Metrics.RecordCheckpointPublish(int32(w.cfg.VShardID), false)
		return
	}
	w.cfg.Metrics.RecordCheckpointPublish(int32(w.cfg.VShardID), true)
	w.svc.AdvanceOffset(partition, maxOffset+1)
	// Stamp advancer liveness for the ADR-0062 §5 evict circuit
	// breaker. "Last publish" means a checkpoint actually landed on
	// Kafka — publish failures above do NOT touch this so the
	// breaker sees the real outage.
	w.lastCheckpointAtMs.Store(time.Now().UnixMilli())
	// Refresh pendingList gauge on each successful advance — the
	// gauge's denominator is the same list this advancer just
	// shortened, so post-pop is the natural sampling point. Using
	// the injected pending reference keeps test harnesses that
	// bypass Run() (and so never populate w.pending) from NPE'ing.
	if pending != nil {
		w.cfg.Metrics.RecordPendingSize(int32(w.cfg.VShardID), pending.Len())
	}
}

// journalWriter is the narrow TxnProducer subset the evictor needs.
// Defined as an interface so tests can exercise the ring→journal→delete
// sequence without a live Kafka broker — the real implementation is
// *journal.TxnProducer.
type journalWriter interface {
	Publish(ctx context.Context, partitionKey string, evt *eventpb.CounterJournalEvent) error
}

// runOrderEvictor periodically ages out terminal orders from
// OrderStore.byID per ADR-0062. Each round queries CandidatesForEvict,
// then for each candidate runs the strict ring→journal→delete sequence
// under the user's sequencer so it interleaves correctly with other
// per-user work. Publish failures surface as errors on the returned
// task; the evictor logs and moves on to the next candidate (the
// failed order will reappear in the next scan).
//
// OrderEvictInterval <= 0 disables the loop entirely (tests set this
// to a negative sentinel; the defaults path in New turns zero into
// the 30s default).
func (w *VShardWorker) runOrderEvictor(ctx context.Context) {
	if w.cfg.OrderEvictInterval <= 0 {
		return
	}
	logger := w.cfg.Logger.With(zap.Int("vshard", int(w.cfg.VShardID)))
	ticker := time.NewTicker(w.cfg.OrderEvictInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !w.evictHealthCheck(logger) {
				// Circuit breaker tripped — skip this round. The
				// health-check itself emits the metric + log so
				// operators have a trail. Orders stay in byID
				// until either health recovers or Counter
				// restarts.
				continue
			}
			w.evictOneRound(ctx, logger, w.producer)
		}
	}
}

// evictHealthCheck implements ADR-0062 §5: before each round, verify
// that downstream (trade-dump) is keeping up by checking two
// liveness signals:
//
//  1. lastSnapshotAtMs: the most recent successful writeSnapshot.
//     In the Counter-writes-own-snapshot world this is our own
//     snapshot; post-ADR-0061 it'll be trade-dump's snapshot read
//     from blob-store metadata. Either way, stale snapshot means
//     orders evicted now would not land in the authoritative
//     snapshot within the retention window.
//  2. lastCheckpointAtMs: the most recent successful
//     TECheckpointEvent publish. Stale checkpoint means the
//     advancer is stuck — no new watermark reaches trade-dump and
//     evict pre-emptively halts to avoid a loss window.
//
// Returns true to proceed, false to skip the round. A false return
// always logs + increments counter_evict_halted_total so it shows
// up in dashboards.
//
// Zero timestamps (never set — cold start before first success)
// are treated as "healthy" so the evictor can warm up on fresh
// pods; the retention window (1h default) is longer than any
// reasonable startup so nothing gets evicted in this window anyway.
func (w *VShardWorker) evictHealthCheck(logger *zap.Logger) bool {
	now := time.Now().UnixMilli()
	if ts := w.lastSnapshotAtMs.Load(); ts > 0 {
		age := time.Duration(now-ts) * time.Millisecond
		if age > w.cfg.EvictSnapshotStaleAfter {
			logger.Warn("evict halted: snapshot stale",
				zap.Duration("age", age),
				zap.Duration("threshold", w.cfg.EvictSnapshotStaleAfter))
			w.cfg.Metrics.RecordEvictHalted(int32(w.cfg.VShardID), "snapshot_stale")
			return false
		}
	}
	if ts := w.lastCheckpointAtMs.Load(); ts > 0 {
		age := time.Duration(now-ts) * time.Millisecond
		if age > w.cfg.EvictCheckpointStaleAfter {
			logger.Warn("evict halted: checkpoint stale",
				zap.Duration("age", age),
				zap.Duration("threshold", w.cfg.EvictCheckpointStaleAfter))
			w.cfg.Metrics.RecordEvictHalted(int32(w.cfg.VShardID), "checkpoint_stale")
			return false
		}
	}
	return true
}

// evictOneRound is one tick of the evictor loop. pub is injected so
// tests can capture emitted journal events without a real Kafka
// producer; production callers pass w.producer.
func (w *VShardWorker) evictOneRound(ctx context.Context, logger *zap.Logger, pub journalWriter) {
	now := time.Now().UnixMilli()
	retentionMS := w.cfg.OrderEvictRetention.Milliseconds()
	candidates := w.state.Orders().CandidatesForEvict(now, retentionMS, w.cfg.OrderEvictMaxBatch)
	if len(candidates) == 0 {
		return
	}
	for _, o := range candidates {
		err := w.evictOne(ctx, o, pub)
		w.cfg.Metrics.RecordEvictProcessed(int32(w.cfg.VShardID), err == nil)
		if err != nil {
			logger.Error("evict order",
				zap.Uint64("order_id", o.ID),
				zap.String("user_id", o.UserID),
				zap.Error(err))
			// Do not abort the round — the failed order will be
			// reconsidered on the next tick. Later scans are idempotent
			// (ring remember + journal publish + byID delete are all
			// crash-safe under retry).
		}
	}
}

// evictOneRound emits its per-order outcome to metrics after each
// evictOne returns (ADR-0062 M8). Wraps the bare loop so the metric
// label stays close to the error-surfacing point.
//
// evictOne performs the ring→journal→delete three-step for a single
// terminal order under the user's sequencer. Strict ordering matters:
//  1. RememberTerminated: populates the ring so CancelOrder retries
//     across the brief window until the event is committed + byID is
//     cleared still have an idempotent answer.
//  2. Publish OrderEvictedEvent: ADR-0060 TxnProducer.Publish, sync
//     Kafka commit.
//  3. Orders().Delete: removes the order from byID.
//
// A crash between any two steps is harmless: on restart the candidate
// still matches on the next scan (RememberTerminated overwrites with
// identical payload; Publish adds a duplicate journal event which
// shadow consumers apply idempotently; Delete is ErrOrderNotFound-safe).
func (w *VShardWorker) evictOne(ctx context.Context, o *engine.Order, pub journalWriter) error {
	_, err := w.seq.Execute(o.UserID, func(counterSeq uint64) (any, error) {
		acc := w.state.Account(o.UserID)
		acc.RememberTerminated(engine.TerminatedOrderEntry{
			OrderID:       o.ID,
			FinalStatus:   o.Status,
			TerminatedAt:  o.TerminatedAt,
			ClientOrderID: o.ClientOrderID,
			Symbol:        o.Symbol,
		})
		evt := journal.BuildOrderEvictedEvent(journal.OrderEvictedEventInput{
			CounterSeqID:   counterSeq,
			ProducerID:     w.cfg.NodeID,
			AccountVersion: acc.Version(),
			UserID:         o.UserID,
			OrderID:        o.ID,
			Symbol:         o.Symbol,
			FinalStatus:    o.Status,
			TerminatedAt:   o.TerminatedAt,
			ClientOrderID:  o.ClientOrderID,
		})
		if err := pub.Publish(ctx, o.UserID, evt); err != nil {
			return nil, fmt.Errorf("publish OrderEvicted: %w", err)
		}
		if err := w.state.Orders().Delete(o.ID); err != nil && !errors.Is(err, engine.ErrOrderNotFound) {
			return nil, fmt.Errorf("delete order: %w", err)
		}
		return nil, nil
	})
	return err
}

// writeSnapshot captures state + offsets + journal_offset under
// SnapshotMu.Lock (ADR-0060 §5, M7). Holding the write lock across
// Flush + Capture briefly pauses every per-user fn (they're blocked
// on SnapshotMu.RLock via the sequencer barrier) so the snapshot is
// a cross-account-consistent point-in-time view. Save runs outside
// the lock — the marshalled in-memory copy no longer depends on
// live state.
//
// journal_offset is sampled AFTER Flush + with the W-lock held:
// Flush drains every in-flight produce (so all committed records
// are reflected in journalHighOffset) and no new Publish can start
// while we hold the lock (sequencer barrier prevents new fn
// execution). Combined, the recorded journal_offset bounds every
// event whose state effects are visible to Capture.
//
// Critical section length: Flush (ms scale on healthy Kafka) +
// Capture (indirect Copy, 5-50ms for 1M accounts). Upstream fn
// callers briefly queue; cumulative impact is bounded by
// SnapshotInterval cadence (30s typical).
//
// This is a transitional lock per ADR-0060 §5 — once ADR-0061
// moves snapshot production out of Counter the acquisition can be
// removed along with the sequencer barrier.
func (w *VShardWorker) writeSnapshot(ctx context.Context) error {
	start := time.Now()
	w.state.SnapshotMu.Lock()
	if err := w.producer.Flush(ctx); err != nil {
		w.state.SnapshotMu.Unlock()
		return fmt.Errorf("flush before snapshot: %w", err)
	}
	snap := snapshot.Capture(
		int(w.cfg.VShardID),
		w.state, w.seq, w.dedupTable,
		w.svc.Offsets(),
		w.producer.JournalOffsetNext(),
		time.Now().UnixMilli(),
	)
	w.state.SnapshotMu.Unlock()
	key := fmt.Sprintf(SnapshotKeyFormat, w.cfg.VShardID)
	err := snapshot.Save(ctx, w.cfg.Store, key, snap, w.cfg.SnapshotFormat)
	// Emit duration even on error so operators see stalls in the
	// W-lock + Flush + Capture segment (Save may be skewed by blob
	// store latency but the interesting cost is the critical
	// section). ADR-0060 M8.
	w.cfg.Metrics.RecordSnapshotDuration(int32(w.cfg.VShardID), time.Since(start).Seconds())
	w.cfg.Metrics.RecordCounterSeq(int32(w.cfg.VShardID), w.seq.CounterSeq())
	// Stamp snapshot liveness ONLY on Save success. The evict
	// circuit breaker reads this to decide if trade-dump /
	// downstream pipeline is healthy enough to accept evict
	// deletes (ADR-0062 §5).
	if err == nil {
		w.lastSnapshotAtMs.Store(time.Now().UnixMilli())
	}
	return err
}
