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
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/clustering"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
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
	seq := sequencer.New()
	dt := dedup.New(w.cfg.DedupTTL)

	key := fmt.Sprintf(SnapshotKeyFormat, w.cfg.VShardID)
	snap, err := snapshot.Load(ctx, w.cfg.Store, key)
	var offsets map[int32]int64
	switch {
	case err == nil:
		if err := snapshot.Restore(int(w.cfg.VShardID), state, seq, dt, snap); err != nil {
			return fmt.Errorf("snapshot restore: %w", err)
		}
		offsets = snapshot.OffsetsSliceToMap(snap.Offsets)
		logger.Info("restored from snapshot",
			zap.Int("version", snap.Version),
			zap.Uint64("counter_seq", snap.CounterSeq),
			zap.Int("accounts", len(snap.Accounts)),
			zap.Int("orders", len(snap.Orders)),
			zap.Int("partitions", len(offsets)))
	case errors.Is(err, os.ErrNotExist):
		logger.Info("no snapshot found, starting fresh")
	default:
		return fmt.Errorf("snapshot load: %w", err)
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
	close(w.ready)

	consumer, err := journal.NewTradePartitionConsumer(journal.TradePartitionConsumerConfig{
		Brokers:    w.cfg.Brokers,
		ClientID:   fmt.Sprintf("%s-vshard-%03d-trade", w.cfg.NodeID, w.cfg.VShardID),
		Topic:      w.cfg.TradeEventTopic,
		Partitions: []int32{int32(w.cfg.VShardID)},
		Offsets:    offsets,
	}, svc, logger)
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
			w.evictOneRound(ctx, logger, w.producer)
		}
	}
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
		if err := w.evictOne(ctx, o, pub); err != nil {
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

// writeSnapshot captures state + offsets after flushing the Kafka
// producer (ADR-0048 output flush barrier). All reads happen via the
// Service / engine APIs that are concurrency-safe, so it runs alongside
// the consumer without extra locking.
func (w *VShardWorker) writeSnapshot(ctx context.Context) error {
	if err := w.producer.Flush(ctx); err != nil {
		return fmt.Errorf("flush before snapshot: %w", err)
	}
	snap := snapshot.Capture(
		int(w.cfg.VShardID),
		w.state, w.seq, w.dedupTable,
		w.svc.Offsets(),
		time.Now().UnixMilli(),
	)
	key := fmt.Sprintf(SnapshotKeyFormat, w.cfg.VShardID)
	return snapshot.Save(ctx, w.cfg.Store, key, snap, w.cfg.SnapshotFormat)
}
