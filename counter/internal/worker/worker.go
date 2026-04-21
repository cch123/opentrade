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

	// TransactionalIDFormat yields Kafka's per-vshard fencing identity
	// (ADR-0058 §Fencing). Embedding the epoch means a new owner's
	// InitProducerID automatically fences any in-flight transaction
	// left by the previous epoch.
	TransactionalIDFormat = "counter-vshard-%03d-ep-%d"

	defaultSnapshotFlushTimeout = 3 * time.Second
	defaultShutdownFlushTimeout = 10 * time.Second
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

	txnID := fmt.Sprintf(TransactionalIDFormat, w.cfg.VShardID, w.cfg.Epoch)
	producer, err := journal.NewTxnProducer(ctx, journal.TxnProducerConfig{
		Brokers:               w.cfg.Brokers,
		ClientID:              fmt.Sprintf("%s-vshard-%03d-txn", w.cfg.NodeID, w.cfg.VShardID),
		TransactionalID:       txnID,
		JournalTopic:          w.cfg.JournalTopic,
		OrderEventTopic:       w.cfg.OrderEventTopic,
		OrderEventTopicPrefix: w.cfg.OrderEventTopicPrefix,
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
		ProducerID:                fmt.Sprintf("%s-vshard-%03d", w.cfg.NodeID, w.cfg.VShardID),
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
