// Package worker packages up everything needed to serve one virtual
// shard (ADR-0058 phase 3b). A VShardWorker owns the per-vshard
// state, sequencer, dedup table, transactional producer (with
// epoch-fenced transactional id), trade-event partition consumer,
// and the Service instance the gRPC dispatcher routes to.
//
// Snapshot production moved out of this worker in ADR-0061 — the
// trade-dump snapshot pipeline is the single authoritative writer.
// Counter only READS snapshots on startup (for state restore +
// consumer seek) and relies on trade-dump monitoring
// (`trade_dump_snapshot_age_seconds`) for snapshot-freshness
// alerts.
//
// Lifecycle:
//
//	w, _ := worker.New(Config{...})
//	go w.Run(ctx)        // blocks until ctx done
//	<-w.Ready()          // safe to route traffic after this closes
//	w.Service().Transfer(...)
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
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/counter/internal/clustering"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/counter/internal/metrics"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/counter/snapshot"
	"github.com/xargin/opentrade/pkg/idgen"
)

const (
	// SnapshotKeyFormat is the BlobStore key stem Counter expects
	// trade-dump's snapshot pipeline to use (ADR-0061). Zero-padded
	// so lexicographic listing matches numeric order.
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

	// State plumbing. Store is READ-only from Counter's side per
	// ADR-0061 — trade-dump owns snapshot production. Counter uses
	// it on startup to Load the authoritative snapshot for
	// ShardState restore + trade-event consumer seek.
	Store    snapshot.BlobStore
	DedupTTL time.Duration

	// Behaviour
	DefaultMaxOpenLimitOrders uint32
	SymbolLookup              service.SymbolLookup // optional precision source

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
	// ADR-0061: no apply barrier — Counter doesn't produce snapshots
	// anymore, so there's no Capture step that needs cross-account
	// stop-the-world coordination. trade-dump's shadow pipeline
	// guarantees consistency via single-threaded Apply (ADR-0061 §4.2).
	seq := sequencer.New()
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

	<-ctx.Done()
	logger.Info("vshard worker shutting down")

	consumer.Close()
	wg.Wait()

	// No final snapshot write — trade-dump's snapshot pipeline is
	// the sole snapshot producer (ADR-0061). The last journal events
	// this worker published before shutdown reach trade-dump via
	// counter-journal and land in its next snapshot tick.
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
	// Refresh pendingList gauge on each successful advance — the
	// gauge's denominator is the same list this advancer just
	// shortened, so post-pop is the natural sampling point. Using
	// the injected pending reference keeps test harnesses that
	// bypass Run() (and so never populate w.pending) from NPE'ing.
	if pending != nil {
		w.cfg.Metrics.RecordPendingSize(int32(w.cfg.VShardID), pending.Len())
	}
}

