package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/internal/clustering"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/counter/internal/snapshot"
	"github.com/xargin/opentrade/pkg/shard"
)

// Manager subscribes to the node's assigned (vshard, epoch) set and
// runs the matching VShardWorker for each entry. On every assignment
// change it reconciles the running set with the desired set:
//
//   - vshard dropped from desired → stop the worker (graceful snapshot
//     on the way out)
//   - vshard stayed but epoch bumped → stop + restart (fresh producer
//     with the new transactional id, fencing the old)
//   - vshard appeared in desired → start a new worker
//
// Manager also implements the Router contract the gRPC dispatcher
// depends on: Lookup maps a user_id to the owning vshard's Service, or
// returns (nil, false) so the dispatcher can reply FailedPrecondition
// and let the BFF refresh its assignment cache.
type Manager struct {
	cluster  *clustering.Cluster
	template WorkerTemplate
	logger   *zap.Logger

	mu      sync.RWMutex
	running map[clustering.VShardID]*workerRun
}

// WorkerTemplate is the shared half of worker.Config — everything that
// stays constant across every vshard on this node. VShardID and Epoch
// are filled in per-worker when the Manager starts one.
type WorkerTemplate struct {
	NodeID      string
	VShardCount int

	Brokers               []string
	JournalTopic          string
	TradeEventTopic       string
	OrderEventTopic       string
	OrderEventTopicPrefix string

	Store            snapshot.BlobStore
	SnapshotFormat   snapshot.Format
	SnapshotInterval time.Duration
	DedupTTL         time.Duration

	DefaultMaxOpenLimitOrders uint32
	SymbolLookup              service.SymbolLookup
}

// workerRun tracks one running VShardWorker plus the goroutine plumbing
// needed to stop it cleanly.
type workerRun struct {
	worker *VShardWorker
	cancel context.CancelFunc
	done   chan struct{}
	epoch  uint64
}

// NewManager wires the control + data plane for this node. cluster and
// template are required; template.Store / Brokers / NodeID /
// VShardCount are the minimums (worker.New validates the rest).
func NewManager(cluster *clustering.Cluster, template WorkerTemplate, logger *zap.Logger) (*Manager, error) {
	if cluster == nil {
		return nil, errors.New("worker: Manager requires Cluster")
	}
	if template.NodeID == "" {
		return nil, errors.New("worker: Manager requires template.NodeID")
	}
	if template.VShardCount <= 0 {
		return nil, errors.New("worker: Manager requires template.VShardCount > 0")
	}
	if template.Store == nil {
		return nil, errors.New("worker: Manager requires template.Store")
	}
	if len(template.Brokers) == 0 {
		return nil, errors.New("worker: Manager requires template.Brokers")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Manager{
		cluster:  cluster,
		template: template,
		logger:   logger,
		running:  make(map[clustering.VShardID]*workerRun),
	}, nil
}

// Run subscribes to WatchAssignedAssignments and reconciles until ctx
// is cancelled. On exit every running worker is stopped and its final
// snapshot is flushed.
func (m *Manager) Run(ctx context.Context) error {
	ch, err := m.cluster.WatchAssignedAssignments(ctx)
	if err != nil {
		return fmt.Errorf("watch assignments: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return ctx.Err()
		case desired, ok := <-ch:
			if !ok {
				// Watcher died before ctx. Drain everything and
				// surface so the caller can restart the Manager.
				m.stopAll()
				return errors.New("worker: assignment watch closed")
			}
			m.reconcile(ctx, desired)
		}
	}
}

// Lookup implements the server.Router contract. It resolves user_id →
// vshard via the same stable hash used by BFF routing (pkg/shard) and
// returns the Service if a ready worker is running. Otherwise it
// returns (nil, false) so the caller can emit FailedPrecondition and
// let the client retry after refreshing its routing view.
func (m *Manager) Lookup(userID string) (*service.Service, bool) {
	vid := clustering.VShardID(shard.Index(userID, m.template.VShardCount))
	m.mu.RLock()
	run, ok := m.running[vid]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	// Don't hand out a not-yet-ready Service — state hasn't been
	// restored so reads would be wrong and writes would publish on an
	// unopened producer.
	select {
	case <-run.worker.Ready():
	default:
		return nil, false
	}
	svc := run.worker.Service()
	if svc == nil {
		return nil, false
	}
	return svc, true
}

// reconcile is the core convergence loop: stop workers no longer in
// the desired set (or whose epoch bumped), then start any missing
// ones. Runs under m.mu so Lookup sees a consistent map.
func (m *Manager) reconcile(ctx context.Context, desired []clustering.Assignment) {
	desiredByID := make(map[clustering.VShardID]clustering.Assignment, len(desired))
	for _, a := range desired {
		desiredByID[a.VShardID] = a
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop workers that dropped or whose epoch bumped.
	for vid, run := range m.running {
		want, ok := desiredByID[vid]
		if !ok {
			m.logger.Info("stopping vshard worker (no longer assigned)",
				zap.Int("vshard", int(vid)),
				zap.Uint64("epoch", run.epoch))
			m.stopUnlocked(vid, run)
			continue
		}
		if want.Epoch != run.epoch {
			m.logger.Info("restarting vshard worker (epoch bumped)",
				zap.Int("vshard", int(vid)),
				zap.Uint64("old_epoch", run.epoch),
				zap.Uint64("new_epoch", want.Epoch))
			m.stopUnlocked(vid, run)
			// fall through to "start" loop below
		}
	}

	// Start workers missing after stop pass.
	for vid, a := range desiredByID {
		if _, ok := m.running[vid]; ok {
			continue
		}
		if err := m.startUnlocked(ctx, a); err != nil {
			m.logger.Error("start vshard worker failed",
				zap.Int("vshard", int(vid)),
				zap.Uint64("epoch", a.Epoch),
				zap.Error(err))
		}
	}
}

// startUnlocked builds a fresh VShardWorker, spawns its Run goroutine,
// and registers it. Caller must hold m.mu.
func (m *Manager) startUnlocked(ctx context.Context, a clustering.Assignment) error {
	cfg := Config{
		VShardID:                  a.VShardID,
		Epoch:                     a.Epoch,
		NodeID:                    m.template.NodeID,
		VShardCount:               m.template.VShardCount,
		Brokers:                   m.template.Brokers,
		JournalTopic:              m.template.JournalTopic,
		TradeEventTopic:           m.template.TradeEventTopic,
		OrderEventTopic:           m.template.OrderEventTopic,
		OrderEventTopicPrefix:     m.template.OrderEventTopicPrefix,
		Store:                     m.template.Store,
		SnapshotFormat:            m.template.SnapshotFormat,
		SnapshotInterval:          m.template.SnapshotInterval,
		DedupTTL:                  m.template.DedupTTL,
		DefaultMaxOpenLimitOrders: m.template.DefaultMaxOpenLimitOrders,
		SymbolLookup:              m.template.SymbolLookup,
		Logger:                    m.logger,
	}
	w, err := New(cfg)
	if err != nil {
		return fmt.Errorf("worker.New: %w", err)
	}
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(wctx); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.Error("vshard worker Run exited",
				zap.Int("vshard", int(a.VShardID)),
				zap.Uint64("epoch", a.Epoch),
				zap.Error(err))
		}
	}()
	m.running[a.VShardID] = &workerRun{
		worker: w,
		cancel: cancel,
		done:   done,
		epoch:  a.Epoch,
	}
	m.logger.Info("started vshard worker",
		zap.Int("vshard", int(a.VShardID)),
		zap.Uint64("epoch", a.Epoch))
	return nil
}

// stopUnlocked cancels one worker and waits for Run to return. Caller
// must hold m.mu. Releases the lock while waiting for Run so Lookup
// callers aren't blocked on a shutdown snapshot. After the worker has
// drained (flushed its transactional producer + final snapshot), this
// method also checks whether the stop was the old-owner leg of a cold
// handoff (ADR-0058 §7) and, if so, promotes the assignment from
// MIGRATING to HANDOFF_READY so the coordinator can complete the swap.
func (m *Manager) stopUnlocked(vid clustering.VShardID, run *workerRun) {
	delete(m.running, vid)
	m.mu.Unlock()
	run.cancel()
	<-run.done
	m.promoteHandoffReadyIfMigrating(vid, run.epoch)
	m.mu.Lock()
}

// promoteHandoffReadyIfMigrating is best-effort: we run it after every
// worker stop because the stop itself doesn't carry a reason. On a
// normal node shutdown / epoch bump the assignment state won't be
// MIGRATING and we skip; only the migration path hits the CAS write.
func (m *Manager) promoteHandoffReadyIfMigrating(vid clustering.VShardID, epoch uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a, err := clustering.ReadAssignment(ctx, m.cluster.Client(), m.cluster.Keys(), vid)
	if err != nil {
		if !errors.Is(err, clustering.ErrAssignmentMissing) {
			m.logger.Debug("handoff check: read assignment",
				zap.Int("vshard", int(vid)), zap.Error(err))
		}
		return
	}
	if a.Owner != m.template.NodeID || a.State != clustering.StateMigrating || a.Epoch != epoch {
		return
	}
	if err := clustering.MarkHandoffReady(ctx,
		m.cluster.Client(), m.cluster.Keys(),
		vid, m.template.NodeID, epoch,
	); err != nil {
		m.logger.Warn("mark HANDOFF_READY failed",
			zap.Int("vshard", int(vid)),
			zap.Uint64("epoch", epoch),
			zap.Error(err))
		return
	}
	m.logger.Info("migration handoff ready",
		zap.Int("vshard", int(vid)),
		zap.Uint64("epoch", epoch),
		zap.String("target", a.Target))
}

// stopAll cancels every running worker and waits for them to drain.
// Used on Manager.Run exit (normal shutdown or watch death).
func (m *Manager) stopAll() {
	m.mu.Lock()
	snapshotRuns := make([]*workerRun, 0, len(m.running))
	for vid, run := range m.running {
		snapshotRuns = append(snapshotRuns, run)
		delete(m.running, vid)
	}
	m.mu.Unlock()

	var wg sync.WaitGroup
	for _, run := range snapshotRuns {
		wg.Add(1)
		go func(r *workerRun) {
			defer wg.Done()
			r.cancel()
			<-r.done
		}(run)
	}
	wg.Wait()
}

// AssignedVShards returns the current live set for diagnostics /
// /healthz / operator tooling. The result is a snapshot; keep it
// cheap.
func (m *Manager) AssignedVShards() []clustering.VShardID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]clustering.VShardID, 0, len(m.running))
	for vid := range m.running {
		out = append(out, vid)
	}
	return out
}
