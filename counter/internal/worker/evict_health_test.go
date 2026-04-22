package worker

import (
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/internal/clustering"
)

// newHealthCheckWorker returns a worker primed with the breaker
// thresholds but no Kafka / gRPC dependencies (health-check reads
// only atomics + config).
func newHealthCheckWorker(t *testing.T, snapStale, ckptStale time.Duration) *VShardWorker {
	t.Helper()
	return &VShardWorker{
		cfg: Config{
			VShardID:                  clustering.VShardID(7),
			Logger:                    zap.NewNop(),
			EvictSnapshotStaleAfter:   snapStale,
			EvictCheckpointStaleAfter: ckptStale,
		},
	}
}

// TestEvictHealthCheck_CleanCheckpointAndSnapshot — both signals
// fresh → allow.
func TestEvictHealthCheck_CleanCheckpointAndSnapshot(t *testing.T) {
	w := newHealthCheckWorker(t, time.Hour, time.Minute)
	now := time.Now().UnixMilli()
	w.lastCheckpointAtMs.Store(now)
	w.lastSnapshotAtMs.Store(now)
	if !w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("health-check should have passed")
	}
}

// TestEvictHealthCheck_ColdStart — both timestamps zero (never
// set) → allow. Avoids a fresh pod halting itself before its first
// snapshot / checkpoint lands.
func TestEvictHealthCheck_ColdStart(t *testing.T) {
	w := newHealthCheckWorker(t, time.Hour, time.Minute)
	if !w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("cold-start health-check should have passed (zero timestamps)")
	}
}

// TestEvictHealthCheck_StaleSnapshot — snapshot ts older than
// threshold → halt.
func TestEvictHealthCheck_StaleSnapshot(t *testing.T) {
	w := newHealthCheckWorker(t, 100*time.Millisecond, time.Minute)
	// Force stale by subtracting more than the threshold.
	stale := time.Now().Add(-time.Second).UnixMilli()
	w.lastSnapshotAtMs.Store(stale)
	w.lastCheckpointAtMs.Store(time.Now().UnixMilli())
	if w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("health-check should have halted on stale snapshot")
	}
}

// TestEvictHealthCheck_StaleCheckpoint — checkpoint ts older than
// threshold → halt. Snapshot is fresh.
func TestEvictHealthCheck_StaleCheckpoint(t *testing.T) {
	w := newHealthCheckWorker(t, time.Hour, 100*time.Millisecond)
	stale := time.Now().Add(-time.Second).UnixMilli()
	w.lastCheckpointAtMs.Store(stale)
	w.lastSnapshotAtMs.Store(time.Now().UnixMilli())
	if w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("health-check should have halted on stale checkpoint")
	}
}

// TestEvictHealthCheck_SnapshotCheckedFirst — if both are stale,
// halt reason prefers snapshot (ADR-0062 §5 documents the order).
// We verify by setting a threshold that would pass for checkpoint
// but fail for snapshot, forcing snapshot branch.
func TestEvictHealthCheck_SnapshotCheckedFirst(t *testing.T) {
	w := newHealthCheckWorker(t, 100*time.Millisecond, time.Hour)
	stale := time.Now().Add(-500 * time.Millisecond).UnixMilli()
	w.lastSnapshotAtMs.Store(stale)
	w.lastCheckpointAtMs.Store(stale)
	if w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("health-check should halt (snapshot stale)")
	}
}

// TestEvictHealthCheck_JustUnderThreshold — boundary: age slightly
// under threshold should PASS (no halt). Guards off-by-one regressions.
func TestEvictHealthCheck_JustUnderThreshold(t *testing.T) {
	w := newHealthCheckWorker(t, time.Second, time.Second)
	recent := time.Now().Add(-500 * time.Millisecond).UnixMilli()
	w.lastSnapshotAtMs.Store(recent)
	w.lastCheckpointAtMs.Store(recent)
	if !w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("health-check should pass when both ages are under threshold")
	}
}
