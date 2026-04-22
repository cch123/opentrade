package worker

import (
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/internal/clustering"
)

// newHealthCheckWorker returns a worker primed with the breaker
// threshold but no Kafka / gRPC dependencies (health-check reads
// only atomics + config).
func newHealthCheckWorker(t *testing.T, ckptStale time.Duration) *VShardWorker {
	t.Helper()
	return &VShardWorker{
		cfg: Config{
			VShardID:                  clustering.VShardID(7),
			Logger:                    zap.NewNop(),
			EvictCheckpointStaleAfter: ckptStale,
		},
	}
}

// TestEvictHealthCheck_CleanCheckpoint — fresh checkpoint → allow.
func TestEvictHealthCheck_CleanCheckpoint(t *testing.T) {
	w := newHealthCheckWorker(t, time.Minute)
	w.lastCheckpointAtMs.Store(time.Now().UnixMilli())
	if !w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("health-check should have passed")
	}
}

// TestEvictHealthCheck_ColdStart — checkpoint never set → allow.
// Avoids a fresh pod halting itself before its first checkpoint
// lands; retention window (1h default) shields against premature
// evict anyway.
func TestEvictHealthCheck_ColdStart(t *testing.T) {
	w := newHealthCheckWorker(t, time.Minute)
	if !w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("cold-start health-check should have passed (zero timestamp)")
	}
}

// TestEvictHealthCheck_StaleCheckpoint — checkpoint ts older than
// threshold → halt.
func TestEvictHealthCheck_StaleCheckpoint(t *testing.T) {
	w := newHealthCheckWorker(t, 100*time.Millisecond)
	stale := time.Now().Add(-time.Second).UnixMilli()
	w.lastCheckpointAtMs.Store(stale)
	if w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("health-check should have halted on stale checkpoint")
	}
}

// TestEvictHealthCheck_JustUnderThreshold — boundary: age slightly
// under threshold should PASS (no halt). Guards off-by-one regressions.
func TestEvictHealthCheck_JustUnderThreshold(t *testing.T) {
	w := newHealthCheckWorker(t, time.Second)
	recent := time.Now().Add(-500 * time.Millisecond).UnixMilli()
	w.lastCheckpointAtMs.Store(recent)
	if !w.evictHealthCheck(zap.NewNop()) {
		t.Fatal("health-check should pass when age is under threshold")
	}
}
