package snapshotrpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	snapshotpkg "github.com/xargin/opentrade/counter/snapshot"
)

// stubLister is an in-memory BlobLister with tunable behaviour for
// housekeeper tests. Implements snapshotpkg.BlobLister.
type stubLister struct {
	mu          sync.Mutex
	objects     map[string]snapshotpkg.BlobObject
	listErr     error
	deleteErr   error
	listCalls   atomic.Int64
	deleteCalls atomic.Int64
}

func newStubLister() *stubLister {
	return &stubLister{objects: map[string]snapshotpkg.BlobObject{}}
}

func (s *stubLister) put(key string, mtime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = snapshotpkg.BlobObject{Key: key, LastModified: mtime, Size: 1}
}

func (s *stubLister) List(ctx context.Context, prefix string) ([]snapshotpkg.BlobObject, error) {
	s.listCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	var out []snapshotpkg.BlobObject
	for _, o := range s.objects {
		if prefix == "" || (len(o.Key) >= len(prefix) && o.Key[:len(prefix)] == prefix) {
			out = append(out, o)
		}
	}
	return out, nil
}

func (s *stubLister) Delete(ctx context.Context, key string) error {
	s.deleteCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.objects, key)
	return nil
}

func (s *stubLister) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.objects))
	for k := range s.objects {
		out = append(out, k)
	}
	return out
}

// TestHousekeeper_Sweep_DeletesStaleOnDemandOnly walks the core
// filtering rule: only "-ondemand-" keys older than TTL are
// deleted. Periodic keys matching the prefix but NOT containing
// "-ondemand-" MUST survive; recent on-demand keys MUST survive.
func TestHousekeeper_Sweep_DeletesStaleOnDemandOnly(t *testing.T) {
	lister := newStubLister()
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)

	// Mix: periodic (never delete), stale on-demand (delete),
	// fresh on-demand (keep).
	lister.put("vshard-001.pb", now.Add(-2*time.Hour))                        // periodic, stale timestamp
	lister.put("vshard-002.pb", now.Add(-2*time.Hour))                        // periodic, stale timestamp
	lister.put("vshard-001-ondemand-1000.pb", now.Add(-90*time.Minute))       // on-demand, 90min > 1h TTL → delete
	lister.put("vshard-002-ondemand-1200.pb", now.Add(-5*time.Minute))        // on-demand, recent → keep
	lister.put("vshard-003-ondemand-1300.pb", now.Add(-2*time.Hour))          // on-demand, stale → delete

	hk := NewHousekeeper(Housekeeper{
		Lister:       lister,
		TTL:          1 * time.Hour,
		ScanInterval: time.Hour, // irrelevant: we call sweep directly
		Logger:       zap.NewNop(),
		nowFn:        func() time.Time { return now },
	})
	hk.sweep(context.Background())

	remaining := lister.keys()
	wantPresent := map[string]bool{
		"vshard-001.pb":                   true, // periodic always kept
		"vshard-002.pb":                   true,
		"vshard-002-ondemand-1200.pb":     true, // fresh on-demand kept
	}
	wantAbsent := []string{
		"vshard-001-ondemand-1000.pb",
		"vshard-003-ondemand-1300.pb",
	}
	have := make(map[string]bool, len(remaining))
	for _, k := range remaining {
		have[k] = true
	}
	for k := range wantPresent {
		if !have[k] {
			t.Errorf("key %q was deleted but should have been kept; remaining=%v", k, remaining)
		}
	}
	for _, k := range wantAbsent {
		if have[k] {
			t.Errorf("key %q should have been deleted; remaining=%v", k, remaining)
		}
	}
	if got := lister.deleteCalls.Load(); got != 2 {
		t.Errorf("delete calls = %d, want 2", got)
	}
}

// TestHousekeeper_Sweep_ListErrorDoesNotPanic — the main loop
// swallows list errors and logs; the next tick retries. This
// pins that behaviour so a flaky blob store doesn't crash
// trade-dump.
func TestHousekeeper_Sweep_ListErrorDoesNotPanic(t *testing.T) {
	lister := newStubLister()
	lister.listErr = errors.New("s3 503")

	hk := NewHousekeeper(Housekeeper{
		Lister: lister,
		TTL:    1 * time.Hour,
		Logger: zap.NewNop(),
		nowFn:  time.Now,
	})
	// Should return normally (no panic) despite list error.
	hk.sweep(context.Background())
	if got := lister.deleteCalls.Load(); got != 0 {
		t.Errorf("delete called despite list failure: %d", got)
	}
}

// TestHousekeeper_Sweep_DeleteErrorContinues — if one Delete
// fails, the sweep MUST still try the rest of the batch. Protects
// against a single stuck key stalling cleanup of 255 other
// vshards' garbage.
func TestHousekeeper_Sweep_DeleteErrorContinues(t *testing.T) {
	lister := newStubLister()
	lister.deleteErr = errors.New("s3 access denied on one key")
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		lister.put(keyForVshard(i), now.Add(-2*time.Hour))
	}

	hk := NewHousekeeper(Housekeeper{
		Lister: lister,
		TTL:    1 * time.Hour,
		Logger: zap.NewNop(),
		nowFn:  func() time.Time { return now },
	})
	hk.sweep(context.Background())

	// Delete attempted for all 5 on-demand keys despite errors.
	if got := lister.deleteCalls.Load(); got != 5 {
		t.Errorf("delete calls = %d, want 5 (must try each key)", got)
	}
	// All still present because the stub returns error for every
	// Delete — but the sweep attempted each one.
	if len(lister.keys()) != 5 {
		t.Errorf("want all keys still present after failed deletes, got %d",
			len(lister.keys()))
	}
}

// TestHousekeeper_Run_InitialSweepThenCancel drives the full Run
// lifecycle: skew delay → first sweep → ctx cancel → clean return.
// The initial skew is 10s in production; tests don't have that
// budget, so we verify the skip-timer behaviour by cancelling
// before the first tick fires and confirming Run returns promptly.
func TestHousekeeper_Run_InitialSweepThenCancel(t *testing.T) {
	lister := newStubLister()
	hk := NewHousekeeper(Housekeeper{
		Lister: lister,
		TTL:    1 * time.Hour,
		Logger: zap.NewNop(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		hk.Run(ctx)
		close(done)
	}()

	// Cancel before the 10s initial skew would fire.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancel")
	}
}

// TestHousekeeper_Run_NilListerExitsImmediately — wiring safety
// valve. Callers that construct Housekeeper with a nil Lister
// (e.g. snap pipeline disabled, BlobStore doesn't implement
// BlobLister) should not hang a goroutine.
func TestHousekeeper_Run_NilListerExitsImmediately(t *testing.T) {
	hk := NewHousekeeper(Housekeeper{
		Lister: nil,
		Logger: zap.NewNop(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		hk.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run with nil Lister should exit immediately")
	}
}

// TestHousekeeper_Sweep_RespectsOnDemandGlob isolates the glob
// parameter: a custom prefix only considers matching keys.
func TestHousekeeper_Sweep_RespectsOnDemandGlob(t *testing.T) {
	lister := newStubLister()
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	// Two glob families. Only the "vshard-" family has on-demand
	// markers; the other should be completely ignored by our glob.
	lister.put("vshard-001-ondemand-1.pb", now.Add(-2*time.Hour)) // stale
	lister.put("other-001-ondemand-1.pb", now.Add(-2*time.Hour)) // stale but wrong glob

	hk := NewHousekeeper(Housekeeper{
		Lister:       lister,
		OnDemandGlob: "vshard-",
		TTL:          1 * time.Hour,
		Logger:       zap.NewNop(),
		nowFn:        func() time.Time { return now },
	})
	hk.sweep(context.Background())

	have := make(map[string]bool)
	for _, k := range lister.keys() {
		have[k] = true
	}
	if have["vshard-001-ondemand-1.pb"] {
		t.Errorf("vshard-001-ondemand-1.pb should have been deleted")
	}
	if !have["other-001-ondemand-1.pb"] {
		t.Errorf("other-001-ondemand-1.pb should have been preserved (outside glob)")
	}
}

func keyForVshard(i int) string {
	return timePrefixed(i)
}

func timePrefixed(i int) string {
	// deliberately simple; avoids pulling fmt into hot loop above.
	return "vshard-00" + string(rune('0'+byte(i))) + "-ondemand-1000.pb"
}
