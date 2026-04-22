package sequencer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestApplyBarrier_WLockBlocksTasks verifies that when the
// snapshot-side holds the W lock, queued tasks do NOT execute
// until it's released. This is the stop-the-world invariant the
// ADR-0060 §5 Capture path relies on.
func TestApplyBarrier_WLockBlocksTasks(t *testing.T) {
	var barrier sync.RWMutex
	s := New(WithApplyBarrier(&barrier), WithIdleTimeout(50*time.Millisecond))

	barrier.Lock() // hold W — simulates Capture in progress
	var ran atomic.Int32
	done := make(chan struct{})
	s.SubmitAsync("u1", func(counterSeq uint64) error {
		ran.Add(1)
		return nil
	}, func(err error) {
		close(done)
	})

	// Give the drain goroutine time to pick up the task. Under the
	// barrier it should NOT reach ran.Add.
	time.Sleep(30 * time.Millisecond)
	if ran.Load() != 0 {
		t.Fatal("task ran while W lock was held")
	}

	barrier.Unlock()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("task did not run after W unlock")
	}
	if ran.Load() != 1 {
		t.Fatalf("ran = %d, want 1", ran.Load())
	}
}

// TestApplyBarrier_ConcurrentTasksUseRLock verifies multiple tasks
// for different users can execute concurrently with R-locks
// (RWMutex allows multiple readers). Earlier R-lock implementations
// have regressed to exclusive locks; this test catches that.
func TestApplyBarrier_ConcurrentTasksUseRLock(t *testing.T) {
	var barrier sync.RWMutex
	s := New(WithApplyBarrier(&barrier), WithIdleTimeout(50*time.Millisecond))

	// 4 users in flight simultaneously; each fn holds a short sleep
	// to force overlap. With exclusive locks wall time would be
	// ~4×sleep; with R locks it's ~sleep.
	const users = 4
	const fnDuration = 100 * time.Millisecond
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < users; i++ {
		wg.Add(1)
		userID := string(rune('a' + i))
		s.SubmitAsync(userID, func(counterSeq uint64) error {
			time.Sleep(fnDuration)
			return nil
		}, func(err error) { wg.Done() })
	}
	wg.Wait()
	elapsed := time.Since(start)
	// Allow ~2x margin for Go scheduler noise + goroutine spawn.
	if elapsed > 3*fnDuration {
		t.Fatalf("elapsed = %v, tasks are serialising (expected ~%v with R-lock concurrency)", elapsed, fnDuration)
	}
}

// TestApplyBarrier_WLockWaitsForRunningTasks covers the safety
// invariant: acquiring W waits for any already-running fn to
// finish (RWMutex semantics). Ensures Capture never interrupts
// mid-fn so state is always read at a task boundary.
func TestApplyBarrier_WLockWaitsForRunningTasks(t *testing.T) {
	var barrier sync.RWMutex
	s := New(WithApplyBarrier(&barrier), WithIdleTimeout(50*time.Millisecond))

	fnStarted := make(chan struct{})
	fnMayFinish := make(chan struct{})
	fnDone := make(chan struct{})
	s.SubmitAsync("u1", func(counterSeq uint64) error {
		close(fnStarted)
		<-fnMayFinish
		return nil
	}, func(err error) { close(fnDone) })

	<-fnStarted

	// Start a goroutine that attempts W. It should block until
	// the fn finishes.
	wAcquired := make(chan struct{})
	go func() {
		barrier.Lock()
		close(wAcquired)
		barrier.Unlock()
	}()

	select {
	case <-wAcquired:
		t.Fatal("W lock acquired while fn still running")
	case <-time.After(50 * time.Millisecond):
		// Good: W is blocked.
	}

	close(fnMayFinish)
	<-fnDone
	select {
	case <-wAcquired:
	case <-time.After(time.Second):
		t.Fatal("W lock never acquired after fn finished")
	}
}

// TestApplyBarrier_NilBarrierIsBackCompat confirms the default
// (no barrier) still works — legacy tests and non-worker paths
// don't break.
func TestApplyBarrier_NilBarrierIsBackCompat(t *testing.T) {
	s := New()
	done := make(chan struct{})
	s.SubmitAsync("u1", func(counterSeq uint64) error { return nil }, func(err error) { close(done) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("task did not run without barrier")
	}
}
