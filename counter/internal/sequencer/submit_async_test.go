package sequencer

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSubmitAsync_ReturnsImmediately verifies the fire-and-forget
// contract: SubmitAsync does not block on fn execution.
func TestSubmitAsync_ReturnsImmediately(t *testing.T) {
	s := New()

	block := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	start := time.Now()
	s.SubmitAsync("u1",
		func(_ uint64) error {
			close(block)
			<-release
			return nil
		},
		func(err error) {
			done <- err
		},
	)
	elapsed := time.Since(start)
	if elapsed > 50*time.Millisecond {
		t.Fatalf("SubmitAsync blocked for %v (expected ~0)", elapsed)
	}

	<-block
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("cb got err: %v", err)
	}
}

// TestSubmitAsync_InvokesCallbackWithError ensures fn's error is
// forwarded to cb verbatim.
func TestSubmitAsync_InvokesCallbackWithError(t *testing.T) {
	s := New()
	target := errors.New("deliberate")
	done := make(chan error, 1)
	s.SubmitAsync("u1",
		func(_ uint64) error { return target },
		func(err error) { done <- err },
	)
	got := <-done
	if !errors.Is(got, target) {
		t.Fatalf("cb got %v, want %v", got, target)
	}
}

// TestSubmitAsync_NilCallbackNoops verifies cb=nil is legal — fn runs
// but no callback fires.
func TestSubmitAsync_NilCallbackNoops(t *testing.T) {
	s := New()
	var ran atomic.Bool
	s.SubmitAsync("u1", func(_ uint64) error {
		ran.Store(true)
		return nil
	}, nil)
	// Wait for fn to run.
	for i := 0; i < 100; i++ {
		if ran.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !ran.Load() {
		t.Fatal("fn never ran with nil callback")
	}
}

// TestSubmitAsync_FIFOPerUser — ADR-0060 preserves per-user FIFO even
// under SubmitAsync. Submit 50 tasks on u1 in program order; assert
// they execute in that same order via a monotonically-increasing shared
// counter.
func TestSubmitAsync_FIFOPerUser(t *testing.T) {
	s := New()
	const n = 50
	var mu sync.Mutex
	order := make([]int, 0, n)
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		i := i
		s.SubmitAsync("u1",
			func(_ uint64) error {
				mu.Lock()
				order = append(order, i)
				mu.Unlock()
				return nil
			},
			func(_ error) { done <- struct{}{} },
		)
	}
	for i := 0; i < n; i++ {
		<-done
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < n; i++ {
		if order[i] != i {
			t.Fatalf("order[%d]=%d, want %d — FIFO broken", i, order[i], i)
		}
	}
}

// TestSubmitAsync_CountersSharedWithExecute verifies Execute and
// SubmitAsync draw counterSeq from the same atomic source — they
// share the per-user sequencer, so interleaving two APIs must still
// yield strictly-monotonic seqs.
func TestSubmitAsync_CountersSharedWithExecute(t *testing.T) {
	s := New()
	var mu sync.Mutex
	seqs := []uint64{}
	done := make(chan struct{}, 4)

	// Alternate Execute and SubmitAsync calls on the same user.
	go func() {
		_, _ = s.Execute("u1", func(seq uint64) (any, error) {
			mu.Lock()
			seqs = append(seqs, seq)
			mu.Unlock()
			return nil, nil
		})
		done <- struct{}{}
	}()
	s.SubmitAsync("u1", func(seq uint64) error {
		mu.Lock()
		seqs = append(seqs, seq)
		mu.Unlock()
		return nil
	}, func(_ error) { done <- struct{}{} })
	go func() {
		_, _ = s.Execute("u1", func(seq uint64) (any, error) {
			mu.Lock()
			seqs = append(seqs, seq)
			mu.Unlock()
			return nil, nil
		})
		done <- struct{}{}
	}()
	s.SubmitAsync("u1", func(seq uint64) error {
		mu.Lock()
		seqs = append(seqs, seq)
		mu.Unlock()
		return nil
	}, func(_ error) { done <- struct{}{} })

	for i := 0; i < 4; i++ {
		<-done
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 4 {
		t.Fatalf("collected %d seqs, want 4", len(seqs))
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("seqs not strictly monotonic: %v", seqs)
		}
	}
}

// TestSubmitAsync_IsolationAcrossUsers — two users run concurrently
// even while one is blocked. Locking u1's drain should not slow u2.
func TestSubmitAsync_IsolationAcrossUsers(t *testing.T) {
	s := New()
	block := make(chan struct{})
	u1Done := make(chan struct{}, 1)
	u2Done := make(chan struct{}, 1)

	s.SubmitAsync("u1",
		func(_ uint64) error {
			<-block
			return nil
		},
		func(_ error) { u1Done <- struct{}{} },
	)
	// u2 must NOT wait for u1.
	s.SubmitAsync("u2",
		func(_ uint64) error {
			return nil
		},
		func(_ error) { u2Done <- struct{}{} },
	)

	select {
	case <-u2Done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("u2 blocked by u1 — per-user isolation broken")
	}
	close(block)
	<-u1Done
}

// TestSubmitAsync_IdleExitThenResume — after the drain exits on idle
// timeout, a fresh SubmitAsync must spin up a replacement.
func TestSubmitAsync_IdleExitThenResume(t *testing.T) {
	s := New(WithIdleTimeout(30 * time.Millisecond))
	done1 := make(chan struct{}, 1)
	s.SubmitAsync("u1", func(_ uint64) error { return nil },
		func(_ error) { done1 <- struct{}{} })
	<-done1
	// Let the drain idle out.
	time.Sleep(80 * time.Millisecond)
	done2 := make(chan struct{}, 1)
	s.SubmitAsync("u1", func(_ uint64) error { return nil },
		func(_ error) { done2 <- struct{}{} })
	select {
	case <-done2:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("post-idle resume did not run")
	}
}

// TestQueueDepth_ReflectsPending verifies the new QueueDepth metric
// correctly reports pending tasks under drain pressure.
func TestQueueDepth_ReflectsPending(t *testing.T) {
	s := New()
	if got := s.QueueDepth("u1"); got != 0 {
		t.Fatalf("initial QueueDepth = %d, want 0", got)
	}
	// Block u1's drain so the rest queue up.
	release := make(chan struct{})
	started := make(chan struct{})
	s.SubmitAsync("u1",
		func(_ uint64) error {
			close(started)
			<-release
			return nil
		},
		nil,
	)
	<-started
	// Queue up 10 followers.
	for i := 0; i < 10; i++ {
		s.SubmitAsync("u1", func(_ uint64) error { return nil }, nil)
	}
	// Poll for queue depth to stabilize at 10.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.QueueDepth("u1") == 10 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := s.QueueDepth("u1"); got != 10 {
		t.Fatalf("QueueDepth = %d, want 10", got)
	}
	close(release)
	// After drain catches up, depth returns to 0.
	for i := 0; i < 200; i++ {
		if s.QueueDepth("u1") == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("QueueDepth never returned to 0; got %d", s.QueueDepth("u1"))
}

// TestSubmitAsync_HighConcurrency exercises the unbounded queue + the
// Idle↔Running handoff protocol under stress. No assertion beyond "no
// race / no deadlock / correct completion count".
func TestSubmitAsync_HighConcurrency(t *testing.T) {
	s := New()
	const workers = 16
	const perWorker = 500
	total := workers * perWorker
	var completed atomic.Int64
	done := make(chan struct{})
	cb := func(_ error) {
		if int(completed.Add(1)) == total {
			close(done)
		}
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			uid := "u" + string(rune('A'+w))
			for i := 0; i < perWorker; i++ {
				s.SubmitAsync(uid, func(_ uint64) error { return nil }, cb)
			}
		}(w)
	}
	wg.Wait()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("completed %d / %d", completed.Load(), total)
	}
}
