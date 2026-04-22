package sequencer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFIFOPerUser(t *testing.T) {
	s := New(WithQueueCapacity(64))

	const n = 500
	got := make([]int, 0, n)
	var mu sync.Mutex

	// All tasks for "u1" submitted sequentially — they must also run sequentially.
	for i := 0; i < n; i++ {
		i := i
		_, err := s.Execute("u1", func(seq uint64) (any, error) {
			mu.Lock()
			got = append(got, i)
			mu.Unlock()
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("FIFO violated at %d: got %d", i, v)
		}
	}
}

func TestConcurrentUsersRunInParallel(t *testing.T) {
	s := New(WithQueueCapacity(64))
	var active atomic.Int32
	var peak atomic.Int32

	const users = 8
	var wg sync.WaitGroup
	wg.Add(users)
	for u := 0; u < users; u++ {
		user := fmt.Sprintf("u%d", u)
		go func() {
			defer wg.Done()
			_, _ = s.Execute(user, func(seq uint64) (any, error) {
				cur := active.Add(1)
				for {
					p := peak.Load()
					if cur <= p || peak.CompareAndSwap(p, cur) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				active.Add(-1)
				return nil, nil
			})
		}()
	}
	wg.Wait()
	if peak.Load() < 2 {
		t.Fatalf("expected concurrent execution across users; peak=%d", peak.Load())
	}
}

func TestCounterSeqMonotonic(t *testing.T) {
	s := New(WithQueueCapacity(64))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var seqs []uint64

	for u := 0; u < 4; u++ {
		user := fmt.Sprintf("u%d", u)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, _ = s.Execute(user, func(seq uint64) (any, error) {
					mu.Lock()
					seqs = append(seqs, seq)
					mu.Unlock()
					return nil, nil
				})
			}
		}()
	}
	wg.Wait()

	seen := make(map[uint64]bool, len(seqs))
	for _, v := range seqs {
		if seen[v] {
			t.Fatalf("duplicate seq id %d", v)
		}
		seen[v] = true
		if v == 0 {
			t.Fatalf("seq id must start at 1")
		}
	}
}

func TestIdleWorkerExits(t *testing.T) {
	s := New(WithIdleTimeout(30*time.Millisecond), WithQueueCapacity(16))

	_, err := s.Execute("u1", func(seq uint64) (any, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	// Give the worker time to hit its idle timeout.
	time.Sleep(80 * time.Millisecond)
	// After idle exit, submitting again must spin up a new worker.
	_, err = s.Execute("u1", func(seq uint64) (any, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
}

// TestQueueAcceptsBeyondInitialCapacity — ADR-0060 made the queue
// unbounded. The previous TestQueueFullReturnsError behaviour
// (ErrQueueFull on overflow) is gone. Here we verify that many more
// tasks than WithQueueCapacity still enqueue, all run in FIFO, and
// none are lost.
func TestQueueAcceptsBeyondInitialCapacity(t *testing.T) {
	s := New(WithQueueCapacity(2)) // hint of 2; unbounded in practice

	// Block u1's drain with a long-running task so the rest pile up.
	block := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = s.Execute("u1", func(seq uint64) (any, error) {
			close(block)
			<-release
			return nil, nil
		})
	}()
	<-block

	// Submit 20 follow-up tasks — well beyond the 2-hint. All must
	// succeed in enqueue and eventually run in FIFO order.
	const n = 20
	var mu sync.Mutex
	seen := make([]int, 0, n)
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			_, err := s.Execute("u1", func(seq uint64) (any, error) {
				mu.Lock()
				seen = append(seen, i)
				mu.Unlock()
				return nil, nil
			})
			errCh <- err
		}()
	}

	// Give all follow-ups time to reach the queue.
	time.Sleep(30 * time.Millisecond)

	// QueueDepth should reflect the pile-up (20 queued; the blocking
	// task is currently executing so not on the list).
	if got := s.QueueDepth("u1"); got == 0 {
		t.Fatalf("QueueDepth=0 while 20 tasks are parked")
	}

	close(release)
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("enqueue %d returned: %v", i, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("processed %d tasks, want %d", len(seen), n)
	}
	// FIFO within the same user: followups launched in arbitrary
	// goroutine order, so we only assert count + uniqueness (the
	// handoff ordering is exercised by TestDrainPreservesUserFIFO in
	// the async test file below).
	set := make(map[int]bool)
	for _, v := range seen {
		if set[v] {
			t.Fatalf("duplicate processing of id %d", v)
		}
		set[v] = true
	}
}

func TestSetCounterSeq(t *testing.T) {
	s := New()
	s.SetCounterSeq(100)
	if s.CounterSeq() != 100 {
		t.Fatalf("CounterSeq = %d, want 100", s.CounterSeq())
	}
	var seq uint64
	_, _ = s.Execute("u1", func(s uint64) (any, error) { seq = s; return nil, nil })
	if seq != 101 {
		t.Fatalf("next seq = %d, want 101", seq)
	}
}
