package worker

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestPendingList_EnqueueThenMarkDone(t *testing.T) {
	p := newPendingList()
	infl := p.Enqueue(5, 100, 2)
	if ready := p.MarkFnDone(infl); ready {
		t.Fatal("first of two fn-dones should not be ready")
	}
	if ready := p.MarkFnDone(infl); !ready {
		t.Fatal("second fn-done should flip ready")
	}
	// Head should now be poppable.
	part, off, ok := p.PopConsecutiveDone()
	if !ok {
		t.Fatal("PopConsecutiveDone returned ok=false after ready")
	}
	if part != 5 || off != 100 {
		t.Fatalf("popped (%d,%d), want (5,100)", part, off)
	}
}

func TestPendingList_EnqueueZeroIsImmediatelyDone(t *testing.T) {
	p := newPendingList()
	p.Enqueue(0, 42, 0)
	_, off, ok := p.PopConsecutiveDone()
	if !ok || off != 42 {
		t.Fatalf("zero-fn TE should be immediately poppable; got (ok=%v, off=%d)", ok, off)
	}
}

func TestPendingList_HeadNotDoneBlocksTail(t *testing.T) {
	p := newPendingList()
	a := p.Enqueue(0, 1, 2)
	b := p.Enqueue(0, 2, 1)
	// Complete b first — head (a) still pending, no advance yet.
	p.MarkFnDone(b)
	if _, _, ok := p.PopConsecutiveDone(); ok {
		t.Fatal("head (offset=1) still pending — pop should refuse")
	}
	// Complete a partially then fully.
	p.MarkFnDone(a)
	if _, _, ok := p.PopConsecutiveDone(); ok {
		t.Fatal("head still has 1 pending")
	}
	p.MarkFnDone(a)
	// Now both ready in order; pop returns the max contiguous offset.
	_, off, ok := p.PopConsecutiveDone()
	if !ok {
		t.Fatal("pop should succeed after head completes")
	}
	if off != 2 {
		t.Fatalf("pop offset = %d, want 2 (should also have taken b)", off)
	}
	if got := p.Len(); got != 0 {
		t.Fatalf("list should be empty, Len=%d", got)
	}
}

func TestPendingList_MultipleCompletionsBetweenPops(t *testing.T) {
	p := newPendingList()
	// Enqueue 5 TEs with 1 fn each. Complete out of order.
	inflights := make([]*inFlightTE, 5)
	for i := 0; i < 5; i++ {
		inflights[i] = p.Enqueue(0, int64(i+1), 1)
	}
	// Complete offsets 2, 3 first — head still at 1 pending.
	p.MarkFnDone(inflights[1])
	p.MarkFnDone(inflights[2])
	if _, _, ok := p.PopConsecutiveDone(); ok {
		t.Fatal("head=1 still pending, pop should not advance")
	}
	// Complete head (1) — now 1, 2, 3 contiguous done; 4, 5 still pending.
	p.MarkFnDone(inflights[0])
	_, off, ok := p.PopConsecutiveDone()
	if !ok || off != 3 {
		t.Fatalf("pop should reach offset 3; got (ok=%v, off=%d)", ok, off)
	}
	if got := p.Len(); got != 2 {
		t.Fatalf("should have 2 left (offsets 4,5), got Len=%d", got)
	}
	// Complete 5 out of order; pop still blocks on 4.
	p.MarkFnDone(inflights[4])
	if _, _, ok := p.PopConsecutiveDone(); ok {
		t.Fatal("head=4 still pending")
	}
	// Complete 4; pop drains both.
	p.MarkFnDone(inflights[3])
	_, off, ok = p.PopConsecutiveDone()
	if !ok || off != 5 {
		t.Fatalf("final pop offset = %d, want 5", off)
	}
	if got := p.Len(); got != 0 {
		t.Fatalf("list should be empty after full drain, Len=%d", got)
	}
}

func TestPendingList_EmptyPopReturnsFalse(t *testing.T) {
	p := newPendingList()
	_, _, ok := p.PopConsecutiveDone()
	if ok {
		t.Fatal("empty list should return ok=false")
	}
}

func TestPendingList_HeadOffset(t *testing.T) {
	p := newPendingList()
	if _, ok := p.HeadOffset(); ok {
		t.Fatal("empty list HeadOffset should be false")
	}
	p.Enqueue(0, 100, 1)
	p.Enqueue(0, 200, 1)
	if off, ok := p.HeadOffset(); !ok || off != 100 {
		t.Fatalf("HeadOffset = (%d, %v), want (100, true)", off, ok)
	}
}

func TestPendingList_NegativeCountPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on negative expectedFnCount")
		}
	}()
	p := newPendingList()
	p.Enqueue(0, 1, -1)
}

func TestPendingList_OverCompletionPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on over-completion")
		}
	}()
	p := newPendingList()
	infl := p.Enqueue(0, 1, 1)
	p.MarkFnDone(infl)
	p.MarkFnDone(infl) // second call pushes pending to -1
}

// TestPendingList_ConcurrentMarkDone stress-tests concurrent fn-done
// callbacks against a single TE with many expected completions.
func TestPendingList_ConcurrentMarkDone(t *testing.T) {
	p := newPendingList()
	const n = 1000
	infl := p.Enqueue(0, 1, n)
	var readyCount atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p.MarkFnDone(infl) {
				readyCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := readyCount.Load(); got != 1 {
		t.Fatalf("exactly 1 MarkFnDone should return ready; got %d", got)
	}
	_, off, ok := p.PopConsecutiveDone()
	if !ok || off != 1 {
		t.Fatalf("pop = (ok=%v, off=%d), want (true, 1)", ok, off)
	}
}

// TestPendingList_ConcurrentEnqueueAndComplete simulates the consumer
// loop's enqueue pattern + per-user drain goroutines completing fns.
// Each TE has 1 fn, enqueued in strict offset order; completions come
// from arbitrary goroutines.
func TestPendingList_ConcurrentEnqueueAndComplete(t *testing.T) {
	p := newPendingList()
	const n = 500
	done := make(chan struct{}, n)

	// Enqueue + kick off completion workers.
	for i := int64(1); i <= n; i++ {
		infl := p.Enqueue(0, i, 1)
		go func(i *inFlightTE) {
			p.MarkFnDone(i)
			done <- struct{}{}
		}(infl)
	}
	for i := 0; i < n; i++ {
		<-done
	}

	// Advancer-style pop drains everything.
	_, maxOff, ok := p.PopConsecutiveDone()
	if !ok {
		t.Fatal("pop should succeed after all fns complete")
	}
	if maxOff != n {
		t.Fatalf("max popped = %d, want %d", maxOff, n)
	}
	if got := p.Len(); got != 0 {
		t.Fatalf("list should be empty, Len=%d", got)
	}
}
