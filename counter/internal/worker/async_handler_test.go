package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// stubSvc implements asyncTradeService for tests. It forwards the
// onCount hook with a configurable count, then calls cb `count` times
// (optionally with injected errors), simulating what
// Service.HandleTradeRecordAsync does without exercising the entire
// per-user sequencer + Kafka producer stack.
type stubSvc struct {
	mu       sync.Mutex
	invoked  int
	plan     []planStep
	fnErr    error
	fnDelay  time.Duration
}

type planStep struct {
	count int32
}

func (s *stubSvc) HandleTradeRecordAsync(
	_ context.Context,
	_ *eventpb.TradeEvent,
	onCount func(count int32),
	cb func(err error),
) {
	s.mu.Lock()
	idx := s.invoked
	s.invoked++
	var step planStep
	if idx < len(s.plan) {
		step = s.plan[idx]
	}
	delay := s.fnDelay
	fnErr := s.fnErr
	s.mu.Unlock()

	onCount(step.count)
	if step.count == 0 {
		return
	}
	// Deliver cb from a goroutine to mirror real drain-goroutine
	// behaviour (cb runs outside the caller's stack).
	for i := int32(0); i < step.count; i++ {
		go func() {
			if delay > 0 {
				time.Sleep(delay)
			}
			cb(fnErr)
		}()
	}
}

func TestAsyncHandler_ZeroCountSignalsAdvancer(t *testing.T) {
	pending := newPendingList()
	signal := make(chan struct{}, 4)
	svc := &stubSvc{plan: []planStep{{count: 0}}}
	h := newAsyncTradeHandler(svc, pending, signal, zap.NewNop())

	err := h.HandleTradeRecord(context.Background(), &eventpb.TradeEvent{}, 5, 42)
	if err != nil {
		t.Fatalf("HandleTradeRecord: %v", err)
	}
	// Signal should fire immediately for count=0.
	select {
	case <-signal:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected advance signal for count=0")
	}
	// pendingList contains the entry; PopConsecutiveDone returns it.
	part, off, ok := pending.PopConsecutiveDone()
	if !ok || part != 5 || off != 42 {
		t.Fatalf("pop = (part=%d, off=%d, ok=%v); want (5, 42, true)", part, off, ok)
	}
}

func TestAsyncHandler_OneCountSignalsOnFnDone(t *testing.T) {
	pending := newPendingList()
	signal := make(chan struct{}, 4)
	svc := &stubSvc{plan: []planStep{{count: 1}}}
	h := newAsyncTradeHandler(svc, pending, signal, zap.NewNop())

	err := h.HandleTradeRecord(context.Background(), &eventpb.TradeEvent{}, 0, 100)
	if err != nil {
		t.Fatalf("HandleTradeRecord: %v", err)
	}
	select {
	case <-signal:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected advance signal after fn completes")
	}
	_, off, ok := pending.PopConsecutiveDone()
	if !ok || off != 100 {
		t.Fatalf("pop = (off=%d, ok=%v); want (100, true)", off, ok)
	}
}

func TestAsyncHandler_TwoCountRequiresBothFns(t *testing.T) {
	pending := newPendingList()
	signal := make(chan struct{}, 8)
	svc := &stubSvc{plan: []planStep{{count: 2}}, fnDelay: 5 * time.Millisecond}
	h := newAsyncTradeHandler(svc, pending, signal, zap.NewNop())

	err := h.HandleTradeRecord(context.Background(), &eventpb.TradeEvent{}, 0, 1)
	if err != nil {
		t.Fatalf("HandleTradeRecord: %v", err)
	}
	// Wait for at least one signal (both fns complete).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, ok := pending.PopConsecutiveDone(); ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("pendingList never became advance-ready")
}

func TestAsyncHandler_SerialConsumerLoop(t *testing.T) {
	// Simulate the real consumer-loop usage: many records in ascending
	// order, each with count=1. Some finish out of order; the advancer
	// (simulated by PopConsecutiveDone loop) must still advance
	// monotonically.
	pending := newPendingList()
	signal := make(chan struct{}, 64)
	svc := &stubSvc{fnDelay: time.Millisecond}
	const n = 20
	svc.plan = make([]planStep, n)
	for i := range svc.plan {
		svc.plan[i].count = 1
	}
	h := newAsyncTradeHandler(svc, pending, signal, zap.NewNop())

	var lastAdvanced int64
	advancerDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-signal:
				for {
					_, maxOff, ok := pending.PopConsecutiveDone()
					if !ok {
						break
					}
					atomic.StoreInt64(&lastAdvanced, maxOff)
				}
				if atomic.LoadInt64(&lastAdvanced) == int64(n-1) {
					close(advancerDone)
					return
				}
			case <-time.After(3 * time.Second):
				close(advancerDone)
				return
			}
		}
	}()

	for i := int64(0); i < n; i++ {
		if err := h.HandleTradeRecord(context.Background(), &eventpb.TradeEvent{}, 0, i); err != nil {
			t.Fatalf("HandleTradeRecord %d: %v", i, err)
		}
	}

	<-advancerDone
	if got := atomic.LoadInt64(&lastAdvanced); got != int64(n-1) {
		t.Fatalf("lastAdvanced = %d, want %d", got, n-1)
	}
}

func TestAsyncHandler_FnErrorDoesNotBlockAdvance(t *testing.T) {
	pending := newPendingList()
	signal := make(chan struct{}, 4)
	// Service reports an error from the drain fn; handler logs and still
	// decrements pending — this keeps the watermark moving even under
	// non-retryable fn errors.
	svc := &stubSvc{
		plan:  []planStep{{count: 1}},
		fnErr: errors.New("simulated non-retryable"),
	}
	h := newAsyncTradeHandler(svc, pending, signal, zap.NewNop())
	if err := h.HandleTradeRecord(context.Background(), &eventpb.TradeEvent{}, 0, 7); err != nil {
		t.Fatalf("HandleTradeRecord: %v", err)
	}
	select {
	case <-signal:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected advance signal even after fn error")
	}
	_, off, ok := pending.PopConsecutiveDone()
	if !ok || off != 7 {
		t.Fatalf("pop = (off=%d, ok=%v); want (7, true)", off, ok)
	}
}
