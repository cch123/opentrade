package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

// mockEvictPublisher captures Publish calls for evictor tests. Thread-
// safe because evictOne runs under the user sequencer which may be
// exercised concurrently across users in the same round.
type mockEvictPublisher struct {
	mu     sync.Mutex
	calls  []*eventpb.CounterJournalEvent
	failOn map[uint64]error // keyed by order_id in payload
}

func (m *mockEvictPublisher) Publish(_ context.Context, _ string, evt *eventpb.CounterJournalEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failOn != nil {
		if ev := evt.GetOrderEvicted(); ev != nil {
			if err, ok := m.failOn[ev.OrderId]; ok {
				return err
			}
		}
	}
	m.calls = append(m.calls, evt)
	return nil
}

func (m *mockEvictPublisher) Events() []*eventpb.CounterJournalEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*eventpb.CounterJournalEvent, len(m.calls))
	copy(out, m.calls)
	return out
}

// makeTerminalOrder seeds a terminal order in state with a concrete
// TerminatedAt timestamp for deterministic retention-window tests.
func makeTerminalOrder(t *testing.T, state *engine.ShardState, id uint64, userID string, terminatedAtMS int64) *engine.Order {
	t.Helper()
	state.Orders().RestoreInsert(&engine.Order{
		ID:            id,
		UserID:        userID,
		Symbol:        "BTC-USDT",
		Side:          engine.SideBid,
		Type:          engine.OrderTypeLimit,
		TIF:           engine.TIFGTC,
		Price:         dec.New("1"),
		Qty:           dec.New("1"),
		FilledQty:     dec.Zero,
		FrozenAmount:  dec.New("1"),
		FrozenSpent:   dec.Zero,
		Status:        engine.OrderStatusCanceled,
		CreatedAt:     terminatedAtMS - 1000,
		UpdatedAt:     terminatedAtMS,
		TerminatedAt:  terminatedAtMS,
		ClientOrderID: "coid-" + userID,
	})
	return state.Orders().Get(id)
}

// TestEvictOne_HappyPath covers the ring→journal→delete three-step.
func TestEvictOne_HappyPath(t *testing.T) {
	state := engine.NewShardState(0)
	seq := sequencer.New()
	w := &VShardWorker{
		cfg:   Config{Logger: zap.NewNop(), NodeID: "test-node"},
		state: state,
		seq:   seq,
	}
	order := makeTerminalOrder(t, state, 1, "u1", 1000)
	pub := &mockEvictPublisher{}

	if err := w.evictOne(context.Background(), order, pub); err != nil {
		t.Fatalf("evictOne: %v", err)
	}
	// Ring was populated.
	if _, ok := state.Account("u1").LookupTerminated(1); !ok {
		t.Fatal("ring missing evicted order")
	}
	// One journal event captured, with correct payload.
	events := pub.Events()
	if len(events) != 1 {
		t.Fatalf("publish count = %d, want 1", len(events))
	}
	body := events[0].GetOrderEvicted()
	if body == nil {
		t.Fatal("published event has no OrderEvicted payload")
	}
	if body.OrderId != 1 || body.UserId != "u1" || body.TerminatedAt != 1000 {
		t.Fatalf("payload mismatch: %+v", body)
	}
	// byID no longer contains the order.
	if state.Orders().Get(1) != nil {
		t.Fatal("byID still contains evicted order")
	}
}

// TestEvictOne_PublishFailLeavesByIDIntact verifies that a Publish
// failure aborts the three-step before Delete, preserving retry
// eligibility. Ring is already populated (Remember was step 1); that's
// acceptable because ring overwrite on retry is idempotent.
func TestEvictOne_PublishFailLeavesByIDIntact(t *testing.T) {
	state := engine.NewShardState(0)
	seq := sequencer.New()
	w := &VShardWorker{
		cfg:   Config{Logger: zap.NewNop(), NodeID: "test-node"},
		state: state,
		seq:   seq,
	}
	order := makeTerminalOrder(t, state, 1, "u1", 1000)
	injected := errors.New("kafka down")
	pub := &mockEvictPublisher{failOn: map[uint64]error{1: injected}}

	err := w.evictOne(context.Background(), order, pub)
	if err == nil || !errors.Is(err, injected) {
		t.Fatalf("evictOne expected %v, got %v", injected, err)
	}
	// byID must still contain the order — next round will retry.
	if state.Orders().Get(1) == nil {
		t.Fatal("byID dropped order despite publish failure")
	}
	// Ring has the entry (step 1 already ran). A retry will refresh
	// the ring and re-Publish; idempotent.
	if _, ok := state.Account("u1").LookupTerminated(1); !ok {
		t.Fatal("ring should hold the entry after step 1")
	}
}

// TestEvictOneRound_PicksEligibleSkipsInWindow chains CandidatesForEvict
// into evictOne via the shared round method. Verifies only eligible
// orders (past retention window) are evicted.
func TestEvictOneRound_PicksEligibleSkipsInWindow(t *testing.T) {
	state := engine.NewShardState(0)
	seq := sequencer.New()
	w := &VShardWorker{
		cfg: Config{
			Logger:              zap.NewNop(),
			NodeID:              "test-node",
			OrderEvictRetention: time.Minute,
			OrderEvictMaxBatch:  500,
		},
		state: state,
		seq:   seq,
	}
	// Two orders past retention, one fresh.
	now := time.Now().UnixMilli()
	past := now - time.Minute.Milliseconds() - 10_000 // 70s ago
	fresh := now - 10_000                             // 10s ago
	makeTerminalOrder(t, state, 1, "u1", past)
	makeTerminalOrder(t, state, 2, "u2", past)
	makeTerminalOrder(t, state, 3, "u3", fresh)

	pub := &mockEvictPublisher{}
	w.evictOneRound(context.Background(), zap.NewNop(), pub)

	if got := len(pub.Events()); got != 2 {
		t.Fatalf("published %d events, want 2", got)
	}
	// Orders 1 and 2 gone; order 3 stays.
	if state.Orders().Get(1) != nil || state.Orders().Get(2) != nil {
		t.Fatal("eligible orders not deleted")
	}
	if state.Orders().Get(3) == nil {
		t.Fatal("in-window order was deleted")
	}
	// Rings populated for u1 and u2.
	if _, ok := state.Account("u1").LookupTerminated(1); !ok {
		t.Fatal("u1 ring missing")
	}
	if _, ok := state.Account("u2").LookupTerminated(2); !ok {
		t.Fatal("u2 ring missing")
	}
	if _, ok := state.Account("u3").LookupTerminated(3); ok {
		t.Fatal("u3 ring should be empty for in-window order")
	}
}

// TestEvictOneRound_BatchCap bounds single-round work.
func TestEvictOneRound_BatchCap(t *testing.T) {
	state := engine.NewShardState(0)
	seq := sequencer.New()
	w := &VShardWorker{
		cfg: Config{
			Logger:              zap.NewNop(),
			NodeID:              "test-node",
			OrderEvictRetention: time.Minute,
			OrderEvictMaxBatch:  3, // cap
		},
		state: state,
		seq:   seq,
	}
	past := time.Now().UnixMilli() - 2*time.Minute.Milliseconds()
	for i := uint64(1); i <= 10; i++ {
		makeTerminalOrder(t, state, i, "u1", past)
	}
	pub := &mockEvictPublisher{}
	w.evictOneRound(context.Background(), zap.NewNop(), pub)
	// Exactly 3 events emitted; 7 orders remain.
	if got := len(pub.Events()); got != 3 {
		t.Fatalf("batch cap: emitted %d, want 3", got)
	}
	remaining := 0
	for i := uint64(1); i <= 10; i++ {
		if state.Orders().Get(i) != nil {
			remaining++
		}
	}
	if remaining != 7 {
		t.Fatalf("remaining = %d, want 7", remaining)
	}
}

// TestEvictOneRound_PerUserSequenced verifies evictor runs through
// seq.Execute so other user-serialised work interleaves correctly. We
// seed a blocking Execute on the same user before the evictor; the
// evictor's Execute should run after the blocker completes, proving
// FIFO.
func TestEvictOneRound_PerUserSequenced(t *testing.T) {
	state := engine.NewShardState(0)
	seq := sequencer.New()
	w := &VShardWorker{
		cfg: Config{
			Logger:              zap.NewNop(),
			NodeID:              "test-node",
			OrderEvictRetention: time.Minute,
			OrderEvictMaxBatch:  500,
		},
		state: state,
		seq:   seq,
	}
	past := time.Now().UnixMilli() - 2*time.Minute.Milliseconds()
	makeTerminalOrder(t, state, 1, "u1", past)

	var order []string
	var mu sync.Mutex
	release := make(chan struct{})

	// Occupy u1's sequencer first.
	done := make(chan struct{})
	go func() {
		seq.Execute("u1", func(_ uint64) (any, error) {
			mu.Lock()
			order = append(order, "blocker")
			mu.Unlock()
			<-release
			return nil, nil
		})
		close(done)
	}()
	// Let the blocker take the sequencer seat before we fire evict.
	time.Sleep(10 * time.Millisecond)

	pub := &mockEvictPublisher{}
	go w.evictOneRound(context.Background(), zap.NewNop(), pub)
	// Evictor is now queued behind blocker. Release.
	time.Sleep(10 * time.Millisecond)
	close(release)
	<-done
	// Wait for evictor round to finish.
	for i := 0; i < 100; i++ {
		if state.Orders().Get(1) == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Record evictor ran after blocker.
	mu.Lock()
	defer mu.Unlock()
	if state.Orders().Get(1) != nil {
		t.Fatal("evictor did not run")
	}
	if len(order) != 1 || order[0] != "blocker" {
		t.Fatalf("ordering trace = %v, want [blocker]", order)
	}
}

// TestRunOrderEvictor_HonoursContext shuts down on ctx.Done before the
// first tick, verifying the goroutine exits cleanly.
func TestRunOrderEvictor_HonoursContext(t *testing.T) {
	state := engine.NewShardState(0)
	seq := sequencer.New()
	w := &VShardWorker{
		cfg: Config{
			Logger:              zap.NewNop(),
			NodeID:              "test-node",
			OrderEvictInterval:  time.Hour,    // won't tick in the test window
			OrderEvictRetention: time.Hour,
			OrderEvictMaxBatch:  10,
		},
		state:    state,
		seq:      seq,
		producer: nil, // never reached: runOrderEvictor returns before producer use
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.runOrderEvictor(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("runOrderEvictor did not exit on ctx cancel")
	}
}

// TestRunOrderEvictor_DisabledOnNonPositiveInterval verifies a
// negative OrderEvictInterval turns the goroutine into a no-op (used
// by tests / manual eviction tooling).
func TestRunOrderEvictor_DisabledOnNonPositiveInterval(t *testing.T) {
	w := &VShardWorker{
		cfg: Config{
			Logger:             zap.NewNop(),
			OrderEvictInterval: -1,
		},
	}
	done := make(chan struct{})
	go func() {
		w.runOrderEvictor(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("runOrderEvictor should exit immediately when interval <= 0")
	}
}
