package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/clustering"
	"github.com/xargin/opentrade/pkg/counterstate"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/service"
)

// mockCheckpointPub captures TECheckpointEvent publishes.
type mockCheckpointPub struct {
	mu       sync.Mutex
	events   []*eventpb.TECheckpointEvent
	failOn   int   // fail the N-th call (1-based); 0 = never
	callNum  int
	injected error
}

func (m *mockCheckpointPub) PublishToVShard(_ context.Context, _ int32, _ string, evt *eventpb.CounterJournalEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callNum++
	if m.failOn > 0 && m.callNum == m.failOn {
		return m.injected
	}
	if cp := evt.GetTeCheckpoint(); cp != nil {
		m.events = append(m.events, cp)
	}
	return nil
}

func (m *mockCheckpointPub) Events() []*eventpb.TECheckpointEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*eventpb.TECheckpointEvent, len(m.events))
	copy(out, m.events)
	return out
}

// newAdvancerTestWorker builds a minimal VShardWorker just enough to
// exercise runAdvancer. Doesn't open Kafka / storage.
func newAdvancerTestWorker(t *testing.T, vshardID clustering.VShardID) *VShardWorker {
	t.Helper()
	state := counterstate.NewShardState(int(vshardID))
	seq := sequencer.New()
	svc := service.New(
		service.Config{ShardID: int(vshardID), TotalShards: 256},
		state, seq, nil, nil, zap.NewNop(),
	)
	return &VShardWorker{
		cfg:   Config{Logger: zap.NewNop(), NodeID: "test", VShardID: vshardID},
		state: state,
		seq:   seq,
		svc:   svc,
	}
}

// runAdvancerInBackground kicks the advancer off and returns a cancel
// func + completion channel.
func runAdvancerInBackground(
	w *VShardWorker,
	pending *pendingList,
	signal <-chan struct{},
	pub checkpointPublisher,
) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.runAdvancerWithPublisher(ctx, pending, signal, pub)
	}()
	return cancel, done
}

// TestAdvancer_PublishesCheckpointAndAdvancesOffset exercises the
// happy-path: enqueue some TEs, mark them done, advancer emits a
// checkpoint and updates the offset map. PopConsecutiveDone coalesces
// adjacent-done entries into one (partition, maxOffset) result, so
// the number of emitted checkpoints is bounded above by the number
// of signals drained, not by the number of TEs — the final offset
// reached is what matters.
func TestAdvancer_PublishesCheckpointAndAdvancesOffset(t *testing.T) {
	w := newAdvancerTestWorker(t, clustering.VShardID(42))
	pending := newPendingList()
	signal := make(chan struct{}, 4)
	pub := &mockCheckpointPub{}
	cancel, done := runAdvancerInBackground(w, pending, signal, pub)
	defer func() {
		cancel()
		<-done
	}()

	// Enqueue three TEs on partition 42, sequentially done.
	for i := int64(100); i <= 102; i++ {
		infl := pending.Enqueue(42, i, 1)
		if pending.MarkFnDone(infl) {
			signal <- struct{}{}
		}
	}

	// Wait until the last enqueued TE's offset has been published.
	waitForLastOffset(t, pub, 42, 102, 2*time.Second)

	events := pub.Events()
	if len(events) == 0 {
		t.Fatal("no checkpoints emitted")
	}
	// The FINAL emitted checkpoint must carry the max offset so the
	// trade-dump shadow pipeline can land on the correct watermark.
	last := events[len(events)-1]
	if last.TeOffset != 102 || last.TePartition != 42 {
		t.Fatalf("last event = %+v, want (partition=42, offset=102)", last)
	}
	// Every emitted offset must be strictly monotonic.
	for i := 1; i < len(events); i++ {
		if events[i].TeOffset <= events[i-1].TeOffset {
			t.Fatalf("non-monotonic events: %v", events)
		}
	}
}

// TestAdvancer_CoalescesMultipleInflights verifies that a single signal
// can drain multiple contiguous-done entries without extra signals.
func TestAdvancer_CoalescesMultipleInflights(t *testing.T) {
	w := newAdvancerTestWorker(t, clustering.VShardID(7))
	pending := newPendingList()
	signal := make(chan struct{}, 1)
	pub := &mockCheckpointPub{}
	cancel, done := runAdvancerInBackground(w, pending, signal, pub)
	defer func() {
		cancel()
		<-done
	}()

	// Enqueue 5 TEs and mark them all done BEFORE sending any signal.
	// Then send one signal — advancer should drain all five.
	infls := make([]*inFlightTE, 5)
	for i := 0; i < 5; i++ {
		infls[i] = pending.Enqueue(7, int64(1000+i), 1)
	}
	for i := 0; i < 5; i++ {
		pending.MarkFnDone(infls[i])
	}
	signal <- struct{}{}

	// Single signal must coalesce all five into one checkpoint at
	// the last enqueued offset.
	waitForLastOffset(t, pub, 7, 1004, 2*time.Second)

	events := pub.Events()
	if len(events) != 1 {
		t.Fatalf("got %d checkpoints, want 1 (coalesced)", len(events))
	}
	if events[0].TeOffset != 1004 {
		t.Fatalf("coalesced event offset = %d, want 1004", events[0].TeOffset)
	}
}

// TestAdvancer_PublishFailSkipsCheckpoint verifies that a Publish
// error prevents the failed TE from being checkpointed — the next
// PopConsecutiveDone is already consumed (entry was removed), so
// retry happens when the next signal arrives with a new completed TE.
// Here we verify the single-failure case: the failed offset is not
// republished even after the next TE succeeds.
func TestAdvancer_PublishFailSkipsCheckpoint(t *testing.T) {
	w := newAdvancerTestWorker(t, clustering.VShardID(3))
	pending := newPendingList()
	signal := make(chan struct{}, 4)
	pub := &mockCheckpointPub{
		failOn:   1, // fail the first publish
		injected: errors.New("kafka temp down"),
	}
	cancel, done := runAdvancerInBackground(w, pending, signal, pub)
	defer func() {
		cancel()
		<-done
	}()

	infl1 := pending.Enqueue(3, 500, 1)
	pending.MarkFnDone(infl1)
	signal <- struct{}{}
	// Give advancer time to consume signal 1 and try (fail) publishing
	// offset 500 BEFORE offset 501 lands in the list, otherwise the
	// single signal-drain loop could coalesce both into one
	// PopConsecutiveDone call and race the fail-on-first-call counter.
	// The production signal cadence spaces these out naturally; the
	// test just makes that spacing deterministic.
	waitForCallCount(t, pub, 1, time.Second)

	infl2 := pending.Enqueue(3, 501, 1)
	pending.MarkFnDone(infl2)
	signal <- struct{}{}

	waitForCallCount(t, pub, 2, time.Second)
	// Only the second publish succeeded (first was injected-fail).
	// The failed offset 500 is not republished — operators notice via
	// the logged error + monitor.
	events := pub.Events()
	if len(events) != 1 {
		t.Fatalf("got %d successful events, want 1", len(events))
	}
	if events[0].TeOffset != 501 {
		t.Fatalf("successful event offset = %d, want 501", events[0].TeOffset)
	}
}

// waitForLastOffset polls the mock publisher's events until the most
// recent one carries (partition, offset), or fails the test on
// timeout. Avoids flake from advancer goroutine scheduling.
func waitForLastOffset(t *testing.T, pub *mockCheckpointPub, partition int32, offset int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events := pub.Events()
		if len(events) > 0 {
			last := events[len(events)-1]
			if last.TePartition == partition && last.TeOffset == offset {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	events := pub.Events()
	t.Fatalf("never observed (partition=%d, offset=%d) within %v; events=%+v", partition, offset, timeout, events)
}

// waitForCallCount polls the mock publisher's callNum until it reaches
// min, or fails the test on timeout. Avoids flake from advancer
// goroutine scheduling.
func waitForCallCount(t *testing.T, pub *mockCheckpointPub, min int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pub.mu.Lock()
		got := pub.callNum
		pub.mu.Unlock()
		if got >= min {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	pub.mu.Lock()
	got := pub.callNum
	pub.mu.Unlock()
	t.Fatalf("callCount = %d, want >= %d within %v", got, min, timeout)
}

// TestAdvancer_ShutdownOnContextCancel confirms runAdvancer exits when
// the parent context is cancelled.
func TestAdvancer_ShutdownOnContextCancel(t *testing.T) {
	w := newAdvancerTestWorker(t, clustering.VShardID(0))
	pending := newPendingList()
	signal := make(chan struct{}, 1)
	pub := &mockCheckpointPub{}
	cancel, done := runAdvancerInBackground(w, pending, signal, pub)
	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("runAdvancer did not exit on ctx cancel")
	}
}
