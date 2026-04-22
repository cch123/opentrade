// Package sequencer implements Counter's per-user FIFO serializer.
//
// Design: see ADR-0018 for the original per-user FIFO contract and
// ADR-0060 §1 for the fire-and-forget extension (SubmitAsync).
//
// Core invariant: all tasks submitted for the same userID run in
// strict FIFO order on a single drain goroutine. Different users run
// concurrently. The drain goroutine exits after idleTimeout without
// new tasks and is re-spawned lazily on the next submit.
//
// The per-user queue was originally a bounded chan, which returned
// ErrQueueFull under pressure. ADR-0060 replaced it with an unbounded
// list so the consumer loop — which is now fire-and-forget — cannot
// be blocked by a slow user's backlog. Memory safety is upheld
// upstream by ADR-0060's Publish 5s-panic-on-failure and the
// pendingList bloat monitoring alerts.
//
// ErrQueueFull is kept as an exported sentinel for backward
// compatibility but is no longer returned anywhere in production
// code — SubmitAsync and Execute are infallible at the enqueue
// layer. Callers that still check for it will compile and simply
// never see the error.
package sequencer

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults tuned for Counter shard sizes in ADR-0018 (2w TPS / shard).
const (
	defaultIdleTimeout   = 30 * time.Second
	defaultQueueCapacity = 256 // initial list hint only; queue is unbounded
)

// ErrQueueFull is retained for API compatibility. ADR-0060 made the
// per-user queue unbounded, so SubmitAsync / Execute no longer return
// this error. Kept exported so existing callers' error-branch code
// still compiles.
var ErrQueueFull = errors.New("sequencer: user queue full")

// UserSequencer serializes per-user work items while allowing different users
// to run in parallel. The counter-shard-scoped monotonic seq (ADR-0018) is
// assigned to every submitted task just before the callback executes.
type UserSequencer struct {
	counterSeq atomic.Uint64
	users      sync.Map // user_id → *userQueue

	idleTimeout   time.Duration
	queueCapacity int // initial list allocation hint; queue is unbounded
}

// Option configures a UserSequencer.
type Option func(*UserSequencer)

// WithIdleTimeout sets how long a worker waits for new tasks before exiting.
func WithIdleTimeout(d time.Duration) Option {
	return func(s *UserSequencer) { s.idleTimeout = d }
}

// WithQueueCapacity sets the initial allocation hint for the per-user
// queue. Since ADR-0060 the queue is unbounded — this value only
// affects how much space the list pre-reserves; it is no longer a
// back-pressure threshold.
func WithQueueCapacity(n int) Option {
	return func(s *UserSequencer) { s.queueCapacity = n }
}

// New constructs a UserSequencer.
func New(opts ...Option) *UserSequencer {
	s := &UserSequencer{
		idleTimeout:   defaultIdleTimeout,
		queueCapacity: defaultQueueCapacity,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// CounterSeq returns the current counter-shard-scoped monotonic seq.
func (s *UserSequencer) CounterSeq() uint64 { return s.counterSeq.Load() }

// SetCounterSeq sets the starting seq value. Call before submitting any
// tasks (typically used to restore from a snapshot).
func (s *UserSequencer) SetCounterSeq(seq uint64) { s.counterSeq.Store(seq) }

// Execute enqueues fn on userID's FIFO queue and BLOCKS until it runs.
// The callback receives a freshly allocated counter-shard-scoped
// monotonic seq. Used by RPC paths (PlaceOrder / Transfer /
// CancelOrder) where the gRPC handler must return the business result
// synchronously.
//
// ADR-0060: the previous bounded-queue ErrQueueFull path is gone; the
// queue is unbounded so Execute is infallible at the enqueue layer.
// The returned error is exclusively whatever fn returns.
//
// fn MUST NOT block indefinitely: it holds the user's serializer
// until it returns. All I/O inside fn (Kafka produce, etc.) should
// have a finite timeout.
func (s *UserSequencer) Execute(userID string, fn func(counterSeq uint64) (any, error)) (any, error) {
	resp := make(chan taskResult, 1)
	s.submit(userID, &task{
		run: func(seq uint64) {
			v, err := fn(seq)
			resp <- taskResult{v: v, err: err}
		},
	})
	r := <-resp
	return r.v, r.err
}

// SubmitAsync enqueues fn on userID's FIFO queue and returns
// immediately. cb (optional, may be nil) is invoked from the drain
// goroutine after fn completes, carrying whatever error fn returned.
// Used by the ADR-0060 fire-and-forget Counter consumer loop: the
// trade-event poller submits handlers here so the next poll can fire
// without waiting for per-user execution.
//
// Both fn and cb run on the user's drain goroutine; cb MUST be short
// and non-blocking (typically a pendingList decrement + channel
// send). If cb panics, the panic propagates on the drain goroutine
// and will tear down the process — treat cb as inviolate.
//
// ADR-0060: queue is unbounded, SubmitAsync is infallible at the
// enqueue layer.
func (s *UserSequencer) SubmitAsync(
	userID string,
	fn func(counterSeq uint64) error,
	cb func(err error),
) {
	s.submit(userID, &task{
		run: func(seq uint64) {
			err := fn(seq)
			if cb != nil {
				cb(err)
			}
		},
	})
}

// ActiveUsers returns the number of users currently tracked (including those
// whose worker is idling but not yet evicted). Useful for metrics / tests.
func (s *UserSequencer) ActiveUsers() int {
	n := 0
	s.users.Range(func(_, _ any) bool { n++; return true })
	return n
}

// QueueDepth returns the current pending-task count for userID. Used
// by monitoring / back-pressure signals (ADR-0063 detector may read
// this to compute per-user queue depth). Zero means no queue entry
// or empty queue. Safe to call concurrently with enqueue / drain.
func (s *UserSequencer) QueueDepth(userID string) int {
	v, ok := s.users.Load(userID)
	if !ok {
		return 0
	}
	uq := v.(*userQueue)
	uq.mu.Lock()
	defer uq.mu.Unlock()
	return uq.tasks.Len()
}

// ---------------------------------------------------------------------------
// internals
// ---------------------------------------------------------------------------

const (
	stateIdle    int32 = 0
	stateRunning int32 = 1
)

type userQueue struct {
	mu     sync.Mutex
	tasks  *list.List    // *task, FIFO
	notify chan struct{} // cap 1, signal that work was enqueued
	state  atomic.Int32  // see handoff protocol below
}

type task struct {
	run func(counterSeq uint64)
}

type taskResult struct {
	v   any
	err error
}

func (s *UserSequencer) getOrCreate(userID string) *userQueue {
	if v, ok := s.users.Load(userID); ok {
		return v.(*userQueue)
	}
	uq := &userQueue{
		tasks:  list.New(),
		notify: make(chan struct{}, 1),
	}
	actual, _ := s.users.LoadOrStore(userID, uq)
	return actual.(*userQueue)
}

// submit is the enqueue primitive shared by Execute and SubmitAsync.
// Always succeeds (unbounded queue).
//
// Ordering matters: we push to the list first (so any newly spawned
// drain cannot miss the task), then CAS the state to Running + spawn
// drain if needed, then ping notify. Any producer that CAS-loses
// relies on the concurrent drain picking up the task on its next
// list.Front() poll — which is safe because our push happened-before
// their poll under uq.mu.
func (s *UserSequencer) submit(userID string, t *task) {
	uq := s.getOrCreate(userID)
	uq.mu.Lock()
	uq.tasks.PushBack(t)
	uq.mu.Unlock()
	if uq.state.CompareAndSwap(stateIdle, stateRunning) {
		go uq.drain(s)
	}
	// Best-effort signal; if the drain is busy or another notify is
	// already pending, drop ours — either way the task is guaranteed
	// visible on the list.
	select {
	case uq.notify <- struct{}{}:
	default:
	}
}

// popFront removes and returns the oldest task, or nil if empty. Must
// run outside any other lock to avoid user-visible deadlock if the
// task's fn re-enters the sequencer.
func (uq *userQueue) popFront() *task {
	uq.mu.Lock()
	defer uq.mu.Unlock()
	e := uq.tasks.Front()
	if e == nil {
		return nil
	}
	uq.tasks.Remove(e)
	return e.Value.(*task)
}

// drain is the per-user worker goroutine. It exits after idleTimeout
// without any new tasks — the *userQueue entry stays in s.users (no
// churn on re-entry); only the goroutine is reclaimed.
//
// Handoff protocol (producer submit + drain idle-exit) guarantees
// that a task never sits on an unattended queue:
//
//   - submit pushes under uq.mu, then CAS(Idle→Running) and spawns a
//     replacement drain iff the old drain has already published Idle
//     and is about to exit.
//   - drain, before exiting, publishes stateIdle and then re-checks
//     the list under uq.mu. If it sees a pending task, it re-wins
//     the seat via CAS(Idle→Running) (Path B). If the producer's CAS
//     already won (Path C), drain exits and the spawned replacement
//     picks up the task.
//
// Unlike the chan-based original, the unbounded list means no
// ErrQueueFull and no "queue full" timing hazards; the CAS sequence
// is simpler: submit CAS-wins ⇒ spawn; drain Store(Idle) then
// list-check ⇒ either re-win seat or exit.
func (uq *userQueue) drain(s *UserSequencer) {
	idle := time.NewTimer(s.idleTimeout)
	defer idle.Stop()
	for {
		// Drain everything currently enqueued before waiting.
		for t := uq.popFront(); t != nil; t = uq.popFront() {
			seq := s.counterSeq.Add(1)
			t.run(seq)
		}
		// Reset idle timer (we just ran or we're at start).
		if !idle.Stop() {
			select {
			case <-idle.C:
			default:
			}
		}
		idle.Reset(s.idleTimeout)
		// Drain consumer signal or idle out.
		select {
		case <-uq.notify:
			// Fall through to the top of the loop — re-drain list.
		case <-idle.C:
			// Publish "I am about to exit" BEFORE re-inspecting the
			// list. This store is the handoff-protocol publication
			// point: any producer whose submit happens after this
			// store must see stateIdle on its CAS and spawn a
			// replacement.
			uq.state.Store(stateIdle)

			uq.mu.Lock()
			empty := uq.tasks.Len() == 0
			uq.mu.Unlock()
			if empty {
				// Path A: queue empty at this load. Any later
				// submit sees stateIdle on its CAS and spawns a
				// replacement. Safe to exit.
				return
			}
			// Path B: a submit raced in between our Store(Idle) and
			// the list-len check. Try to reclaim the seat.
			if uq.state.CompareAndSwap(stateIdle, stateRunning) {
				continue
			}
			// Path C: CAS failed ⇒ a producer's CAS flipped state
			// to Running and has already issued `go drain`. Safe
			// to exit; the replacement will drain the leftover.
			return
		}
	}
}
