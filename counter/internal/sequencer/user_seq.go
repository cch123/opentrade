// Package sequencer implements Counter's per-user FIFO serializer.
//
// Design: see ADR-0018. Strict FIFO is mandatory (sync.Mutex does NOT
// guarantee FIFO under contention in Go). Each active user gets a channel
// plus a lazily-started worker goroutine that drains the channel in arrival
// order. Workers exit after a configurable idle timeout so inactive users
// do not retain goroutines.
package sequencer

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults tuned for Counter shard sizes in ADR-0018 (2w TPS / shard).
const (
	defaultIdleTimeout   = 30 * time.Second
	defaultQueueCapacity = 256
)

// ErrQueueFull is returned when a user's channel is full. Upstream should
// translate this to 429 / TooManyRequests to the caller.
var ErrQueueFull = errors.New("sequencer: user queue full")

// UserSequencer serializes per-user work items while allowing different users
// to run in parallel. The counter-shard-scoped monotonic seq (ADR-0018) is
// assigned to every submitted task just before the callback executes.
type UserSequencer struct {
	counterSeq atomic.Uint64
	users      sync.Map // user_id → *userQueue

	idleTimeout   time.Duration
	queueCapacity int
}

// Option configures a UserSequencer.
type Option func(*UserSequencer)

// WithIdleTimeout sets how long a worker waits for new tasks before exiting.
func WithIdleTimeout(d time.Duration) Option {
	return func(s *UserSequencer) { s.idleTimeout = d }
}

// WithQueueCapacity sets per-user channel buffer capacity.
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

// Execute enqueues fn on userID's FIFO queue and blocks until it runs.
// The callback receives a freshly allocated counter-shard-scoped monotonic
// seq. Returns ErrQueueFull if the user's channel is full.
//
// fn MUST NOT block indefinitely: it holds the user's serializer until it
// returns. All I/O inside fn (Kafka produce, etc.) should have a finite
// timeout.
//
// Ordering matters: we send on uq.tasks first, then CAS(Idle→Running).
// This sequence is required by the worker handoff protocol documented
// on drain — reversing it can orphan a task.
func (s *UserSequencer) Execute(userID string, fn func(counterSeq uint64) (any, error)) (any, error) {
	uq := s.getOrCreate(userID)
	t := &task{fn: fn, resp: make(chan taskResult, 1)}

	select {
	case uq.tasks <- t:
	default:
		return nil, ErrQueueFull
	}

	if uq.state.CompareAndSwap(stateIdle, stateRunning) {
		go uq.drain(s)
	}

	r := <-t.resp
	return r.v, r.err
}

// ActiveUsers returns the number of users currently tracked (including those
// whose worker is idling but not yet evicted). Useful for metrics / tests.
func (s *UserSequencer) ActiveUsers() int {
	n := 0
	s.users.Range(func(_, _ any) bool { n++; return true })
	return n
}

// ---------------------------------------------------------------------------
// internals
// ---------------------------------------------------------------------------

const (
	stateIdle    int32 = 0
	stateRunning int32 = 1
)

type userQueue struct {
	tasks chan *task
	state atomic.Int32
}

type task struct {
	fn   func(counterSeq uint64) (any, error)
	resp chan taskResult
}

type taskResult struct {
	v   any
	err error
}

func (s *UserSequencer) getOrCreate(userID string) *userQueue {
	if v, ok := s.users.Load(userID); ok {
		return v.(*userQueue)
	}
	uq := &userQueue{tasks: make(chan *task, s.queueCapacity)}
	actual, _ := s.users.LoadOrStore(userID, uq)
	return actual.(*userQueue)
}

// drain is the per-user worker goroutine. It exits after idleTimeout
// without any new tasks — but the *userQueue entry stays in s.users
// (no churn on re-entry); only the goroutine is reclaimed.
//
// Producer / worker handoff (subtle — read before touching the state
// transitions below).
//
// Invariant: when drain returns, uq.state is one of
//
//	(A) stateIdle    — the next Execute's CAS(Idle→Running) will
//	                   succeed and spawn a replacement via `go drain`.
//	(B) stateRunning — a producer's CAS already won and has already
//	                   spawned a replacement.
//
// The invariant holds because drain stores stateIdle BEFORE inspecting
// len(uq.tasks). That store publishes "I am about to exit"; any
// producer that enqueues after this store will observe stateIdle on
// its own CAS and take responsibility for the replacement worker.
//
// Every interesting interleaving (T = the producer's task, sent via
// `uq.tasks <- T`; "✓ T drained by X" means worker X eventually runs T).
// The diagrams below show timelines as two columns: worker events on the
// left, producer events on the right.
//
// S1. Worker is actively running; not yet in the <-idle.C branch.
//
//	worker (state=Running)         producer
//	----------------------------   ----------------------------
//	                               uq.tasks <- T
//	                               CAS(Idle→Running)  FAILS
//	                               (relies on current worker)
//	next select iteration:
//	<-uq.tasks reads T
//	✓ T drained by current worker
//
// S2. Another producer already flipped state to Running.
//
//	producer A                     producer B
//	----------------------------   ----------------------------
//	uq.tasks <- tA
//	CAS(Idle→Running)  OK
//	go drain   ─► spawns W         uq.tasks <- tB
//	                               CAS(Idle→Running)  FAILS
//	                               (relies on W)
//	✓ tA, tB both drained by W
//
// S3. Path B: worker sits between Store(Idle) and the len check; producer enqueues, but the worker's reclaim CAS wins.
//
//	worker                         producer
//	----------------------------   ----------------------------
//	<-idle.C fires
//	state.Store(Idle)
//	                               uq.tasks <- T
//	len(uq.tasks)==0 ?  false
//	CAS(Idle→Running)  OK
//	idle.Reset; continue
//	                               CAS(Idle→Running)  FAILS
//	                               (relies on reclaimed worker)
//	✓ T drained by reclaimed worker
//
// S4. Path C: same as S3, but the producer's CAS wins first.
//
//	worker                         producer
//	----------------------------   ----------------------------
//	<-idle.C fires
//	state.Store(Idle)
//	                               uq.tasks <- T
//	                               CAS(Idle→Running)  OK
//	                               go drain   ─► spawns W'
//	len(uq.tasks)==0 ?  false
//	CAS(Idle→Running)  FAILS    (state is Running, set by producer)
//	return (Path C)    -- old worker exits
//	✓ T drained by W'
//
// S5. Path A: worker sees the queue empty and exits cleanly; the producer arrives afterwards.
//
//	worker                         producer
//	----------------------------   ----------------------------
//	<-idle.C fires
//	state.Store(Idle)
//	len(uq.tasks)==0 ?  true
//	return (Path A)    -- state still Idle, worker gone
//	                               uq.tasks <- T
//	                               CAS(Idle→Running)  OK
//	                               go drain   ─► spawns W'
//	✓ T drained by W'
//
// --------------------------------------------------------------------
// What cannot happen
// --------------------------------------------------------------------
//
// "T sits in uq.tasks, no worker is alive, and producer's CAS failed."
//
// That would require producer's CAS to see state=Running while no
// worker exists. state=Running is set only by:
//
//	(i)   the original `go drain` on first enqueue;
//	(ii)  the worker's own Path B reclaim CAS;
//	(iii) another producer's successful CAS.
//
// In (i) and (ii) the worker is still in its select loop and has not
// returned. In (iii) the other producer already issued `go drain`
// before this producer could observe Running. So whenever any
// producer's CAS sees Running, some worker is alive to drain T.
func (uq *userQueue) drain(s *UserSequencer) {
	idle := time.NewTimer(s.idleTimeout)
	defer idle.Stop()
	for {
		select {
		case t, ok := <-uq.tasks:
			if !ok {
				return // defensive: nothing closes uq.tasks today.
			}
			seq := s.counterSeq.Add(1)
			v, err := t.fn(seq)
			t.resp <- taskResult{v: v, err: err}
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(s.idleTimeout)

		case <-idle.C:
			// Publish "I am about to exit" BEFORE inspecting the
			// queue. This atomic store is what makes the handoff
			// invariant hold — see scenarios S3/S4/S5 above.
			uq.state.Store(stateIdle)

			if len(uq.tasks) == 0 {
				// Path A (scenario S5): queue empty at this load.
				// Any producer that enqueues later sees stateIdle
				// on its CAS and spawns a replacement. Safe to exit.
				return
			}

			if uq.state.CompareAndSwap(stateIdle, stateRunning) {
				// Path B (scenario S3): a producer raced in between
				// our Store(Idle) and the len check, but we still
				// hold the worker seat — reclaim it and keep draining.
				idle.Reset(s.idleTimeout)
				continue
			}

			// Path C (scenario S4): CAS failed ⇒ a producer's CAS
			// already flipped state to Running and has already
			// spawned a fresh `go drain`. Safe to exit; the
			// replacement will drain the queue.
			return
		}
	}
}
