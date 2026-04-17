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
// to run in parallel. The shard-level seq id (ADR-0018) is assigned to every
// submitted task just before the callback executes.
type UserSequencer struct {
	shardSeq atomic.Uint64
	users    sync.Map // user_id → *userQueue

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

// ShardSeq returns the current shard-level monotonic seq id.
func (s *UserSequencer) ShardSeq() uint64 { return s.shardSeq.Load() }

// SetShardSeq sets the starting seq id. Call before submitting any tasks
// (typically used to restore from a snapshot).
func (s *UserSequencer) SetShardSeq(seq uint64) { s.shardSeq.Store(seq) }

// Execute enqueues fn on userID's FIFO queue and blocks until it runs.
// The callback receives a freshly allocated shard-level monotonic seq id.
// Returns ErrQueueFull if the user's channel is full.
//
// fn MUST NOT block indefinitely: it holds the user's serializer until it
// returns. All I/O inside fn (Kafka produce, etc.) should have a finite
// timeout.
func (s *UserSequencer) Execute(userID string, fn func(seqID uint64) (any, error)) (any, error) {
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
	fn   func(seqID uint64) (any, error)
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

// drain is the worker goroutine. Exits after idleTimeout has elapsed without
// any new tasks, releasing the user's seat in the goroutine pool.
func (uq *userQueue) drain(s *UserSequencer) {
	idle := time.NewTimer(s.idleTimeout)
	defer idle.Stop()
	for {
		select {
		case t, ok := <-uq.tasks:
			if !ok {
				return
			}
			seq := s.shardSeq.Add(1)
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
			// Mark idle; verify the channel is actually empty before exit.
			uq.state.Store(stateIdle)
			if len(uq.tasks) == 0 {
				return
			}
			// A producer raced; reclaim the worker and continue.
			if uq.state.CompareAndSwap(stateIdle, stateRunning) {
				idle.Reset(s.idleTimeout)
				continue
			}
			return
		}
	}
}
