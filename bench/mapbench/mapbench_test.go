// Package mapbench compares concurrent map implementations for Counter's
// per-user sequencer (ADR-0018) access pattern.
//
// Production pattern:
//   - ~200k unique users on a shard
//   - ~10k ops/sec peak, every op starts with Load(userID) on the fast path
//   - first-access races only on user onboarding
//   - entries are never explicitly deleted (idle worker exits, userQueue stays)
//
// Three implementations:
//   - sync.Map                          (stdlib, read-optimized)
//   - github.com/puzpuzpuz/xsync/v4     (CLHT concurrent hash table)
//   - github.com/orcaman/concurrent-map (classic shard + RWMutex)
package mapbench

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/puzpuzpuz/xsync/v4"
)

const (
	numUsers    = 200_000 // production shard size
	writeBatch  = 1024    // amortize atomic counter in write-heavy scenario
	loadStride  = 7919    // coprime prime to spread goroutines across key space
	mixedInsert = 100     // 1-in-100 ops in mixed scenario inserts a new user
)

// userQueue stands in for counter's real per-user payload. Same rough size
// (a channel + state) so allocation cost is realistic.
type userQueue struct {
	ch    chan int
	state int32
}

func newUserQueue() *userQueue { return &userQueue{ch: make(chan int, 256)} }

// --- abstraction ------------------------------------------------------------

type impl interface {
	name() string
	preload(keys []string)
	getOrCreate(key string) *userQueue
}

// --- sync.Map ---------------------------------------------------------------

type syncImpl struct{ m sync.Map }

func (s *syncImpl) name() string { return "sync.Map" }
func (s *syncImpl) preload(ks []string) {
	for _, k := range ks {
		s.m.Store(k, newUserQueue())
	}
}
func (s *syncImpl) getOrCreate(k string) *userQueue {
	if v, ok := s.m.Load(k); ok {
		return v.(*userQueue)
	}
	uq := newUserQueue()
	actual, _ := s.m.LoadOrStore(k, uq)
	return actual.(*userQueue)
}

// --- puzpuzpuz/xsync v4 Map -------------------------------------------------

type xsyncImpl struct{ m *xsync.Map[string, *userQueue] }

func newXsyncImpl() *xsyncImpl { return &xsyncImpl{m: xsync.NewMap[string, *userQueue]()} }
func (x *xsyncImpl) name() string { return "xsync.Map" }
func (x *xsyncImpl) preload(ks []string) {
	for _, k := range ks {
		x.m.Store(k, newUserQueue())
	}
}
func (x *xsyncImpl) getOrCreate(k string) *userQueue {
	if v, ok := x.m.Load(k); ok {
		return v
	}
	uq := newUserQueue()
	actual, _ := x.m.LoadOrStore(k, uq)
	return actual
}

// --- orcaman/concurrent-map v2 ----------------------------------------------

type cmapImpl struct{ m cmap.ConcurrentMap[string, *userQueue] }

func newCMapImpl() *cmapImpl { return &cmapImpl{m: cmap.New[*userQueue]()} }
func (c *cmapImpl) name() string { return "cmap(v2)" }
func (c *cmapImpl) preload(ks []string) {
	for _, k := range ks {
		c.m.Set(k, newUserQueue())
	}
}
func (c *cmapImpl) getOrCreate(k string) *userQueue {
	if v, ok := c.m.Get(k); ok {
		return v
	}
	uq := newUserQueue()
	if c.m.SetIfAbsent(k, uq) {
		return uq
	}
	v, _ := c.m.Get(k)
	return v
}

// --- scenarios --------------------------------------------------------------

func makeKeys(n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = "user-" + strconv.Itoa(i)
	}
	return ks
}

// SteadyHit: all 200k users pre-populated, every op is Load hit.
// Mirrors the dominant production path — stable user set, read-heavy.
func benchSteadyHit(b *testing.B, m impl) {
	keys := makeKeys(numUsers)
	m.preload(keys)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = m.getOrCreate(keys[(i*loadStride)%numUsers])
			i++
		}
	})
}

// Mixed: 99% Load hit on existing users, 1% brand-new user insertion.
// Simulates sustained trading plus trickle of onboarding.
func benchMixed(b *testing.B, m impl) {
	keys := makeKeys(numUsers)
	m.preload(keys)

	var newKeyCounter atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// per-goroutine batch of monotonic counter values for new-user keys,
		// so we don't hammer a single atomic on every iteration.
		var base uint64
		var local uint64
		i := 0
		for pb.Next() {
			if i%mixedInsert == 0 {
				if local == 0 {
					base = newKeyCounter.Add(writeBatch) - writeBatch
					local = writeBatch
				}
				local--
				_ = m.getOrCreate("new-" + strconv.FormatUint(base+(writeBatch-local-1)+uint64(numUsers), 10))
			} else {
				_ = m.getOrCreate(keys[(i*loadStride)%numUsers])
			}
			i++
		}
	})
}

// NewKeyEach: every op inserts a brand-new key. Not realistic for steady
// trading, but exposes worst-case LoadOrStore / write-path cost.
func benchNewKeyEach(b *testing.B, m impl) {
	var counter atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var base uint64
		var local uint64
		for pb.Next() {
			if local == 0 {
				base = counter.Add(writeBatch) - writeBatch
				local = writeBatch
			}
			local--
			_ = m.getOrCreate("u-" + strconv.FormatUint(base+(writeBatch-local-1), 10))
		}
	})
}

// --- benchmark registrations ------------------------------------------------

var impls = []struct {
	name    string
	factory func() impl
}{
	{"sync.Map", func() impl { return &syncImpl{} }},
	{"xsync.Map", func() impl { return newXsyncImpl() }},
	{"cmap(v2)", func() impl { return newCMapImpl() }},
}

func BenchmarkSteadyHit(b *testing.B) {
	for _, it := range impls {
		b.Run(it.name, func(b *testing.B) { benchSteadyHit(b, it.factory()) })
	}
}

func BenchmarkMixed99Load1Insert(b *testing.B) {
	for _, it := range impls {
		b.Run(it.name, func(b *testing.B) { benchMixed(b, it.factory()) })
	}
}

func BenchmarkNewKeyEach(b *testing.B) {
	for _, it := range impls {
		b.Run(it.name, func(b *testing.B) { benchNewKeyEach(b, it.factory()) })
	}
}
