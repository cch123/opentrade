package hub

import (
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
)

type fakeSink struct {
	id     string
	userID string
	cap    int
	mu     sync.Mutex
	out    [][]byte
	// drop forces the next TrySend to fail (simulates a full channel).
	dropped atomic.Int64
}

func newSink(id, user string, cap int) *fakeSink {
	return &fakeSink{id: id, userID: user, cap: cap}
}

func (f *fakeSink) ID() string     { return f.id }
func (f *fakeSink) UserID() string { return f.userID }
func (f *fakeSink) TrySend(p []byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cap > 0 && len(f.out) >= f.cap {
		f.dropped.Add(1)
		return false
	}
	f.out = append(f.out, p)
	return true
}
func (f *fakeSink) Out() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([][]byte, len(f.out))
	copy(cp, f.out)
	return cp
}

func newHub() *Hub { return New(zap.NewNop()) }

func TestRegister_Unregister_CleansUpIndexes(t *testing.T) {
	h := newHub()
	s := newSink("c1", "u1", 10)
	h.Register(s)
	if h.ConnCount() != 1 {
		t.Fatalf("conns: %d", h.ConnCount())
	}
	h.Subscribe("c1", []string{"trade@BTC"})
	if got := h.StreamSubscribers("trade@BTC"); got != 1 {
		t.Fatalf("stream subscribers: %d", got)
	}

	h.Unregister("c1")
	if h.ConnCount() != 0 {
		t.Errorf("conns after unregister: %d", h.ConnCount())
	}
	if h.StreamSubscribers("trade@BTC") != 0 {
		t.Errorf("stream index not cleaned up")
	}
	// Sending to the removed user / stream is a no-op.
	if sent, _ := h.SendUser("u1", []byte("x")); sent != 0 {
		t.Errorf("expected no delivery after unregister")
	}
}

func TestSubscribe_Idempotent(t *testing.T) {
	h := newHub()
	s := newSink("c1", "", 10)
	h.Register(s)
	added1 := h.Subscribe("c1", []string{"a", "b"})
	added2 := h.Subscribe("c1", []string{"b", "c"})
	if len(added1) != 2 || len(added2) != 1 || added2[0] != "c" {
		t.Errorf("subscribe idempotency: added1=%v added2=%v", added1, added2)
	}
	// Empty strings ignored.
	empty := h.Subscribe("c1", []string{""})
	if len(empty) != 0 {
		t.Errorf("empty subscribe should be noop: %v", empty)
	}
}

func TestUnsubscribe_OnlyReportsActual(t *testing.T) {
	h := newHub()
	s := newSink("c1", "", 10)
	h.Register(s)
	h.Subscribe("c1", []string{"a", "b"})
	removed := h.Unsubscribe("c1", []string{"a", "z"})
	if len(removed) != 1 || removed[0] != "a" {
		t.Errorf("expected only 'a' removed: %v", removed)
	}
	if h.StreamSubscribers("a") != 0 {
		t.Error("'a' not cleaned")
	}
	if h.StreamSubscribers("b") != 1 {
		t.Error("'b' disturbed")
	}
}

func TestBroadcastStream_DeliversToSubscribersOnly(t *testing.T) {
	h := newHub()
	a := newSink("a", "", 10)
	b := newSink("b", "", 10)
	c := newSink("c", "", 10)
	for _, s := range []*fakeSink{a, b, c} {
		h.Register(s)
	}
	h.Subscribe("a", []string{"trade@X"})
	h.Subscribe("b", []string{"trade@X"})
	// c not subscribed

	sent, dropped := h.BroadcastStream("trade@X", []byte("hello"))
	if sent != 2 || dropped != 0 {
		t.Fatalf("sent=%d dropped=%d", sent, dropped)
	}
	if len(a.Out()) != 1 || len(b.Out()) != 1 {
		t.Errorf("a/b did not both receive: a=%d b=%d", len(a.Out()), len(b.Out()))
	}
	if len(c.Out()) != 0 {
		t.Errorf("c should not receive: %d", len(c.Out()))
	}
}

func TestBroadcastStream_SlowConsumerDropped(t *testing.T) {
	h := newHub()
	fast := newSink("fast", "", 10)
	slow := newSink("slow", "", 1) // capacity 1 → second send fails
	h.Register(fast)
	h.Register(slow)
	h.Subscribe("fast", []string{"s"})
	h.Subscribe("slow", []string{"s"})

	_, d0 := h.BroadcastStream("s", []byte("1"))
	_, d1 := h.BroadcastStream("s", []byte("2"))
	if d0 != 0 || d1 != 1 {
		t.Errorf("drop counts: d0=%d d1=%d", d0, d1)
	}
	if slow.dropped.Load() != 1 {
		t.Errorf("slow dropped counter: %d", slow.dropped.Load())
	}
}

func TestSendUser_FanOutAcrossConnections(t *testing.T) {
	h := newHub()
	// Two connections for the same user.
	c1 := newSink("c1", "u1", 10)
	c2 := newSink("c2", "u1", 10)
	other := newSink("c3", "u2", 10)
	for _, s := range []*fakeSink{c1, c2, other} {
		h.Register(s)
	}
	sent, _ := h.SendUser("u1", []byte("priv"))
	if sent != 2 {
		t.Fatalf("sent: %d", sent)
	}
	if len(c1.Out()) != 1 || len(c2.Out()) != 1 {
		t.Errorf("both u1 conns should receive: %d %d", len(c1.Out()), len(c2.Out()))
	}
	if len(other.Out()) != 0 {
		t.Errorf("u2 should not receive: %d", len(other.Out()))
	}
}

func TestSendUser_EmptyUserNoop(t *testing.T) {
	h := newHub()
	if sent, _ := h.SendUser("", []byte("x")); sent != 0 {
		t.Error("empty user should be no-op")
	}
}

func TestSubscribe_UnknownConnectionIsNoop(t *testing.T) {
	h := newHub()
	added := h.Subscribe("ghost", []string{"a"})
	if added != nil {
		t.Errorf("expected nil for unknown conn, got %v", added)
	}
}
