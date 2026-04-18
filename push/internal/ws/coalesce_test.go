package ws

import (
	"testing"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/push/internal/hub"
)

// newBareConn builds a Conn sufficient for testing the queue/coalesce
// bookkeeping. It deliberately skips Start() so no read/write goroutines
// run and no real websocket is touched.
func newBareConn(t *testing.T) *Conn {
	t.Helper()
	h := hub.New(zap.NewNop())
	return NewConn("c1", "u1", nil, h, Config{SendBuffer: 4}, zap.NewNop())
}

func TestTrySendCoalesce_StoresLatestByKey(t *testing.T) {
	c := newBareConn(t)
	if !c.TrySendCoalesce("k", []byte("one")) {
		t.Fatal("initial send should succeed")
	}
	if !c.TrySendCoalesce("k", []byte("two")) {
		t.Fatal("overwrite should succeed")
	}
	if !c.TrySendCoalesce("other", []byte("three")) {
		t.Fatal("second key should succeed")
	}
	c.coalMu.Lock()
	defer c.coalMu.Unlock()
	if got := string(c.coalMap["k"]); got != "two" {
		t.Errorf("k latest = %q, want two", got)
	}
	if got := string(c.coalMap["other"]); got != "three" {
		t.Errorf("other latest = %q, want three", got)
	}
}

func TestTrySendCoalesce_SignalSent(t *testing.T) {
	c := newBareConn(t)
	c.TrySendCoalesce("k", []byte("x"))
	select {
	case <-c.coalSig:
	default:
		t.Error("expected coalesce signal after TrySendCoalesce")
	}
}

func TestTrySendCoalesce_ClosedConnRejects(t *testing.T) {
	c := newBareConn(t)
	c.closed.Store(true)
	if c.TrySendCoalesce("k", []byte("x")) {
		t.Error("closed conn must not accept coalesced sends")
	}
}

// TestTrySend_ClosedConnRejects confirms the non-coalesced path still short
// circuits once the conn is marked closed.
func TestTrySend_ClosedConnRejects(t *testing.T) {
	c := newBareConn(t)
	c.closed.Store(true)
	if c.TrySend([]byte("x")) {
		t.Error("closed conn must not accept sends")
	}
}
