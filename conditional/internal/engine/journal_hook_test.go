package engine

import (
	"context"
	"sync"
	"testing"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
)

// recordingSink captures every Emit call for assertions.
type recordingSink struct {
	mu   sync.Mutex
	seen []Conditional
}

func (r *recordingSink) Emit(c *Conditional) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c == nil {
		return
	}
	r.seen = append(r.seen, *c)
}

func (r *recordingSink) snapshot() []Conditional {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Conditional, len(r.seen))
	copy(out, r.seen)
	return out
}

func TestJournal_EmitOnPlace(t *testing.T) {
	e := newEngine(&fakePlacer{})
	sink := &recordingSink{}
	e.SetJournal(sink)

	id, _, _, err := e.Place(context.Background(), goodReq())
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	got := sink.snapshot()
	if len(got) != 1 {
		t.Fatalf("emit count = %d, want 1", len(got))
	}
	if got[0].ID != id || got[0].Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
		t.Fatalf("emit payload: %+v", got[0])
	}
}

func TestJournal_EmitOnCancel(t *testing.T) {
	e := newEngine(&fakePlacer{})
	sink := &recordingSink{}
	e.SetJournal(sink)

	id, _, _, err := e.Place(context.Background(), goodReq())
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	_, _, err = e.Cancel(context.Background(), "u1", id)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	got := sink.snapshot()
	if len(got) != 2 {
		t.Fatalf("emit count = %d, want 2 (PENDING + CANCELED)", len(got))
	}
	if got[1].Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED {
		t.Fatalf("second emit status = %v, want CANCELED", got[1].Status)
	}
}

func TestJournal_NilSinkSafe(t *testing.T) {
	e := newEngine(&fakePlacer{})
	// do not set a sink
	if _, _, _, err := e.Place(context.Background(), goodReq()); err != nil {
		t.Fatalf("place with no sink: %v", err)
	}
}
