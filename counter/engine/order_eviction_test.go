package engine

import (
	"testing"
	"time"

	"github.com/xargin/opentrade/pkg/dec"
)

// seedTerminalOrder is a test helper: inserts a NEW order then transitions
// it directly to newStatus with the given updatedAtMS. Returns the live
// order pointer for assertions.
func seedTerminalOrder(t *testing.T, s *OrderStore, id uint64, newStatus OrderStatus, updatedAtMS int64) *Order {
	t.Helper()
	o := &Order{
		ID:           id,
		UserID:       "u1",
		Symbol:       "BTC-USDT",
		Side:         SideBid,
		Type:         OrderTypeLimit,
		TIF:          TIFGTC,
		Price:        dec.New("1"),
		Qty:          dec.New("1"),
		FilledQty:    dec.Zero,
		FrozenAmount: dec.New("1"),
		FrozenSpent:  dec.Zero,
		Status:       OrderStatusNew,
		CreatedAt:    1000,
		UpdatedAt:    1000,
	}
	if err := s.Insert(o); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	got, err := s.UpdateStatus(id, newStatus, updatedAtMS)
	if err != nil {
		t.Fatalf("seed update: %v", err)
	}
	return got
}

// TestUpdateStatus_SetsTerminatedAt verifies every terminal transition
// stamps TerminatedAt; non-terminal transitions leave it at zero.
func TestUpdateStatus_SetsTerminatedAt(t *testing.T) {
	cases := []struct {
		name         string
		newStatus    OrderStatus
		updatedAtMS  int64
		wantTermAt   int64
		wantTerminal bool
	}{
		{"canceled_with_ts", OrderStatusCanceled, 12345, 12345, true},
		{"filled_with_ts", OrderStatusFilled, 12345, 12345, true},
		{"rejected_with_ts", OrderStatusRejected, 12345, 12345, true},
		{"expired_with_ts", OrderStatusExpired, 12345, 12345, true},
		{"partial_fill_no_term", OrderStatusPartiallyFilled, 12345, 0, false},
		{"pending_cancel_no_term", OrderStatusPendingCancel, 12345, 0, false},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newOrderStore()
			o := seedTerminalOrder(t, s, uint64(i+1), tc.newStatus, tc.updatedAtMS)
			if tc.wantTerminal && !o.Status.IsTerminal() {
				t.Fatalf("status %s should be terminal", o.Status)
			}
			if o.TerminatedAt != tc.wantTermAt {
				t.Fatalf("TerminatedAt = %d, want %d", o.TerminatedAt, tc.wantTermAt)
			}
		})
	}
}

// TestUpdateStatus_TerminatedAtFallbackOnZero exercises the
// settlement.go:221 path: UpdateStatus called with updatedAtMS=0 on a
// terminal transition must fall back to wall clock so the evictor does
// not see a 1970 timestamp.
func TestUpdateStatus_TerminatedAtFallbackOnZero(t *testing.T) {
	s := newOrderStore()
	before := time.Now().UnixMilli()
	o := seedTerminalOrder(t, s, 1, OrderStatusFilled, 0)
	after := time.Now().UnixMilli()
	if o.TerminatedAt < before || o.TerminatedAt > after {
		t.Fatalf("TerminatedAt = %d, expected within [%d, %d]", o.TerminatedAt, before, after)
	}
}

// TestUpdateStatus_TerminatedAtIdempotent verifies a second terminal
// transition (defensive — should not happen in practice) leaves the
// original TerminatedAt untouched.
func TestUpdateStatus_TerminatedAtIdempotent(t *testing.T) {
	s := newOrderStore()
	o := seedTerminalOrder(t, s, 1, OrderStatusCanceled, 10000)
	first := o.TerminatedAt
	// Second attempt (would be a bug somewhere, but UpdateStatus should
	// not re-stamp).
	got, err := s.UpdateStatus(1, OrderStatusExpired, 99999)
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
	if got.TerminatedAt != first {
		t.Fatalf("TerminatedAt re-stamped: got %d want %d", got.TerminatedAt, first)
	}
}

// TestCandidatesForEvict_RetentionBoundary tests the evict candidate
// selection honours the retention window.
func TestCandidatesForEvict_RetentionBoundary(t *testing.T) {
	s := newOrderStore()
	// Three orders at distinct terminated-at ages.
	// now = 10_000_000 ms; retention = 1_000_000 ms.
	now := int64(10_000_000)
	retention := int64(1_000_000)
	seedTerminalOrder(t, s, 1, OrderStatusCanceled, now-retention-1) // just past window -> evictable
	seedTerminalOrder(t, s, 2, OrderStatusCanceled, now-retention+1) // within window -> not yet
	seedTerminalOrder(t, s, 3, OrderStatusCanceled, now-retention*5) // deep in past -> evictable
	// Seed a non-terminal order; must never surface.
	active := &Order{
		ID:           4,
		UserID:       "u1",
		Symbol:       "BTC-USDT",
		Side:         SideBid,
		Type:         OrderTypeLimit,
		TIF:          TIFGTC,
		Price:        dec.New("1"),
		Qty:          dec.New("1"),
		FrozenAmount: dec.New("1"),
		FrozenSpent:  dec.Zero,
		FilledQty:    dec.Zero,
		Status:       OrderStatusNew,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.Insert(active); err != nil {
		t.Fatalf("seed active: %v", err)
	}

	got := s.CandidatesForEvict(now, retention, 0)
	if len(got) != 2 {
		t.Fatalf("candidates len = %d, want 2", len(got))
	}
	ids := map[uint64]bool{}
	for _, o := range got {
		ids[o.ID] = true
	}
	if !ids[1] || !ids[3] {
		t.Fatalf("missing expected ids: got %v", ids)
	}
	if ids[2] || ids[4] {
		t.Fatalf("should not include in-window or active: %v", ids)
	}
}

// TestCandidatesForEvict_BatchCap bounds batch size.
func TestCandidatesForEvict_BatchCap(t *testing.T) {
	s := newOrderStore()
	now := int64(10_000_000)
	retention := int64(1_000_000)
	// Seed 20 evict-eligible terminal orders.
	for i := uint64(1); i <= 20; i++ {
		seedTerminalOrder(t, s, i, OrderStatusCanceled, now-retention*2)
	}
	got := s.CandidatesForEvict(now, retention, 5)
	if len(got) != 5 {
		t.Fatalf("capped candidates len = %d, want 5", len(got))
	}
}

// TestCandidatesForEvict_SkipsZeroTerminatedAt covers legacy-snapshot
// orders that have IsTerminal()==true but TerminatedAt==0; they must
// not be selected to avoid 1970-based retention math.
func TestCandidatesForEvict_SkipsZeroTerminatedAt(t *testing.T) {
	s := newOrderStore()
	// Simulate a pre-M3 snapshot restore: direct Insert with terminal
	// Status but TerminatedAt=0.
	s.RestoreInsert(&Order{
		ID:           1,
		UserID:       "u1",
		Symbol:       "BTC-USDT",
		Side:         SideBid,
		Type:         OrderTypeLimit,
		TIF:          TIFGTC,
		Price:        dec.New("1"),
		Qty:          dec.New("1"),
		FrozenAmount: dec.New("1"),
		FrozenSpent:  dec.Zero,
		FilledQty:    dec.New("1"),
		Status:       OrderStatusFilled,
		CreatedAt:    0,
		UpdatedAt:    0,
		TerminatedAt: 0, // legacy
	})
	got := s.CandidatesForEvict(time.Now().UnixMilli(), 60_000, 10)
	if len(got) != 0 {
		t.Fatalf("legacy terminal order (TerminatedAt=0) must be skipped; got %d", len(got))
	}
}

// TestOrderStore_Delete verifies Delete removes the order + indices and
// is idempotent on the missing-id path (ErrOrderNotFound, not panic).
func TestOrderStore_Delete(t *testing.T) {
	s := newOrderStore()
	o := seedTerminalOrder(t, s, 1, OrderStatusCanceled, 10000)
	// activeByCOID should already be empty for this order (terminal
	// transition dropped it), but add a defensive seed to exercise the
	// Delete path's re-check.
	o.ClientOrderID = "coid-x"
	// Delete the order.
	if err := s.Delete(1); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if o := s.Get(1); o != nil {
		t.Fatalf("Get after Delete should be nil, got %+v", o)
	}
	// Second Delete is the crash-and-retry path: must not panic.
	if err := s.Delete(1); err == nil {
		t.Fatal("second Delete should return ErrOrderNotFound")
	}
}
