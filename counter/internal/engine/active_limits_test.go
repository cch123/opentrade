package engine

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

// ADR-0054: OrderStore.activeLimits tracks per-(user, symbol) active LIMIT
// orders. The tests below exercise every transition that must move the
// counter: Insert, UpdateStatus to various terminal states, RestoreInsert,
// partial fills (non-terminal, must NOT decrement), and MARKET orders
// (never counted).

func newLimit(id uint64, user, sym string, status OrderStatus) *Order {
	return &Order{
		ID:     id,
		UserID: user,
		Symbol: sym,
		Type:   OrderTypeLimit,
		Status: status,
		Price:  dec.New("100"),
		Qty:    dec.New("1"),
	}
}

func TestActiveLimits_InsertIncrements(t *testing.T) {
	s := newOrderStore()
	if err := s.Insert(newLimit(1, "u1", "BTC-USDT", OrderStatusPendingNew)); err != nil {
		t.Fatal(err)
	}
	if got := s.CountActiveLimits("u1", "BTC-USDT"); got != 1 {
		t.Errorf("count = %d, want 1", got)
	}
	if got := s.CountActiveLimits("u1", "ETH-USDT"); got != 0 {
		t.Errorf("other symbol leak: %d", got)
	}
	if got := s.CountActiveLimits("u2", "BTC-USDT"); got != 0 {
		t.Errorf("other user leak: %d", got)
	}
}

func TestActiveLimits_MarketOrdersNotCounted(t *testing.T) {
	s := newOrderStore()
	mkt := &Order{
		ID: 1, UserID: "u1", Symbol: "BTC-USDT",
		Type: OrderTypeMarket, Status: OrderStatusPendingNew,
		Price: dec.Zero, Qty: dec.New("1"),
	}
	if err := s.Insert(mkt); err != nil {
		t.Fatal(err)
	}
	if got := s.CountActiveLimits("u1", "BTC-USDT"); got != 0 {
		t.Errorf("MARKET must not count, got %d", got)
	}
}

func TestActiveLimits_PartialFillPreservesCount(t *testing.T) {
	s := newOrderStore()
	_ = s.Insert(newLimit(1, "u1", "BTC-USDT", OrderStatusPendingNew))
	if _, err := s.UpdateStatus(1, OrderStatusPartiallyFilled, 0); err != nil {
		t.Fatal(err)
	}
	if got := s.CountActiveLimits("u1", "BTC-USDT"); got != 1 {
		t.Errorf("partial fill must keep slot, got %d", got)
	}
}

func TestActiveLimits_TerminalDecrementsAndCleansEmptyKeys(t *testing.T) {
	cases := []struct {
		name   string
		target OrderStatus
	}{
		{"filled", OrderStatusFilled},
		{"canceled", OrderStatusCanceled},
		{"rejected", OrderStatusRejected},
		{"expired", OrderStatusExpired},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newOrderStore()
			_ = s.Insert(newLimit(1, "u1", "BTC-USDT", OrderStatusPendingNew))
			if _, err := s.UpdateStatus(1, c.target, 0); err != nil {
				t.Fatal(err)
			}
			if got := s.CountActiveLimits("u1", "BTC-USDT"); got != 0 {
				t.Errorf("terminal %s should zero count, got %d", c.name, got)
			}
			// The outer user key must be dropped so unused users don't leak.
			if _, ok := s.activeLimits["u1"]; ok {
				t.Errorf("empty user bucket leaked")
			}
		})
	}
}

func TestActiveLimits_PendingCancelThenCanceled(t *testing.T) {
	// PENDING_CANCEL is still active (Match may race and fill instead); the
	// counter must only drop when the terminal state lands.
	s := newOrderStore()
	_ = s.Insert(newLimit(1, "u1", "BTC-USDT", OrderStatusPendingNew))
	if _, err := s.UpdateStatus(1, OrderStatusPendingCancel, 0); err != nil {
		t.Fatal(err)
	}
	if got := s.CountActiveLimits("u1", "BTC-USDT"); got != 1 {
		t.Errorf("PENDING_CANCEL still active, got %d", got)
	}
	if _, err := s.UpdateStatus(1, OrderStatusCanceled, 0); err != nil {
		t.Fatal(err)
	}
	if got := s.CountActiveLimits("u1", "BTC-USDT"); got != 0 {
		t.Errorf("after Canceled got %d, want 0", got)
	}
}

func TestActiveLimits_RestoreInsertRebuildsCount(t *testing.T) {
	// Simulate snapshot restore: mix of active and terminal orders. Only
	// active LIMITs should contribute to the counter.
	s := newOrderStore()
	s.RestoreInsert(newLimit(1, "u1", "BTC-USDT", OrderStatusNew))
	s.RestoreInsert(newLimit(2, "u1", "BTC-USDT", OrderStatusPartiallyFilled))
	s.RestoreInsert(newLimit(3, "u1", "BTC-USDT", OrderStatusFilled))   // terminal → skip
	s.RestoreInsert(newLimit(4, "u1", "ETH-USDT", OrderStatusNew))
	s.RestoreInsert(newLimit(5, "u2", "BTC-USDT", OrderStatusPendingCancel))

	if got := s.CountActiveLimits("u1", "BTC-USDT"); got != 2 {
		t.Errorf("u1 BTC got %d, want 2", got)
	}
	if got := s.CountActiveLimits("u1", "ETH-USDT"); got != 1 {
		t.Errorf("u1 ETH got %d, want 1", got)
	}
	if got := s.CountActiveLimits("u2", "BTC-USDT"); got != 1 {
		t.Errorf("u2 BTC got %d, want 1", got)
	}
}

func TestActiveLimits_MultipleOrdersSameUserSymbol(t *testing.T) {
	s := newOrderStore()
	for i := uint64(1); i <= 5; i++ {
		o := newLimit(i, "u1", "BTC-USDT", OrderStatusPendingNew)
		if err := s.Insert(o); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.CountActiveLimits("u1", "BTC-USDT"); got != 5 {
		t.Errorf("got %d, want 5", got)
	}
	// Drop two to terminal.
	if _, err := s.UpdateStatus(1, OrderStatusFilled, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateStatus(2, OrderStatusCanceled, 0); err != nil {
		t.Fatal(err)
	}
	if got := s.CountActiveLimits("u1", "BTC-USDT"); got != 3 {
		t.Errorf("got %d, want 3", got)
	}
}
