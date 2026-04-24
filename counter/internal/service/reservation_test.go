package service

import (
	"context"
	"errors"
	"testing"

	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

func seedBalance(svc *Service, user, asset string, available dec.Decimal) {
	svc.state.Account(user).PutForRestore(asset, engine.Balance{Available: available, Frozen: dec.Zero})
}

func TestReserve_HappyPath(t *testing.T) {
	svc, state, _ := newFixture(t)
	seedBalance(svc, "u1", "USDT", dec.New("1000"))

	res, err := svc.Reserve(context.Background(), ReserveRequest{
		UserID:        "u1",
		ReservationID: "trig-1",
		Symbol:        "BTC-USDT",
		Side:          engine.SideBid,
		OrderType:     engine.OrderTypeLimit,
		Price:         dec.New("100"),
		Qty:           dec.New("1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted || res.Asset != "USDT" || res.Amount.String() != "100" {
		t.Errorf("result: %+v", res)
	}
	// Balance: Available 1000 → 900, Frozen 0 → 100.
	b := state.Balance("u1", "USDT")
	if b.Available.String() != "900" || b.Frozen.String() != "100" {
		t.Errorf("balance: %+v", b)
	}
	// Record exists.
	if got := state.LookupReservation("trig-1"); got == nil || got.Amount.String() != "100" {
		t.Errorf("reservation: %+v", got)
	}
}

func TestReserve_IdempotentByRefID(t *testing.T) {
	svc, _, _ := newFixture(t)
	seedBalance(svc, "u1", "USDT", dec.New("1000"))
	req := ReserveRequest{
		UserID:        "u1",
		ReservationID: "trig-1",
		Symbol:        "BTC-USDT",
		Side:          engine.SideBid,
		OrderType:     engine.OrderTypeLimit,
		Price:         dec.New("100"),
		Qty:           dec.New("1"),
	}
	res1, err := svc.Reserve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	res2, err := svc.Reserve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !res1.Accepted || res2.Accepted {
		t.Errorf("first should accept, second should not: %v %v", res1.Accepted, res2.Accepted)
	}
}

func TestReserve_InsufficientBalanceReturnsError(t *testing.T) {
	svc, _, _ := newFixture(t)
	seedBalance(svc, "u1", "USDT", dec.New("50"))
	_, err := svc.Reserve(context.Background(), ReserveRequest{
		UserID:        "u1",
		ReservationID: "trig-1",
		Symbol:        "BTC-USDT",
		Side:          engine.SideBid,
		OrderType:     engine.OrderTypeLimit,
		Price:         dec.New("100"),
		Qty:           dec.New("1"),
	})
	if !errors.Is(err, engine.ErrInsufficientAvailable) {
		t.Errorf("err: %v", err)
	}
}

func TestReleaseReservation_HappyPath(t *testing.T) {
	svc, state, _ := newFixture(t)
	seedBalance(svc, "u1", "USDT", dec.New("1000"))
	_, _ = svc.Reserve(context.Background(), ReserveRequest{
		UserID: "u1", ReservationID: "trig-1",
		Symbol: "BTC-USDT", Side: engine.SideBid, OrderType: engine.OrderTypeLimit,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	res, err := svc.ReleaseReservation(context.Background(), ReleaseReservationRequest{
		UserID:        "u1",
		ReservationID: "trig-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted {
		t.Errorf("expected accepted=true")
	}
	b := state.Balance("u1", "USDT")
	if b.Available.String() != "1000" || b.Frozen.String() != "0" {
		t.Errorf("balance not restored: %+v", b)
	}
	if state.LookupReservation("trig-1") != nil {
		t.Errorf("reservation record still present")
	}
}

func TestReleaseReservation_UnknownIsIdempotent(t *testing.T) {
	svc, _, _ := newFixture(t)
	res, err := svc.ReleaseReservation(context.Background(), ReleaseReservationRequest{
		UserID:        "u1",
		ReservationID: "never-existed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted {
		t.Errorf("accepted should be false for unknown ref")
	}
}

func TestPlaceOrder_UsingReservation(t *testing.T) {
	svc, state, _, txn := newOrderFixture(t)
	seedBalance(svc, "u1", "USDT", dec.New("1000"))
	// Reserve 100 USDT for a limit buy.
	if _, err := svc.Reserve(context.Background(), ReserveRequest{
		UserID: "u1", ReservationID: "trig-1",
		Symbol: "BTC-USDT", Side: engine.SideBid, OrderType: engine.OrderTypeLimit,
		Price: dec.New("100"), Qty: dec.New("1"),
	}); err != nil {
		t.Fatal(err)
	}
	// PlaceOrder consuming reservation: balance should stay at (900, 100).
	res, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID:        "u1",
		ClientOrderID: "cli-1",
		Symbol:        "BTC-USDT",
		Side:          engine.SideBid,
		OrderType:     engine.OrderTypeLimit,
		TIF:           engine.TIFGTC,
		Price:         dec.New("100"),
		Qty:           dec.New("1"),
		ReservationID: "trig-1",
	})
	if err != nil || !res.Accepted {
		t.Fatalf("place: res=%+v err=%v", res, err)
	}
	b := state.Balance("u1", "USDT")
	if b.Available.String() != "900" || b.Frozen.String() != "100" {
		t.Errorf("balance drifted: %+v", b)
	}
	if state.LookupReservation("trig-1") != nil {
		t.Errorf("reservation should be consumed")
	}
	if len(txn.pairs) == 0 {
		t.Errorf("expected a journal event")
	}
}

func TestPlaceOrder_ReservationMismatchRejected(t *testing.T) {
	svc, _, _, _ := newOrderFixture(t)
	seedBalance(svc, "u1", "USDT", dec.New("1000"))
	_, _ = svc.Reserve(context.Background(), ReserveRequest{
		UserID: "u1", ReservationID: "trig-1",
		Symbol: "BTC-USDT", Side: engine.SideBid, OrderType: engine.OrderTypeLimit,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	// PlaceOrder with different qty → computed freeze = 200, but reservation is 100.
	res, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID:        "u1",
		ClientOrderID: "cli-1",
		Symbol:        "BTC-USDT",
		Side:          engine.SideBid,
		OrderType:     engine.OrderTypeLimit,
		Price:         dec.New("100"),
		Qty:           dec.New("2"),
		ReservationID: "trig-1",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Accepted {
		t.Errorf("mismatch should reject, got accepted=true")
	}
	if res.RejectReason == "" {
		t.Errorf("reject reason missing")
	}
}

func TestPlaceOrder_UnknownReservationRejected(t *testing.T) {
	svc, _, _, _ := newOrderFixture(t)
	seedBalance(svc, "u1", "USDT", dec.New("1000"))
	res, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID:        "u1",
		Symbol:        "BTC-USDT",
		Side:          engine.SideBid,
		OrderType:     engine.OrderTypeLimit,
		Price:         dec.New("100"),
		Qty:           dec.New("1"),
		ReservationID: "no-such-ref",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Accepted || res.RejectReason == "" {
		t.Errorf("should have been rejected: %+v", res)
	}
}
