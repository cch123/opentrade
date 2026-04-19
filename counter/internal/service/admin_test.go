package service

import (
	"context"
	"errors"
	"testing"

	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// place3Orders seeds 3 live orders across 2 users + 2 symbols on a fresh
// fixture. Returns the service + the order IDs so callers can assert per-
// order outcome.
func place3Orders(t *testing.T) (*Service, []uint64) {
	t.Helper()
	svc, _, _, _ := newOrderFixture(t)
	// u1 buy BTC-USDT — limit buy freezes USDT.
	r1, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", ClientOrderID: "c1", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("50000"), Qty: dec.New("0.01"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// u1 buy ETH-USDT — different symbol same user.
	r2, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", ClientOrderID: "c2", Symbol: "ETH-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("3000"), Qty: dec.New("0.01"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// u2 sell BTC-USDT — different user same symbol as r1.
	r3, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u2", ClientOrderID: "s1", Symbol: "BTC-USDT",
		Side: engine.SideAsk, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("51000"), Qty: dec.New("0.01"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc, []uint64{r1.OrderID, r2.OrderID, r3.OrderID}
}

func TestAdminCancel_ByUser(t *testing.T) {
	svc, ids := place3Orders(t)
	res, err := svc.AdminCancelOrders(context.Background(), AdminCancelFilter{UserID: "u1"})
	if err != nil {
		t.Fatalf("admin cancel: %v", err)
	}
	if res.Cancelled != 2 || res.Skipped != 0 {
		t.Fatalf("cancelled=%d skipped=%d", res.Cancelled, res.Skipped)
	}
	// u2's order must still be live (not PENDING_CANCEL, not terminal).
	o := svc.state.Orders().Get(ids[2])
	if o == nil || o.Status != engine.OrderStatusPendingNew && o.Status != engine.OrderStatusNew {
		t.Fatalf("u2 order status = %v", o.Status)
	}
}

func TestAdminCancel_BySymbol(t *testing.T) {
	svc, ids := place3Orders(t)
	res, err := svc.AdminCancelOrders(context.Background(), AdminCancelFilter{Symbol: "BTC-USDT"})
	if err != nil {
		t.Fatalf("admin cancel: %v", err)
	}
	if res.Cancelled != 2 || res.Skipped != 0 {
		t.Fatalf("cancelled=%d skipped=%d", res.Cancelled, res.Skipped)
	}
	// ETH-USDT (ids[1]) untouched.
	o := svc.state.Orders().Get(ids[1])
	if o == nil {
		t.Fatal("eth order vanished")
	}
	if o.Status == engine.OrderStatusPendingCancel {
		t.Fatal("eth order got cancelled by symbol=BTC-USDT")
	}
}

func TestAdminCancel_ByUserAndSymbolIntersection(t *testing.T) {
	svc, ids := place3Orders(t)
	res, err := svc.AdminCancelOrders(context.Background(), AdminCancelFilter{UserID: "u1", Symbol: "BTC-USDT"})
	if err != nil {
		t.Fatalf("admin cancel: %v", err)
	}
	if res.Cancelled != 1 {
		t.Fatalf("cancelled=%d, want 1", res.Cancelled)
	}
	// Only ids[0] should be pending cancel.
	if s := svc.state.Orders().Get(ids[0]).Status; s != engine.OrderStatusPendingCancel {
		t.Fatalf("ids[0] status = %v", s)
	}
	if s := svc.state.Orders().Get(ids[1]).Status; s == engine.OrderStatusPendingCancel {
		t.Fatalf("ids[1] unexpectedly cancelled")
	}
	if s := svc.state.Orders().Get(ids[2]).Status; s == engine.OrderStatusPendingCancel {
		t.Fatalf("ids[2] unexpectedly cancelled")
	}
}

func TestAdminCancel_EmptyFilterRejected(t *testing.T) {
	svc, _ := place3Orders(t)
	_, err := svc.AdminCancelOrders(context.Background(), AdminCancelFilter{})
	if !errors.Is(err, ErrAdminCancelFilterEmpty) {
		t.Fatalf("err = %v, want ErrAdminCancelFilterEmpty", err)
	}
}

func TestAdminCancel_AlreadyPendingCancelIsSkipped(t *testing.T) {
	svc, ids := place3Orders(t)
	// Pre-cancel id[0] through the normal path.
	if _, err := svc.CancelOrder(context.Background(), CancelOrderRequest{UserID: "u1", OrderID: ids[0]}); err != nil {
		t.Fatal(err)
	}
	res, err := svc.AdminCancelOrders(context.Background(), AdminCancelFilter{UserID: "u1"})
	if err != nil {
		t.Fatal(err)
	}
	// id[0] is already PENDING_CANCEL → skipped; id[1] is cancelled now.
	if res.Cancelled != 1 || res.Skipped != 1 {
		t.Fatalf("cancelled=%d skipped=%d", res.Cancelled, res.Skipped)
	}
}

func TestAdminCancel_WrongShardForUser(t *testing.T) {
	svc, _ := place3Orders(t)
	// Flip the service into a 2-shard world where u1 hashes to shard 1 (not
	// this shard 0). Any filter that names u1 should bail out before
	// touching state.
	svc.cfg.TotalShards = 2
	if svc.OwnsUser("u1") {
		// If u1 happens to hash to shard 0 the assertion below is vacuous;
		// pick the user that this shard does NOT own.
		svc.cfg.ShardID = 1
	}
	// Pick a user id we know does not belong to this shard: try "u1" first,
	// and if OwnsUser("u1") now true, use a different id.
	nonOwned := "u1"
	if svc.OwnsUser(nonOwned) {
		nonOwned = "u-other"
	}
	_, err := svc.AdminCancelOrders(context.Background(), AdminCancelFilter{UserID: nonOwned})
	if !errors.Is(err, ErrWrongShard) {
		t.Fatalf("err = %v, want ErrWrongShard", err)
	}
}
