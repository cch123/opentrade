package service

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// ADR-0054: PlaceOrder must reject a LIMIT order that would exceed the
// per-(user, symbol) MAX_OPEN_LIMIT_ORDERS cap. Tests cover the default
// cap path, the SymbolLookup-override path, the dedup (no-double-count)
// edge, and the "cancel frees a slot" release path.

func newCapFixture(t *testing.T, cap uint32, lookup SymbolLookup) (*Service, *engine.ShardState) {
	t.Helper()
	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &mockPublisher{}
	txn := &mockTxnPublisher{}
	svc := New(Config{
		ShardID:                   0,
		ProducerID:                "counter-shard-0-main",
		DefaultMaxOpenLimitOrders: cap,
	}, state, seq, dt, pub, zap.NewNop())
	svc.SetOrderDeps(txn, &intIDGen{})
	if lookup != nil {
		svc.SetSymbolLookup(lookup)
	}
	_, _ = svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "seed-u1", UserID: "u1", Asset: "USDT",
		Amount: dec.New("1000000"), Type: engine.TransferDeposit,
	})
	return svc, state
}

func placeLimit(t *testing.T, svc *Service, user, symbol, coid string) *PlaceOrderResult {
	t.Helper()
	res, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: user, ClientOrderID: coid, Symbol: symbol,
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("1"), Qty: dec.New("1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestPlaceOrder_DefaultCapEnforced(t *testing.T) {
	svc, _ := newCapFixture(t, 2, nil)
	if r := placeLimit(t, svc, "u1", "BTC-USDT", "c1"); !r.Accepted {
		t.Fatalf("place 1: %+v", r)
	}
	if r := placeLimit(t, svc, "u1", "BTC-USDT", "c2"); !r.Accepted {
		t.Fatalf("place 2: %+v", r)
	}
	r3 := placeLimit(t, svc, "u1", "BTC-USDT", "c3")
	if r3.Accepted {
		t.Fatalf("place 3 must be rejected: %+v", r3)
	}
	if r3.RejectReason != string(etcdcfg.RejectMaxOpenLimitOrders) {
		t.Errorf("reject reason = %q, want %q", r3.RejectReason, etcdcfg.RejectMaxOpenLimitOrders)
	}
}

func TestPlaceOrder_CancelFreesSlot(t *testing.T) {
	svc, _ := newCapFixture(t, 1, nil)
	r := placeLimit(t, svc, "u1", "BTC-USDT", "c1")
	if !r.Accepted {
		t.Fatalf("place 1: %+v", r)
	}
	// Second attempt at cap blocks.
	if r := placeLimit(t, svc, "u1", "BTC-USDT", "c2"); r.Accepted {
		t.Fatalf("place 2 should block: %+v", r)
	}
	// Cancel drives the slot back to 0 via UpdateStatus(PENDING_CANCEL →
	// CANCELED on Match ack); we simulate that here by driving the store
	// directly, since CancelOrder only reaches PENDING_CANCEL which is
	// non-terminal.
	if _, err := svc.state.Orders().UpdateStatus(r.OrderID, engine.OrderStatusCanceled, 0); err != nil {
		t.Fatal(err)
	}
	// Third attempt should now pass.
	if r := placeLimit(t, svc, "u1", "BTC-USDT", "c3"); !r.Accepted {
		t.Fatalf("place 3 after cancel: %+v", r)
	}
}

func TestPlaceOrder_SymbolConfigOverridesDefault(t *testing.T) {
	// Default is generous, but symbol config tightens to 1.
	lookup := func(sym string) (etcdcfg.SymbolConfig, bool) {
		return etcdcfg.SymbolConfig{MaxOpenLimitOrders: 1}, true
	}
	svc, _ := newCapFixture(t, 100, lookup)
	if r := placeLimit(t, svc, "u1", "BTC-USDT", "c1"); !r.Accepted {
		t.Fatalf("place 1: %+v", r)
	}
	if r := placeLimit(t, svc, "u1", "BTC-USDT", "c2"); r.Accepted {
		t.Fatalf("place 2 must be blocked by symbol override: %+v", r)
	}
}

func TestPlaceOrder_CapZeroDisablesCheck(t *testing.T) {
	// cap == 0 in both service default and symbol config: no enforcement.
	svc, _ := newCapFixture(t, 0, nil)
	for i := 0; i < 5; i++ {
		coid := "c-" + string(rune('0'+i))
		if r := placeLimit(t, svc, "u1", "BTC-USDT", coid); !r.Accepted {
			t.Fatalf("place %d: %+v", i, r)
		}
	}
}

func TestPlaceOrder_DedupDoesNotDoubleCount(t *testing.T) {
	// cap=1, issue the same client_order_id twice: the second call hits
	// dedup and must not produce a rejection (it returns the existing
	// order with Accepted=false).
	svc, _ := newCapFixture(t, 1, nil)
	r1 := placeLimit(t, svc, "u1", "BTC-USDT", "dup")
	if !r1.Accepted {
		t.Fatalf("place 1: %+v", r1)
	}
	r2 := placeLimit(t, svc, "u1", "BTC-USDT", "dup")
	if r2.Accepted {
		t.Errorf("dedup should return accepted=false on repeat: %+v", r2)
	}
	if r2.RejectReason != "" {
		t.Errorf("dedup should not emit a reject reason, got %q", r2.RejectReason)
	}
	if r2.OrderID != r1.OrderID {
		t.Errorf("dedup should echo original order id, got %d want %d", r2.OrderID, r1.OrderID)
	}
}

func TestPlaceOrder_MarketOrdersDoNotConsumeSlot(t *testing.T) {
	svc, _ := newCapFixture(t, 1, nil)
	// Fill the one LIMIT slot.
	if r := placeLimit(t, svc, "u1", "BTC-USDT", "l1"); !r.Accepted {
		t.Fatalf("place limit: %+v", r)
	}
	// MARKET sell from u2 (not u1, but the point is MARKETs of any user
	// must not touch LIMIT accounting). Use u2 with BTC balance seeded
	// below.
	_, _ = svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "seed-u2", UserID: "u2", Asset: "BTC",
		Amount: dec.New("10"), Type: engine.TransferDeposit,
	})
	_, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u2", Symbol: "BTC-USDT",
		Side: engine.SideAsk, OrderType: engine.OrderTypeMarket, TIF: engine.TIFGTC,
		Qty: dec.New("1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// MARKET didn't count against u2's slots, MARKET-sell orders never
	// consume a LIMIT slot even if the user would later want one.
	if got := svc.state.Orders().CountActiveLimits("u2", "BTC-USDT"); got != 0 {
		t.Errorf("MARKET leaked into LIMIT counter: %d", got)
	}
}
