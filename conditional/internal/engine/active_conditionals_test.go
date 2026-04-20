package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// ADR-0054: Engine.activeConditionals caps pending conditionals per
// (user, symbol). Tests below cover default cap, SymbolLookup override,
// release on cancel, restore rebuild, and the derived LIMIT trigger
// landing in EXPIRED_IN_MATCH when Counter reports its own cap full.

func newEngineWithCap(cap uint32, lookup SymbolLookup, placer OrderPlacer) *Engine {
	return New(Config{
		TerminalHistoryLimit:              100,
		Clock:                             func() time.Time { return time.Unix(1_700_000_000, 0) },
		DefaultMaxActiveConditionalOrders: cap,
		SymbolLookup:                      lookup,
	}, &counterSeq{}, placer, nil, zap.NewNop())
}

func TestConditionalCap_DefaultEnforced(t *testing.T) {
	e := newEngineWithCap(2, nil, &fakePlacer{})
	for i := 0; i < 2; i++ {
		if _, _, ok, err := e.Place(context.Background(), goodReq()); err != nil || !ok {
			t.Fatalf("place %d: err=%v ok=%v", i, err, ok)
		}
	}
	_, _, ok, err := e.Place(context.Background(), goodReq())
	if ok {
		t.Fatal("expected rejection at cap")
	}
	if !errors.Is(err, ErrMaxActiveConditionalOrdersExceeded) {
		t.Errorf("err = %v, want ErrMaxActiveConditionalOrdersExceeded", err)
	}
	if got := e.CountActiveConditionals("u1", "BTC-USDT"); got != 2 {
		t.Errorf("count = %d, want 2", got)
	}
}

func TestConditionalCap_CancelFreesSlot(t *testing.T) {
	e := newEngineWithCap(1, nil, &fakePlacer{})
	id, _, _, err := e.Place(context.Background(), goodReq())
	if err != nil {
		t.Fatal(err)
	}
	// Second place blocks.
	if _, _, ok, err := e.Place(context.Background(), goodReq()); ok || err == nil {
		t.Fatalf("second place should block: ok=%v err=%v", ok, err)
	}
	// Cancel the first → slot released (graduateLocked drops the counter).
	if _, _, err := e.Cancel(context.Background(), "u1", id); err != nil {
		t.Fatal(err)
	}
	if got := e.CountActiveConditionals("u1", "BTC-USDT"); got != 0 {
		t.Errorf("after cancel count = %d, want 0", got)
	}
	if _, _, ok, err := e.Place(context.Background(), goodReq()); !ok {
		t.Fatalf("place after cancel: ok=%v err=%v", ok, err)
	}
}

func TestConditionalCap_SymbolOverrideTightens(t *testing.T) {
	lookup := SymbolLookup(func(sym string) (etcdcfg.SymbolConfig, bool) {
		if sym == "BTC-USDT" {
			return etcdcfg.SymbolConfig{MaxActiveConditionalOrders: 1}, true
		}
		return etcdcfg.SymbolConfig{}, false
	})
	e := newEngineWithCap(100, lookup, &fakePlacer{})
	if _, _, ok, _ := e.Place(context.Background(), goodReq()); !ok {
		t.Fatal("first place rejected")
	}
	if _, _, ok, err := e.Place(context.Background(), goodReq()); ok || !errors.Is(err, ErrMaxActiveConditionalOrdersExceeded) {
		t.Fatalf("symbol override cap should block: ok=%v err=%v", ok, err)
	}
}

func TestConditionalCap_ZeroDisablesCheck(t *testing.T) {
	e := newEngineWithCap(0, nil, &fakePlacer{})
	for i := 0; i < 20; i++ {
		req := goodReq()
		if _, _, ok, err := e.Place(context.Background(), req); !ok {
			t.Fatalf("place %d: err=%v", i, err)
		}
	}
	if got := e.CountActiveConditionals("u1", "BTC-USDT"); got != 20 {
		t.Errorf("cap=0 should allow unlimited, got %d", got)
	}
}

func TestConditionalCap_RestoreRebuildsIndex(t *testing.T) {
	e := newEngineWithCap(0, nil, &fakePlacer{})
	pending := []*Conditional{
		{ID: 1, UserID: "u1", Symbol: "BTC-USDT", Status: condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING},
		{ID: 2, UserID: "u1", Symbol: "BTC-USDT", Status: condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING},
		{ID: 3, UserID: "u1", Symbol: "ETH-USDT", Status: condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING},
		{ID: 4, UserID: "u2", Symbol: "BTC-USDT", Status: condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING},
	}
	e.Restore(pending, nil, nil)
	if got := e.CountActiveConditionals("u1", "BTC-USDT"); got != 2 {
		t.Errorf("u1 BTC = %d, want 2", got)
	}
	if got := e.CountActiveConditionals("u1", "ETH-USDT"); got != 1 {
		t.Errorf("u1 ETH = %d, want 1", got)
	}
	if got := e.CountActiveConditionals("u2", "BTC-USDT"); got != 1 {
		t.Errorf("u2 BTC = %d, want 1", got)
	}
}

// TestTryFire_CounterMaxOpenLimitOrders_RoutesToExpiredInMatch exercises
// the M5 plumbing: Counter signals RejectMaxOpenLimitOrders via a
// FailedPrecondition gRPC status; the engine must land the conditional
// in CONDITIONAL_STATUS_EXPIRED_IN_MATCH (not the generic REJECTED).
func TestTryFire_CounterMaxOpenLimitOrders_RoutesToExpiredInMatch(t *testing.T) {
	placer := &fakePlacer{respFn: func(_ *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, string(etcdcfg.RejectMaxOpenLimitOrders))
	}}
	e := newEngineWithCap(0, nil, placer)
	id, _, _, err := e.Place(context.Background(), goodReq())
	if err != nil {
		t.Fatal(err)
	}
	// Drive a price update past the stop (stop_price=100, side=SELL →
	// triggers when price <= 100).
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "99"), 0, 0)

	got, err := e.Get("u1", id)
	if err != nil {
		t.Fatalf("Get after trigger: %v", err)
	}
	if got.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_EXPIRED_IN_MATCH {
		t.Errorf("status = %v, want EXPIRED_IN_MATCH", got.Status)
	}
	if got.RejectReason != string(etcdcfg.RejectMaxOpenLimitOrders) {
		t.Errorf("reject reason = %q", got.RejectReason)
	}
}

// TestTryFire_GenericCounterReject_StaysAsRejected keeps the historical
// behaviour for errors other than the ADR-0054 cap.
func TestTryFire_GenericCounterReject_StaysAsRejected(t *testing.T) {
	placer := &fakePlacer{respFn: func(_ *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "insufficient_balance")
	}}
	e := newEngineWithCap(0, nil, placer)
	id, _, _, err := e.Place(context.Background(), goodReq())
	if err != nil {
		t.Fatal(err)
	}
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "99"), 0, 0)
	got, err := e.Get("u1", id)
	if err != nil {
		t.Fatalf("Get after trigger: %v", err)
	}
	if got.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_REJECTED {
		t.Errorf("status = %v, want REJECTED", got.Status)
	}
}
