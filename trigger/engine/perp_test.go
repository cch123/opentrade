package engine

// perp_test.go covers the ADR-0078 §6 perp position-bound trigger path:
// placement validation, the mark-price (perp-price) fire basis, the inner
// reduce_only order wire, EXPIRED_POSITION_GONE mapping, COID dedup-hit
// convergence, the no-spot-reservation rule, and the perp offset map.

import (
	"context"
	"sync"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
)

type fakePerpPlacer struct {
	mu     sync.Mutex
	seen   []*perprpc.PlaceOrderRequest
	respFn func(*perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error)
}

func (p *fakePerpPlacer) PlaceOrder(_ context.Context, req *perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error) {
	p.mu.Lock()
	p.seen = append(p.seen, req)
	p.mu.Unlock()
	if p.respFn != nil {
		return p.respFn(req)
	}
	return &perprpc.PlaceOrderResponse{OrderId: 4242, Accepted: true}, nil
}

func (p *fakePerpPlacer) calls() []*perprpc.PlaceOrderRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*perprpc.PlaceOrderRequest, len(p.seen))
	copy(out, p.seen)
	return out
}

const perpSym = "BTC-USDT-PERP"

func perpTriggerReq(user uint64, side eventpb.Side, typ condrpc.TriggerType, stop, qty string) *condrpc.PlaceTriggerRequest {
	return &condrpc.PlaceTriggerRequest{
		UserId: user, Symbol: perpSym, Side: side, Type: typ,
		StopPrice: stop, Qty: qty,
		Perp: true, PositionIdx: 0, SlippageBps: 50,
	}
}

func markTick(symbol, mark string) *eventpb.PerpPriceEvent {
	return &eventpb.PerpPriceEvent{
		Symbol:  symbol,
		Payload: &eventpb.PerpPriceEvent_Tick{Tick: &eventpb.MarkTick{MarkPrice: mark}},
	}
}

func newPerpEngine(spot OrderPlacer, perp PerpOrderPlacer) *Engine {
	e := newEngine(spot)
	if perp != nil {
		e.SetPerpPlacer(perp)
	}
	return e
}

func TestPerpTrigger_PlaceRejectsWithoutPerpPlacer(t *testing.T) {
	e := newEngine(&fakePlacer{}) // no perp placer wired
	_, _, _, err := e.Place(context.Background(), perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "1"))
	if err != ErrPerpNotEnabled {
		t.Fatalf("want ErrPerpNotEnabled, got %v", err)
	}
}

func TestPerpTrigger_ShapeValidation(t *testing.T) {
	e := newPerpEngine(&fakePlacer{}, &fakePerpPlacer{})
	ctx := context.Background()

	// Perp fields on a non-perp trigger.
	req := perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "1")
	req.Perp = false
	if _, _, _, err := e.Place(ctx, req); err != ErrPerpFieldsForbidden {
		t.Fatalf("perp fields without perp=true: %v", err)
	}
	// quote_qty forbidden for perp.
	req = perpTriggerReq(7, eventpb.Side_SIDE_BUY, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "110", "")
	req.QuoteQty = "100"
	req.SlippageBps = 0
	if _, _, _, err := e.Place(ctx, req); err != ErrPerpQuoteQtyForbidden {
		t.Fatalf("perp quote_qty: %v", err)
	}
	// position_idx range.
	req = perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "1")
	req.PositionIdx = 3
	if _, _, _, err := e.Place(ctx, req); err != ErrPerpPositionIdxRange {
		t.Fatalf("position_idx range: %v", err)
	}
	// slippage on a LIMIT variant.
	req = perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT, "90", "1")
	req.LimitPrice = "89"
	if _, _, _, err := e.Place(ctx, req); err != ErrPerpSlippageShape {
		t.Fatalf("slippage on limit variant: %v", err)
	}
}

func TestPerpTrigger_FiresOnMarkPriceIntoPerpCounter(t *testing.T) {
	spot := &fakePlacer{}
	perp := &fakePerpPlacer{}
	e := newPerpEngine(spot, perp)
	ctx := context.Background()

	id, _, accepted, err := e.Place(ctx, perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "2"))
	if err != nil || !accepted {
		t.Fatalf("place: %v accepted=%v", err, accepted)
	}
	// A spot PublicTrade for some other symbol does nothing; the perp
	// MarkTick at 89 crosses the stop.
	e.HandlePerpPriceRecord(ctx, markTick(perpSym, "95"), 0, 1)
	if got := e.CountActiveTriggers(7, perpSym); got != 1 {
		t.Fatalf("not crossed yet, active=%d", got)
	}
	e.HandlePerpPriceRecord(ctx, markTick(perpSym, "89"), 0, 2)

	calls := perp.calls()
	if len(calls) != 1 {
		t.Fatalf("perp placer calls: %d", len(calls))
	}
	inner := calls[0]
	if !inner.GetReduceOnly() || inner.GetSymbol() != perpSym ||
		inner.GetOrderType() != eventpb.OrderType_ORDER_TYPE_MARKET ||
		inner.GetSlippageBps() != 50 || inner.GetQty() != "2" {
		t.Fatalf("inner order wire: %+v", inner)
	}
	if inner.GetClientOrderId() != "trig-"+formatUint(id) {
		t.Fatalf("inner COID: %s", inner.GetClientOrderId())
	}
	if len(spot.calls()) != 0 {
		t.Fatal("perp trigger must never reach the spot placer")
	}
	q, err := e.Get(7, id)
	if err != nil || q.Status != condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED || q.PlacedOrderID != 4242 {
		t.Fatalf("post-fire: %+v err=%v", q, err)
	}
	// Offsets advanced for the perp feed.
	if got := e.PerpPriceOffsets()[0]; got != 3 {
		t.Fatalf("perp offset: %d", got)
	}
	if got := e.CountActiveTriggers(7, perpSym); got != 0 {
		t.Fatalf("active after fire: %d", got)
	}
}

func TestPerpTrigger_PositionGoneMapsToExpired(t *testing.T) {
	perp := &fakePerpPlacer{respFn: func(*perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error) {
		return &perprpc.PlaceOrderResponse{Accepted: false, RejectReason: "reduce_only_requires_opposite_position"}, nil
	}}
	e := newPerpEngine(&fakePlacer{}, perp)
	ctx := context.Background()
	id, _, _, err := e.Place(ctx, perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "1"))
	if err != nil {
		t.Fatal(err)
	}
	e.HandlePerpPriceRecord(ctx, markTick(perpSym, "89"), 0, 1)
	q, _ := e.Get(7, id)
	if q.Status != condrpc.TriggerStatus_TRIGGER_STATUS_EXPIRED_POSITION_GONE {
		t.Fatalf("want EXPIRED_POSITION_GONE, got %v (%s)", q.Status, q.RejectReason)
	}
}

func TestPerpTrigger_BusinessRejectStaysRejected(t *testing.T) {
	perp := &fakePerpPlacer{respFn: func(*perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error) {
		return &perprpc.PlaceOrderResponse{Accepted: false, RejectReason: "close_all_in_progress"}, nil
	}}
	e := newPerpEngine(&fakePlacer{}, perp)
	ctx := context.Background()
	id, _, _, _ := e.Place(ctx, perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "1"))
	e.HandlePerpPriceRecord(ctx, markTick(perpSym, "89"), 0, 1)
	q, _ := e.Get(7, id)
	if q.Status != condrpc.TriggerStatus_TRIGGER_STATUS_REJECTED || q.RejectReason != "close_all_in_progress" {
		t.Fatalf("want REJECTED(close_all_in_progress), got %v (%s)", q.Status, q.RejectReason)
	}
}

func TestPerpTrigger_DedupHitConvergesToTriggered(t *testing.T) {
	// Crash-replay shape: perp-counter's COID dedup returns accepted=false
	// with the ORIGINAL order id and no reason — the fire must commit
	// TRIGGERED with that id (ADR-0078 修订 #3).
	perp := &fakePerpPlacer{respFn: func(*perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error) {
		return &perprpc.PlaceOrderResponse{Accepted: false, OrderId: 777}, nil
	}}
	e := newPerpEngine(&fakePlacer{}, perp)
	ctx := context.Background()
	id, _, _, _ := e.Place(ctx, perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "1"))
	e.HandlePerpPriceRecord(ctx, markTick(perpSym, "89"), 0, 1)
	q, _ := e.Get(7, id)
	if q.Status != condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED || q.PlacedOrderID != 777 {
		t.Fatalf("dedup-hit convergence: %+v", q)
	}
}

func TestPerpTrigger_NoSpotReservationTaken(t *testing.T) {
	res := &fakeReserver{}
	e := newEngineWithReserver(&fakePlacer{}, res)
	e.SetPerpPlacer(&fakePerpPlacer{})
	ctx := context.Background()
	if _, _, _, err := e.Place(ctx, perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "1")); err != nil {
		t.Fatal(err)
	}
	res.mu.Lock()
	n := len(res.reserves)
	res.mu.Unlock()
	if n != 0 {
		t.Fatalf("perp trigger must not touch the spot reservation ledger: %d", n)
	}
}

func TestPerpTrigger_FundingTickAdvancesOffsetOnly(t *testing.T) {
	perp := &fakePerpPlacer{}
	e := newPerpEngine(&fakePlacer{}, perp)
	ctx := context.Background()
	e.Place(ctx, perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "1"))
	e.HandlePerpPriceRecord(ctx, &eventpb.PerpPriceEvent{
		Symbol:  perpSym,
		Payload: &eventpb.PerpPriceEvent_Funding{Funding: &eventpb.FundingTick{FundingRate: "0.0001", MarkPrice: "10"}},
	}, 2, 10)
	if len(perp.calls()) != 0 {
		t.Fatal("FundingTick must not fire triggers")
	}
	if got := e.PerpPriceOffsets()[2]; got != 11 {
		t.Fatalf("funding tick must still advance the offset: %d", got)
	}
}

func TestPerpTrigger_TrailingStopOnMarkPrice(t *testing.T) {
	perp := &fakePerpPlacer{}
	e := newPerpEngine(&fakePlacer{}, perp)
	ctx := context.Background()
	req := &condrpc.PlaceTriggerRequest{
		UserId: 7, Symbol: perpSym, Side: eventpb.Side_SIDE_SELL,
		Type: condrpc.TriggerType_TRIGGER_TYPE_TRAILING_STOP_LOSS,
		Qty:  "1", TrailingDeltaBps: 100, // 1%
		Perp: true,
	}
	if _, _, _, err := e.Place(ctx, req); err != nil {
		t.Fatal(err)
	}
	e.HandlePerpPriceRecord(ctx, markTick(perpSym, "100"), 0, 1) // watermark 100
	e.HandlePerpPriceRecord(ctx, markTick(perpSym, "105"), 0, 2) // watermark 105
	if len(perp.calls()) != 0 {
		t.Fatal("no retracement yet")
	}
	e.HandlePerpPriceRecord(ctx, markTick(perpSym, "103.9"), 0, 3) // 105 - 1.05 = 103.95 crossed
	if len(perp.calls()) != 1 {
		t.Fatalf("trailing stop must fire on mark retracement: %d", len(perp.calls()))
	}
}

func TestPerpTrigger_OCOPairSharesLeg(t *testing.T) {
	perp := &fakePerpPlacer{}
	e := newPerpEngine(&fakePlacer{}, perp)
	ctx := context.Background()
	tp := perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "120", "1")
	sl := perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "1")
	_, results, accepted, err := e.PlaceOCO(ctx, 7, "oco-1", []*condrpc.PlaceTriggerRequest{tp, sl})
	if err != nil || !accepted || len(results) != 2 {
		t.Fatalf("perp OCO: err=%v accepted=%v", err, accepted)
	}
	// Mixed spot/perp group is rejected.
	spotLeg := &condrpc.PlaceTriggerRequest{
		UserId: 7, Symbol: perpSym, Side: eventpb.Side_SIDE_SELL,
		Type: condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, StopPrice: "90", Qty: "1",
	}
	if _, _, _, err := e.PlaceOCO(ctx, 7, "oco-2", []*condrpc.PlaceTriggerRequest{perpTriggerReq(7, eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "120", "1"), spotLeg}); err != ErrOCOPerpMismatch {
		t.Fatalf("mixed OCO group: %v", err)
	}
	// SL fires → TP sibling auto-cancels.
	e.HandlePerpPriceRecord(ctx, markTick(perpSym, "89"), 0, 1)
	if len(perp.calls()) != 1 {
		t.Fatalf("one leg fires: %d", len(perp.calls()))
	}
	qTP, _ := e.Get(7, results[0].ID)
	if qTP.Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Fatalf("TP sibling must cascade-cancel: %v", qTP.Status)
	}
}
