package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/dec"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

type counterSeq struct {
	next atomic.Uint64
}

func (c *counterSeq) Next() uint64 { return c.next.Add(1) }

type fakePlacer struct {
	mu   sync.Mutex
	seen []*counterrpc.PlaceOrderRequest
	// respFn optionally returns a canned response/error pair.
	respFn func(*counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error)
}

func (p *fakePlacer) PlaceOrder(_ context.Context, req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
	p.mu.Lock()
	p.seen = append(p.seen, req)
	p.mu.Unlock()
	if p.respFn != nil {
		return p.respFn(req)
	}
	return &counterrpc.PlaceOrderResponse{OrderId: 42, Accepted: true}, nil
}

func (p *fakePlacer) calls() []*counterrpc.PlaceOrderRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*counterrpc.PlaceOrderRequest, len(p.seen))
	copy(out, p.seen)
	return out
}

func newEngine(placer OrderPlacer) *Engine {
	idg := &counterSeq{}
	return New(Config{TerminalHistoryLimit: 100, Clock: func() time.Time { return time.Unix(1_700_000_000, 0) }}, idg, placer, zap.NewNop())
}

// ---------------------------------------------------------------------------
// Trigger rule matrix
// ---------------------------------------------------------------------------

func TestShouldFire_Matrix(t *testing.T) {
	stop := dec.New("100")
	type row struct {
		side   eventpb.Side
		typ    condrpc.ConditionalType
		price  string
		expect bool
	}
	cases := []row{
		// sell + stop_loss: fires when price falls to/below stop
		{eventpb.Side_SIDE_SELL, condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS, "101", false},
		{eventpb.Side_SIDE_SELL, condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS, "100", true},
		{eventpb.Side_SIDE_SELL, condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS, "99", true},
		// sell + take_profit: fires when price rises to/above stop
		{eventpb.Side_SIDE_SELL, condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT, "99", false},
		{eventpb.Side_SIDE_SELL, condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT, "100", true},
		{eventpb.Side_SIDE_SELL, condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT, "101", true},
		// buy + stop_loss (break-in): fires when price rises to/above stop
		{eventpb.Side_SIDE_BUY, condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT, "99", false},
		{eventpb.Side_SIDE_BUY, condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT, "100", true},
		{eventpb.Side_SIDE_BUY, condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT, "101", true},
		// buy + take_profit: fires when price falls to/below stop
		{eventpb.Side_SIDE_BUY, condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT, "101", false},
		{eventpb.Side_SIDE_BUY, condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT, "100", true},
		{eventpb.Side_SIDE_BUY, condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT, "99", true},
	}
	for _, c := range cases {
		got := ShouldFire(c.side, c.typ, dec.New(c.price), stop)
		if got != c.expect {
			t.Errorf("%v/%v at %s: got %v want %v", c.side, c.typ, c.price, got, c.expect)
		}
	}
}

func TestShouldFire_UnknownTypeIsFalse(t *testing.T) {
	if ShouldFire(eventpb.Side_SIDE_BUY, condrpc.ConditionalType_CONDITIONAL_TYPE_UNSPECIFIED, dec.New("100"), dec.New("100")) {
		t.Error("unspecified type must not fire")
	}
}

// ---------------------------------------------------------------------------
// Validation / placement
// ---------------------------------------------------------------------------

func goodReq() *condrpc.PlaceConditionalRequest {
	return &condrpc.PlaceConditionalRequest{
		UserId:    "u1",
		Symbol:    "BTC-USDT",
		Side:      eventpb.Side_SIDE_SELL,
		Type:      condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS,
		StopPrice: "100",
		Qty:       "0.5",
	}
}

func TestPlace_HappyPath(t *testing.T) {
	e := newEngine(&fakePlacer{})
	id, status, accepted, err := e.Place(goodReq())
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if id == 0 {
		t.Errorf("id = 0")
	}
	if status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
		t.Errorf("status = %v", status)
	}
	if !accepted {
		t.Errorf("accepted = false")
	}
}

func TestPlace_DedupByClientID(t *testing.T) {
	e := newEngine(&fakePlacer{})
	req := goodReq()
	req.ClientConditionalId = "my-cond-1"
	id1, _, ok1, err := e.Place(req)
	if err != nil || !ok1 {
		t.Fatalf("first place: err=%v ok=%v", err, ok1)
	}
	id2, _, ok2, err := e.Place(req)
	if err != nil {
		t.Fatalf("second place: %v", err)
	}
	if ok2 {
		t.Errorf("expected accepted=false for dup")
	}
	if id2 != id1 {
		t.Errorf("dup returned different id: %d vs %d", id2, id1)
	}
}

func TestPlace_ValidationErrors(t *testing.T) {
	e := newEngine(&fakePlacer{})
	cases := map[string]func(*condrpc.PlaceConditionalRequest){
		"no user":      func(r *condrpc.PlaceConditionalRequest) { r.UserId = "" },
		"no symbol":    func(r *condrpc.PlaceConditionalRequest) { r.Symbol = "" },
		"bad side":     func(r *condrpc.PlaceConditionalRequest) { r.Side = eventpb.Side_SIDE_UNSPECIFIED },
		"bad type":     func(r *condrpc.PlaceConditionalRequest) { r.Type = condrpc.ConditionalType_CONDITIONAL_TYPE_UNSPECIFIED },
		"zero stop":    func(r *condrpc.PlaceConditionalRequest) { r.StopPrice = "0" },
		"no qty":       func(r *condrpc.PlaceConditionalRequest) { r.Qty = "" },
		"limit wants limit price": func(r *condrpc.PlaceConditionalRequest) {
			r.Type = condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT
			r.LimitPrice = ""
		},
		"market forbids limit price": func(r *condrpc.PlaceConditionalRequest) { r.LimitPrice = "105" },
		"market buy needs quote_qty": func(r *condrpc.PlaceConditionalRequest) {
			r.Side = eventpb.Side_SIDE_BUY
			r.Qty = "1"
			r.QuoteQty = ""
		},
		"market buy rejects both qty and quote_qty": func(r *condrpc.PlaceConditionalRequest) {
			r.Side = eventpb.Side_SIDE_BUY
			r.Qty = "1"
			r.QuoteQty = "100"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := goodReq()
			mutate(req)
			if _, _, _, err := e.Place(req); err == nil {
				t.Error("expected err")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

func TestCancel_TransitionsPendingToCanceled(t *testing.T) {
	e := newEngine(&fakePlacer{})
	id, _, _, _ := e.Place(goodReq())
	status, ok, err := e.Cancel("u1", id)
	if err != nil || !ok {
		t.Fatalf("cancel: err=%v ok=%v", err, ok)
	}
	if status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED {
		t.Errorf("status = %v", status)
	}
}

func TestCancel_NonOwnerRejected(t *testing.T) {
	e := newEngine(&fakePlacer{})
	id, _, _, _ := e.Place(goodReq())
	if _, _, err := e.Cancel("someone-else", id); !errors.Is(err, ErrNotOwner) {
		t.Errorf("err = %v, want ErrNotOwner", err)
	}
}

func TestCancel_Unknown(t *testing.T) {
	e := newEngine(&fakePlacer{})
	if _, _, err := e.Cancel("u1", 99999); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestCancel_IdempotentOnTerminal(t *testing.T) {
	e := newEngine(&fakePlacer{})
	id, _, _, _ := e.Place(goodReq())
	_, _, _ = e.Cancel("u1", id)
	status, ok, err := e.Cancel("u1", id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Error("second cancel must report accepted=false")
	}
	if status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED {
		t.Errorf("status drift: %v", status)
	}
}

// ---------------------------------------------------------------------------
// Trigger / fire
// ---------------------------------------------------------------------------

// publicTradeEvent is a tiny builder for market-data events of only the
// shape the conditional engine reads.
func publicTradeEvent(symbol, price string) *eventpb.MarketDataEvent {
	return &eventpb.MarketDataEvent{
		Symbol: symbol,
		Payload: &eventpb.MarketDataEvent_PublicTrade{PublicTrade: &eventpb.PublicTrade{
			Symbol: symbol,
			Price:  price,
		}},
	}
}

func TestHandleRecord_SellStopLossFires(t *testing.T) {
	placer := &fakePlacer{}
	e := newEngine(placer)
	id, _, _, _ := e.Place(goodReq()) // sell stop_loss @ 100, qty 0.5

	// Price 101 → no fire
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "101"), 0, 10)
	if calls := placer.calls(); len(calls) != 0 {
		t.Fatalf("fired too early: %d calls", len(calls))
	}
	// Price 99 → fire
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "99"), 0, 11)
	calls := placer.calls()
	if len(calls) != 1 {
		t.Fatalf("fire count = %d, want 1", len(calls))
	}
	if calls[0].OrderType != eventpb.OrderType_ORDER_TYPE_MARKET {
		t.Errorf("inner order type: %v", calls[0].OrderType)
	}
	if calls[0].Qty != "0.5" {
		t.Errorf("inner qty: %q", calls[0].Qty)
	}
	if calls[0].ClientOrderId == "" || calls[0].ClientOrderId[:5] != "cond-" {
		t.Errorf("client_order_id: %q", calls[0].ClientOrderId)
	}
	// State transitioned to TRIGGERED
	got, err := e.Get("u1", id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_TRIGGERED {
		t.Errorf("status: %v", got.Status)
	}
	if got.PlacedOrderID != 42 {
		t.Errorf("placed order id: %d", got.PlacedOrderID)
	}
}

func TestHandleRecord_OnlyRelevantSymbolFires(t *testing.T) {
	placer := &fakePlacer{}
	e := newEngine(placer)
	_, _, _, _ = e.Place(goodReq()) // BTC-USDT @ 100
	e.HandleRecord(context.Background(), publicTradeEvent("ETH-USDT", "10"), 0, 1)
	if calls := placer.calls(); len(calls) != 0 {
		t.Errorf("unrelated symbol fired: %d", len(calls))
	}
}

func TestHandleRecord_RejectionCapturedAsStatus(t *testing.T) {
	placer := &fakePlacer{
		respFn: func(*counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			return nil, status.Error(codes.FailedPrecondition, "insufficient balance")
		},
	}
	e := newEngine(placer)
	id, _, _, _ := e.Place(goodReq())
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "99"), 0, 1)
	got, _ := e.Get("u1", id)
	if got.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_REJECTED {
		t.Errorf("status: %v", got.Status)
	}
	if got.RejectReason == "" {
		t.Error("reject_reason should be set")
	}
}

func TestHandleRecord_OffsetsAdvance(t *testing.T) {
	e := newEngine(&fakePlacer{})
	_, _, _, _ = e.Place(goodReq())
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "101"), 3, 99)
	got := e.Offsets()
	if got[3] != 100 {
		t.Errorf("offset = %d, want 100 (next-to-consume)", got[3])
	}
}

func TestHandleRecord_NonPublicTradePayloadIgnored(t *testing.T) {
	e := newEngine(&fakePlacer{})
	_, _, _, _ = e.Place(goodReq())
	evt := &eventpb.MarketDataEvent{
		Symbol:  "BTC-USDT",
		Payload: &eventpb.MarketDataEvent_DepthUpdate{DepthUpdate: &eventpb.DepthUpdate{Symbol: "BTC-USDT"}},
	}
	e.HandleRecord(context.Background(), evt, 0, 1)
	// DepthUpdate carries no price → no trigger; but offset still advances.
	if got := e.Offsets()[0]; got != 2 {
		t.Errorf("offset = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// List / Snapshot / Restore
// ---------------------------------------------------------------------------

func TestList_DefaultPendingOnly(t *testing.T) {
	e := newEngine(&fakePlacer{})
	id1, _, _, _ := e.Place(goodReq())
	r2 := goodReq()
	r2.ClientConditionalId = "alt"
	_, _, _, _ = e.Place(r2)
	_, _, _ = e.Cancel("u1", id1)

	active := e.List("u1", false)
	if len(active) != 1 {
		t.Fatalf("active len = %d, want 1", len(active))
	}
	all := e.List("u1", true)
	if len(all) != 2 {
		t.Fatalf("all len = %d, want 2", len(all))
	}
}

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	placer := &fakePlacer{}
	src := newEngine(placer)
	id, _, _, _ := src.Place(goodReq())
	src.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "101"), 0, 5)
	pending, terminals, offsets := src.Snapshot()

	dst := newEngine(placer)
	dst.Restore(pending, terminals, offsets)

	got, err := dst.Get("u1", id)
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
		t.Errorf("status: %v", got.Status)
	}
	if dst.Offsets()[0] != 6 {
		t.Errorf("offset: %d", dst.Offsets()[0])
	}
}
