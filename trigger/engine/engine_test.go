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
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
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
	return New(Config{TerminalHistoryLimit: 100, Clock: func() time.Time { return time.Unix(1_700_000_000, 0) }}, idg, placer, nil, zap.NewNop())
}

// newEngineWithReserver constructs an engine with both a placer and a
// reservations stub — MVP-14b behaviour.
func newEngineWithReserver(placer OrderPlacer, res Reservations) *Engine {
	idg := &counterSeq{}
	return New(Config{TerminalHistoryLimit: 100, Clock: func() time.Time { return time.Unix(1_700_000_000, 0) }}, idg, placer, res, zap.NewNop())
}

// fakeReserver records Reserve / Release calls so tests can inspect the
// sequence without a live Counter gRPC.
type fakeReserver struct {
	mu            sync.Mutex
	reserves      []*counterrpc.ReserveRequest
	releases      []*counterrpc.ReleaseReservationRequest
	reserveErr    error
	reserveFn     func(*counterrpc.ReserveRequest) (*counterrpc.ReserveResponse, error)
	releaseFn     func(*counterrpc.ReleaseReservationRequest) (*counterrpc.ReleaseReservationResponse, error)
}

func (r *fakeReserver) Reserve(_ context.Context, req *counterrpc.ReserveRequest) (*counterrpc.ReserveResponse, error) {
	r.mu.Lock()
	r.reserves = append(r.reserves, req)
	r.mu.Unlock()
	if r.reserveErr != nil {
		return nil, r.reserveErr
	}
	if r.reserveFn != nil {
		return r.reserveFn(req)
	}
	return &counterrpc.ReserveResponse{
		ReservationId: req.ReservationId,
		Asset:         "USDT",
		Amount:        "100",
		Accepted:      true,
	}, nil
}

func (r *fakeReserver) ReleaseReservation(_ context.Context, req *counterrpc.ReleaseReservationRequest) (*counterrpc.ReleaseReservationResponse, error) {
	r.mu.Lock()
	r.releases = append(r.releases, req)
	r.mu.Unlock()
	if r.releaseFn != nil {
		return r.releaseFn(req)
	}
	return &counterrpc.ReleaseReservationResponse{ReservationId: req.ReservationId, Accepted: true}, nil
}

func (r *fakeReserver) reserveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reserves)
}

func (r *fakeReserver) releaseCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.releases)
}

// ---------------------------------------------------------------------------
// Trigger rule matrix
// ---------------------------------------------------------------------------

func TestShouldFire_Matrix(t *testing.T) {
	stop := dec.New("100")
	type row struct {
		side   eventpb.Side
		typ    condrpc.TriggerType
		price  string
		expect bool
	}
	cases := []row{
		// sell + stop_loss: fires when price falls to/below stop
		{eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "101", false},
		{eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "100", true},
		{eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "99", true},
		// sell + take_profit: fires when price rises to/above stop
		{eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "99", false},
		{eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "100", true},
		{eventpb.Side_SIDE_SELL, condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "101", true},
		// buy + stop_loss (break-in): fires when price rises to/above stop
		{eventpb.Side_SIDE_BUY, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT, "99", false},
		{eventpb.Side_SIDE_BUY, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT, "100", true},
		{eventpb.Side_SIDE_BUY, condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT, "101", true},
		// buy + take_profit: fires when price falls to/below stop
		{eventpb.Side_SIDE_BUY, condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT_LIMIT, "101", false},
		{eventpb.Side_SIDE_BUY, condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT_LIMIT, "100", true},
		{eventpb.Side_SIDE_BUY, condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT_LIMIT, "99", true},
	}
	for _, c := range cases {
		got := ShouldFire(c.side, c.typ, dec.New(c.price), stop)
		if got != c.expect {
			t.Errorf("%v/%v at %s: got %v want %v", c.side, c.typ, c.price, got, c.expect)
		}
	}
}

func TestShouldFire_UnknownTypeIsFalse(t *testing.T) {
	if ShouldFire(eventpb.Side_SIDE_BUY, condrpc.TriggerType_TRIGGER_TYPE_UNSPECIFIED, dec.New("100"), dec.New("100")) {
		t.Error("unspecified type must not fire")
	}
}

// ---------------------------------------------------------------------------
// Validation / placement
// ---------------------------------------------------------------------------

func goodReq() *condrpc.PlaceTriggerRequest {
	return &condrpc.PlaceTriggerRequest{
		UserId:    "u1",
		Symbol:    "BTC-USDT",
		Side:      eventpb.Side_SIDE_SELL,
		Type:      condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS,
		StopPrice: "100",
		Qty:       "0.5",
	}
}

func TestPlace_HappyPath(t *testing.T) {
	e := newEngine(&fakePlacer{})
	id, status, accepted, err := e.Place(context.Background(), goodReq())
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if id == 0 {
		t.Errorf("id = 0")
	}
	if status != condrpc.TriggerStatus_TRIGGER_STATUS_PENDING {
		t.Errorf("status = %v", status)
	}
	if !accepted {
		t.Errorf("accepted = false")
	}
}

func TestPlace_DedupByClientID(t *testing.T) {
	e := newEngine(&fakePlacer{})
	req := goodReq()
	req.ClientTriggerId = "my-trig-1"
	id1, _, ok1, err := e.Place(context.Background(), req)
	if err != nil || !ok1 {
		t.Fatalf("first place: err=%v ok=%v", err, ok1)
	}
	id2, _, ok2, err := e.Place(context.Background(), req)
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
	cases := map[string]func(*condrpc.PlaceTriggerRequest){
		"no user":      func(r *condrpc.PlaceTriggerRequest) { r.UserId = "" },
		"no symbol":    func(r *condrpc.PlaceTriggerRequest) { r.Symbol = "" },
		"bad side":     func(r *condrpc.PlaceTriggerRequest) { r.Side = eventpb.Side_SIDE_UNSPECIFIED },
		"bad type":     func(r *condrpc.PlaceTriggerRequest) { r.Type = condrpc.TriggerType_TRIGGER_TYPE_UNSPECIFIED },
		"zero stop":    func(r *condrpc.PlaceTriggerRequest) { r.StopPrice = "0" },
		"no qty":       func(r *condrpc.PlaceTriggerRequest) { r.Qty = "" },
		"limit wants limit price": func(r *condrpc.PlaceTriggerRequest) {
			r.Type = condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT
			r.LimitPrice = ""
		},
		"market forbids limit price": func(r *condrpc.PlaceTriggerRequest) { r.LimitPrice = "105" },
		"market buy needs quote_qty": func(r *condrpc.PlaceTriggerRequest) {
			r.Side = eventpb.Side_SIDE_BUY
			r.Qty = "1"
			r.QuoteQty = ""
		},
		"market buy rejects both qty and quote_qty": func(r *condrpc.PlaceTriggerRequest) {
			r.Side = eventpb.Side_SIDE_BUY
			r.Qty = "1"
			r.QuoteQty = "100"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := goodReq()
			mutate(req)
			if _, _, _, err := e.Place(context.Background(), req); err == nil {
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
	id, _, _, _ := e.Place(context.Background(), goodReq())
	status, ok, err := e.Cancel(context.Background(), "u1", id)
	if err != nil || !ok {
		t.Fatalf("cancel: err=%v ok=%v", err, ok)
	}
	if status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Errorf("status = %v", status)
	}
}

func TestCancel_NonOwnerRejected(t *testing.T) {
	e := newEngine(&fakePlacer{})
	id, _, _, _ := e.Place(context.Background(), goodReq())
	if _, _, err := e.Cancel(context.Background(), "someone-else", id); !errors.Is(err, ErrNotOwner) {
		t.Errorf("err = %v, want ErrNotOwner", err)
	}
}

func TestCancel_Unknown(t *testing.T) {
	e := newEngine(&fakePlacer{})
	if _, _, err := e.Cancel(context.Background(), "u1", 99999); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestCancel_IdempotentOnTerminal(t *testing.T) {
	e := newEngine(&fakePlacer{})
	id, _, _, _ := e.Place(context.Background(), goodReq())
	_, _, _ = e.Cancel(context.Background(), "u1", id)
	status, ok, err := e.Cancel(context.Background(), "u1", id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Error("second cancel must report accepted=false")
	}
	if status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Errorf("status drift: %v", status)
	}
}

// ---------------------------------------------------------------------------
// Trigger / fire
// ---------------------------------------------------------------------------

// publicTradeEvent is a tiny builder for market-data events of only the
// shape the trigger engine reads.
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
	id, _, _, _ := e.Place(context.Background(), goodReq()) // sell stop_loss @ 100, qty 0.5

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
	if calls[0].ClientOrderId == "" || calls[0].ClientOrderId[:5] != "trig-" {
		t.Errorf("client_order_id: %q", calls[0].ClientOrderId)
	}
	// State transitioned to TRIGGERED
	got, err := e.Get("u1", id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED {
		t.Errorf("status: %v", got.Status)
	}
	if got.PlacedOrderID != 42 {
		t.Errorf("placed order id: %d", got.PlacedOrderID)
	}
}

func TestHandleRecord_OnlyRelevantSymbolFires(t *testing.T) {
	placer := &fakePlacer{}
	e := newEngine(placer)
	_, _, _, _ = e.Place(context.Background(), goodReq()) // BTC-USDT @ 100
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
	id, _, _, _ := e.Place(context.Background(), goodReq())
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "99"), 0, 1)
	got, _ := e.Get("u1", id)
	if got.Status != condrpc.TriggerStatus_TRIGGER_STATUS_REJECTED {
		t.Errorf("status: %v", got.Status)
	}
	if got.RejectReason == "" {
		t.Error("reject_reason should be set")
	}
}

func TestHandleRecord_OffsetsAdvance(t *testing.T) {
	e := newEngine(&fakePlacer{})
	_, _, _, _ = e.Place(context.Background(), goodReq())
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "101"), 3, 99)
	got := e.Offsets()
	if got[3] != 100 {
		t.Errorf("offset = %d, want 100 (next-to-consume)", got[3])
	}
}

func TestHandleRecord_NonPublicTradePayloadIgnored(t *testing.T) {
	e := newEngine(&fakePlacer{})
	_, _, _, _ = e.Place(context.Background(), goodReq())
	// OrderBook (Full or Delta) carries no price → no trigger; but offset
	// still advances. ADR-0055 replaced the old DepthUpdate payload with
	// Match-authored OrderBook frames.
	evt := &eventpb.MarketDataEvent{
		Symbol: "BTC-USDT",
		Payload: &eventpb.MarketDataEvent_OrderBook{OrderBook: &eventpb.OrderBook{
			Data: &eventpb.OrderBook_Delta{Delta: &eventpb.OrderBookDelta{}},
		}},
	}
	e.HandleRecord(context.Background(), evt, 0, 1)
	if got := e.Offsets()[0]; got != 2 {
		t.Errorf("offset = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// List / Snapshot / Restore
// ---------------------------------------------------------------------------

func TestList_DefaultPendingOnly(t *testing.T) {
	e := newEngine(&fakePlacer{})
	id1, _, _, _ := e.Place(context.Background(), goodReq())
	r2 := goodReq()
	r2.ClientTriggerId = "alt"
	_, _, _, _ = e.Place(context.Background(), r2)
	_, _, _ = e.Cancel(context.Background(), "u1", id1)

	active := e.List("u1", false)
	if len(active) != 1 {
		t.Fatalf("active len = %d, want 1", len(active))
	}
	all := e.List("u1", true)
	if len(all) != 2 {
		t.Fatalf("all len = %d, want 2", len(all))
	}
}

// ---------------------------------------------------------------------------
// Reservation integration (MVP-14b)
// ---------------------------------------------------------------------------

func TestPlace_CallsReserveFirst(t *testing.T) {
	placer := &fakePlacer{}
	reserver := &fakeReserver{}
	e := newEngineWithReserver(placer, reserver)
	id, _, accepted, err := e.Place(context.Background(), goodReq())
	if err != nil || !accepted {
		t.Fatalf("place: err=%v accepted=%v", err, accepted)
	}
	if reserver.reserveCount() != 1 {
		t.Errorf("reserve count = %d, want 1", reserver.reserveCount())
	}
	got := reserver.reserves[0]
	if got.UserId != "u1" || got.Symbol != "BTC-USDT" {
		t.Errorf("reserve req shape: %+v", got)
	}
	if got.ReservationId != e.refIDFor(id) {
		t.Errorf("ref_id: got %q want %q", got.ReservationId, e.refIDFor(id))
	}
}

func TestPlace_ReserveErrorDoesNotStoreTrigger(t *testing.T) {
	placer := &fakePlacer{}
	reserver := &fakeReserver{reserveErr: errors.New("insufficient")}
	e := newEngineWithReserver(placer, reserver)
	_, _, _, err := e.Place(context.Background(), goodReq())
	if err == nil {
		t.Fatal("expected err")
	}
	if got := e.List("u1", true); len(got) != 0 {
		t.Errorf("trigger stored despite reserve error: %+v", got)
	}
}

func TestCancel_ReleasesReservation(t *testing.T) {
	placer := &fakePlacer{}
	reserver := &fakeReserver{}
	e := newEngineWithReserver(placer, reserver)
	id, _, _, _ := e.Place(context.Background(), goodReq())
	_, ok, err := e.Cancel(context.Background(), "u1", id)
	if err != nil || !ok {
		t.Fatalf("cancel: err=%v ok=%v", err, ok)
	}
	if reserver.releaseCount() != 1 {
		t.Errorf("release count = %d, want 1", reserver.releaseCount())
	}
	if got := reserver.releases[0]; got.ReservationId != e.refIDFor(id) {
		t.Errorf("release ref_id = %q", got.ReservationId)
	}
}

func TestTrigger_UsesReservationID(t *testing.T) {
	placer := &fakePlacer{}
	reserver := &fakeReserver{}
	e := newEngineWithReserver(placer, reserver)
	id, _, _, _ := e.Place(context.Background(), goodReq())
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "99"), 0, 1)
	calls := placer.calls()
	if len(calls) != 1 {
		t.Fatalf("place calls = %d", len(calls))
	}
	if calls[0].ReservationId != e.refIDFor(id) {
		t.Errorf("reservation id not forwarded: got %q", calls[0].ReservationId)
	}
	// Success path: no release call (Counter consumed the reservation).
	if reserver.releaseCount() != 0 {
		t.Errorf("unexpected release on success: %d", reserver.releaseCount())
	}
}

func TestTrigger_RejectionReleasesReservation(t *testing.T) {
	placer := &fakePlacer{
		respFn: func(*counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			return nil, status.Error(codes.Unavailable, "counter down")
		},
	}
	reserver := &fakeReserver{}
	e := newEngineWithReserver(placer, reserver)
	_, _, _, _ = e.Place(context.Background(), goodReq())
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "99"), 0, 1)
	if reserver.releaseCount() != 1 {
		t.Errorf("expected one release on rejection, got %d", reserver.releaseCount())
	}
}

// ---------------------------------------------------------------------------
// Expiry (MVP-14d / ADR-0043)
// ---------------------------------------------------------------------------

// withClock constructs an engine whose clock returns a controllable time.
func withClock(placer OrderPlacer, res Reservations, now *time.Time) *Engine {
	return New(Config{
		TerminalHistoryLimit: 100,
		Clock:                func() time.Time { return *now },
	}, &counterSeq{}, placer, res, zap.NewNop())
}

func TestPlace_ExpiryInPastRejected(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	e := withClock(&fakePlacer{}, nil, &now)
	req := goodReq()
	req.ExpiresAtUnixMs = now.Add(-time.Minute).UnixMilli()
	if _, _, _, err := e.Place(context.Background(), req); !errors.Is(err, ErrExpiryInPast) {
		t.Errorf("err = %v, want ErrExpiryInPast", err)
	}
}

func TestPlace_ExpiryInFutureStored(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	e := withClock(&fakePlacer{}, nil, &now)
	req := goodReq()
	req.ExpiresAtUnixMs = now.Add(10 * time.Minute).UnixMilli()
	id, _, _, err := e.Place(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.Get("u1", id)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExpiresAtMs != req.ExpiresAtUnixMs {
		t.Errorf("expires: got %d want %d", got.ExpiresAtMs, req.ExpiresAtUnixMs)
	}
}

func TestSweepExpired_FlipsOverdue(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	reserver := &fakeReserver{}
	e := withClock(&fakePlacer{}, reserver, &now)

	// One expires at now+1m, another at now+10m, a third with no expiry.
	mk := func(client string, exp time.Duration) uint64 {
		req := goodReq()
		req.ClientTriggerId = client
		if exp > 0 {
			req.ExpiresAtUnixMs = now.Add(exp).UnixMilli()
		}
		id, _, _, err := e.Place(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	soonID := mk("soon", time.Minute)
	lateID := mk("late", 10*time.Minute)
	foreverID := mk("forever", 0)

	// Advance clock past the 'soon' expiry.
	now = now.Add(2 * time.Minute)
	n := e.SweepExpired(context.Background())
	if n != 1 {
		t.Fatalf("sweep flipped %d, want 1", n)
	}

	if got, _ := e.Get("u1", soonID); got.Status != condrpc.TriggerStatus_TRIGGER_STATUS_EXPIRED {
		t.Errorf("soon status = %v", got.Status)
	}
	if got, _ := e.Get("u1", lateID); got.Status != condrpc.TriggerStatus_TRIGGER_STATUS_PENDING {
		t.Errorf("late should still be pending: %v", got.Status)
	}
	if got, _ := e.Get("u1", foreverID); got.Status != condrpc.TriggerStatus_TRIGGER_STATUS_PENDING {
		t.Errorf("forever should still be pending: %v", got.Status)
	}
	// Expired reservation was best-effort released.
	if reserver.releaseCount() != 1 {
		t.Errorf("release count = %d, want 1", reserver.releaseCount())
	}
}

func TestSweepExpired_NoopOnEmpty(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	e := withClock(&fakePlacer{}, nil, &now)
	if n := e.SweepExpired(context.Background()); n != 0 {
		t.Errorf("sweep on empty = %d", n)
	}
}

// ---------------------------------------------------------------------------
// OCO (MVP-14e / ADR-0044)
// ---------------------------------------------------------------------------

func ocoLeg(client string, typ condrpc.TriggerType, stop, limit string) *condrpc.PlaceTriggerRequest {
	return &condrpc.PlaceTriggerRequest{
		UserId:              "u1",
		ClientTriggerId: client,
		Symbol:              "BTC-USDT",
		Side:                eventpb.Side_SIDE_SELL,
		Type:                typ,
		StopPrice:           stop,
		LimitPrice:          limit,
		Qty:                 "0.5",
	}
}

func TestPlaceOCO_HappyPath(t *testing.T) {
	placer := &fakePlacer{}
	reserver := &fakeReserver{}
	e := newEngineWithReserver(placer, reserver)
	gid, legs, accepted, err := e.PlaceOCO(context.Background(), "u1", "oco-client-1", []*condrpc.PlaceTriggerRequest{
		ocoLeg("leg-tp", condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "110", ""),
		ocoLeg("leg-sl", condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", ""),
	})
	if err != nil || !accepted {
		t.Fatalf("place: err=%v accepted=%v", err, accepted)
	}
	if gid == "" {
		t.Error("group id should be non-empty")
	}
	if len(legs) != 2 {
		t.Fatalf("legs = %d", len(legs))
	}
	if reserver.reserveCount() != 2 {
		t.Errorf("reserve called %d times, want 2", reserver.reserveCount())
	}
}

func TestPlaceOCO_DedupByClientOCOID(t *testing.T) {
	placer := &fakePlacer{}
	reserver := &fakeReserver{}
	e := newEngineWithReserver(placer, reserver)
	req := []*condrpc.PlaceTriggerRequest{
		ocoLeg("a", condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "110", ""),
		ocoLeg("b", condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", ""),
	}
	gid1, _, ok1, err := e.PlaceOCO(context.Background(), "u1", "dedup-oco", req)
	if err != nil || !ok1 {
		t.Fatal(err)
	}
	// Rebuild the reqs with fresh client_trigger_ids so the per-leg dedup
	// doesn't interfere — we're testing group-level dedup.
	req2 := []*condrpc.PlaceTriggerRequest{
		ocoLeg("c", condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "110", ""),
		ocoLeg("d", condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", ""),
	}
	gid2, _, ok2, err := e.PlaceOCO(context.Background(), "u1", "dedup-oco", req2)
	if err != nil {
		t.Fatal(err)
	}
	if ok2 {
		t.Error("dup should report accepted=false")
	}
	if gid2 != gid1 {
		t.Errorf("gid drift: got %q want %q", gid2, gid1)
	}
	// Reserve must NOT be called for the second OCO.
	if reserver.reserveCount() != 2 {
		t.Errorf("reserve called %d times, want 2 (dup should not reserve)", reserver.reserveCount())
	}
}

func TestPlaceOCO_MismatchedSymbolRejected(t *testing.T) {
	e := newEngineWithReserver(&fakePlacer{}, nil)
	legA := ocoLeg("a", condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "")
	legB := ocoLeg("b", condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "110", "")
	legB.Symbol = "ETH-USDT"
	if _, _, _, err := e.PlaceOCO(context.Background(), "u1", "", []*condrpc.PlaceTriggerRequest{legA, legB}); !errors.Is(err, ErrOCOSymbolMismatch) {
		t.Errorf("err = %v, want ErrOCOSymbolMismatch", err)
	}
}

func TestPlaceOCO_MismatchedSideRejected(t *testing.T) {
	e := newEngineWithReserver(&fakePlacer{}, nil)
	legA := ocoLeg("a", condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "")
	legB := ocoLeg("b", condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "110", "")
	legB.Side = eventpb.Side_SIDE_BUY
	legB.Type = condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS
	legB.QuoteQty = "100"
	legB.Qty = ""
	if _, _, _, err := e.PlaceOCO(context.Background(), "u1", "", []*condrpc.PlaceTriggerRequest{legA, legB}); !errors.Is(err, ErrOCOSideMismatch) {
		t.Errorf("err = %v, want ErrOCOSideMismatch", err)
	}
}

func TestPlaceOCO_TooFewLegsRejected(t *testing.T) {
	e := newEngineWithReserver(&fakePlacer{}, nil)
	if _, _, _, err := e.PlaceOCO(context.Background(), "u1", "", []*condrpc.PlaceTriggerRequest{
		ocoLeg("only", condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", ""),
	}); !errors.Is(err, ErrOCONeedsTwoLegs) {
		t.Errorf("err = %v, want ErrOCONeedsTwoLegs", err)
	}
}

func TestPlaceOCO_CancelOneCascadesToSibling(t *testing.T) {
	placer := &fakePlacer{}
	reserver := &fakeReserver{}
	e := newEngineWithReserver(placer, reserver)
	_, legs, _, _ := e.PlaceOCO(context.Background(), "u1", "", []*condrpc.PlaceTriggerRequest{
		ocoLeg("a", condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", ""),
		ocoLeg("b", condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "110", ""),
	})
	// Cancel leg[0]; leg[1] should cascade to CANCELED.
	_, _, _ = e.Cancel(context.Background(), "u1", legs[0].ID)

	sib, err := e.Get("u1", legs[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if sib.Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Errorf("sibling status = %v, want CANCELED", sib.Status)
	}
	if reserver.releaseCount() != 2 {
		t.Errorf("release called %d times, want 2 (one per leg)", reserver.releaseCount())
	}
}

func TestPlaceOCO_TriggerOneCascadesToSibling(t *testing.T) {
	placer := &fakePlacer{}
	reserver := &fakeReserver{}
	e := newEngineWithReserver(placer, reserver)
	_, legs, _, _ := e.PlaceOCO(context.Background(), "u1", "", []*condrpc.PlaceTriggerRequest{
		ocoLeg("a", condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", ""),
		ocoLeg("b", condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "110", ""),
	})
	// Price 85 triggers leg-a (stop_loss @ 90); leg-b should cascade CANCELED.
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "85"), 0, 1)

	fired, err := e.Get("u1", legs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if fired.Status != condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED {
		t.Errorf("fired status = %v, want TRIGGERED", fired.Status)
	}
	sib, err := e.Get("u1", legs[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if sib.Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Errorf("sibling status = %v, want CANCELED", sib.Status)
	}
	// One Counter PlaceOrder only (the triggered leg). Sibling never fires.
	if len(placer.calls()) != 1 {
		t.Errorf("PlaceOrder calls = %d, want 1", len(placer.calls()))
	}
	// Consumed reservation (leg-a via PlaceOrder) + released reservation (leg-b).
	if reserver.releaseCount() != 1 {
		t.Errorf("release called %d, want 1 (only cancelled sibling)", reserver.releaseCount())
	}
}

func TestPlaceOCO_ExpirationCascadesToSibling(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	placer := &fakePlacer{}
	reserver := &fakeReserver{}
	e := withClock(placer, reserver, &now)
	expLeg := ocoLeg("exp", condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, "90", "")
	expLeg.ExpiresAtUnixMs = now.Add(time.Minute).UnixMilli()
	steadyLeg := ocoLeg("steady", condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, "110", "")
	_, legs, _, err := e.PlaceOCO(context.Background(), "u1", "", []*condrpc.PlaceTriggerRequest{expLeg, steadyLeg})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if n := e.SweepExpired(context.Background()); n != 1 {
		t.Fatalf("sweep count = %d, want 1 (only the expired leg)", n)
	}
	sib, _ := e.Get("u1", legs[1].ID)
	if sib.Status != condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED {
		t.Errorf("sibling status = %v, want CANCELED", sib.Status)
	}
}

// ---------------------------------------------------------------------------
// Trailing stop (MVP-14f / ADR-0045)
// ---------------------------------------------------------------------------

func trailingSellReq(delta int32, activation string) *condrpc.PlaceTriggerRequest {
	return &condrpc.PlaceTriggerRequest{
		UserId:           "u1",
		Symbol:           "BTC-USDT",
		Side:             eventpb.Side_SIDE_SELL,
		Type:             condrpc.TriggerType_TRIGGER_TYPE_TRAILING_STOP_LOSS,
		Qty:              "0.5",
		TrailingDeltaBps: delta,
		ActivationPrice:  activation,
	}
}

func TestPlace_TrailingRequiresDelta(t *testing.T) {
	e := newEngine(&fakePlacer{})
	req := trailingSellReq(0, "")
	if _, _, _, err := e.Place(context.Background(), req); !errors.Is(err, ErrTrailingDeltaNeeded) {
		t.Errorf("err = %v, want ErrTrailingDeltaNeeded", err)
	}
}

func TestPlace_TrailingDeltaRangeEnforced(t *testing.T) {
	e := newEngine(&fakePlacer{})
	req := trailingSellReq(10_001, "")
	if _, _, _, err := e.Place(context.Background(), req); !errors.Is(err, ErrTrailingDeltaRange) {
		t.Errorf("err = %v, want ErrTrailingDeltaRange", err)
	}
}

func TestPlace_NonTrailingWithDeltaRejected(t *testing.T) {
	e := newEngine(&fakePlacer{})
	req := goodReq()
	req.TrailingDeltaBps = 100
	if _, _, _, err := e.Place(context.Background(), req); !errors.Is(err, ErrTrailingDeltaForbidden) {
		t.Errorf("err = %v, want ErrTrailingDeltaForbidden", err)
	}
}

func TestTrailing_SellWatermarkAndFire(t *testing.T) {
	placer := &fakePlacer{}
	reserver := &fakeReserver{}
	e := newEngineWithReserver(placer, reserver)
	// 500 bps = 5% retracement.
	id, _, _, err := e.Place(context.Background(), trailingSellReq(500, ""))
	if err != nil {
		t.Fatal(err)
	}

	// Price goes 100 → 110 → 115 (watermark), no fire yet.
	for _, p := range []string{"100", "110", "115"} {
		e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", p), 0, 1)
	}
	if calls := placer.calls(); len(calls) != 0 {
		t.Fatalf("fired too early: %d calls", len(calls))
	}
	got, _ := e.Get("u1", id)
	if !got.TrailingActive {
		t.Errorf("should be active after first price")
	}
	if got.TrailingWatermark.String() != "115" {
		t.Errorf("watermark = %s, want 115", got.TrailingWatermark.String())
	}

	// Price 115 × (1 - 0.05) = 109.25 → first price ≤ 109.25 triggers.
	// 109 is below; fires.
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "109"), 0, 2)
	if calls := placer.calls(); len(calls) != 1 {
		t.Fatalf("fire count = %d, want 1", len(calls))
	}
	got, _ = e.Get("u1", id)
	if got.Status != condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED {
		t.Errorf("status = %v", got.Status)
	}
}

func TestTrailing_BuyWatermarkAndFire(t *testing.T) {
	placer := &fakePlacer{}
	e := newEngineWithReserver(placer, nil)
	req := trailingSellReq(500, "")
	req.Side = eventpb.Side_SIDE_BUY
	req.Qty = ""
	req.QuoteQty = "100"
	id, _, _, err := e.Place(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Price drops 100 → 90 → 85 (watermark = 85), no fire.
	for _, p := range []string{"100", "90", "85"} {
		e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", p), 0, 1)
	}
	if len(placer.calls()) != 0 {
		t.Fatalf("fired too early")
	}
	// 85 × 1.05 = 89.25 → first price ≥ 89.25 fires. 90 is the trigger.
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "90"), 0, 2)
	if len(placer.calls()) != 1 {
		t.Fatalf("fire count = %d, want 1", len(placer.calls()))
	}
	got, _ := e.Get("u1", id)
	if got.Status != condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED {
		t.Errorf("status = %v", got.Status)
	}
}

func TestTrailing_ActivationPriceGatesWatermark(t *testing.T) {
	placer := &fakePlacer{}
	e := newEngineWithReserver(placer, nil)
	// sell, trailing 500 bps, activate only once price ≥ 120.
	req := trailingSellReq(500, "120")
	id, _, _, err := e.Place(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Price 115 → not activated; watermark stays 0.
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "115"), 0, 1)
	got, _ := e.Get("u1", id)
	if got.TrailingActive {
		t.Errorf("should not be active below activation price")
	}
	if !got.TrailingWatermark.IsZero() {
		t.Errorf("watermark = %s, want 0", got.TrailingWatermark.String())
	}

	// 130 crosses activation → active + watermark initializes.
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "130"), 0, 2)
	got, _ = e.Get("u1", id)
	if !got.TrailingActive {
		t.Errorf("should be active after crossing activation")
	}
	if got.TrailingWatermark.String() != "130" {
		t.Errorf("watermark = %s", got.TrailingWatermark.String())
	}

	// 130 × 0.95 = 123.5 → 120 retraces below → fire.
	e.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "120"), 0, 3)
	if len(placer.calls()) != 1 {
		t.Fatalf("fire count = %d, want 1", len(placer.calls()))
	}
}

func TestPlace_TrailingRejectsStopPrice(t *testing.T) {
	e := newEngine(&fakePlacer{})
	req := trailingSellReq(500, "")
	req.StopPrice = "100"
	if _, _, _, err := e.Place(context.Background(), req); !errors.Is(err, ErrStopPriceForbidden) {
		t.Errorf("err = %v, want ErrStopPriceForbidden", err)
	}
}

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	placer := &fakePlacer{}
	src := newEngine(placer)
	id, _, _, _ := src.Place(context.Background(), goodReq())
	src.HandleRecord(context.Background(), publicTradeEvent("BTC-USDT", "101"), 0, 5)

	pending, terminals, offsets := snapshotEngineState(src)

	dst := newEngine(placer)
	dst.Restore(pending, terminals, offsets)

	got, err := dst.Get("u1", id)
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.Status != condrpc.TriggerStatus_TRIGGER_STATUS_PENDING {
		t.Errorf("status: %v", got.Status)
	}
	if dst.Offsets()[0] != 6 {
		t.Errorf("offset: %d", dst.Offsets()[0])
	}
}

// snapshotEngineState reads pending / terminal / offset state directly off
// the engine for round-trip tests. Production capture lives in trade-dump's
// shadow per ADR-0067, so the engine itself no longer exports a getter.
func snapshotEngineState(e *Engine) ([]*Trigger, []*Trigger, map[int32]int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	pending := make([]*Trigger, 0, len(e.pending))
	for _, c := range e.pending {
		cp := *c
		pending = append(pending, &cp)
	}
	terminals := make([]*Trigger, 0, len(e.termOrder))
	for _, id := range e.termOrder {
		if c, ok := e.terminals[id]; ok {
			cp := *c
			terminals = append(terminals, &cp)
		}
	}
	offsets := make(map[int32]int64, len(e.offsets))
	for p, o := range e.offsets {
		offsets[p] = o
	}
	return pending, terminals, offsets
}
