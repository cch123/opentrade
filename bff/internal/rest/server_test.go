package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	assetrpc "github.com/xargin/opentrade/api/gen/rpc/asset"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/bff/internal/client"
	"github.com/xargin/opentrade/bff/internal/marketcache"
	"github.com/xargin/opentrade/pkg/auth"
)

// -----------------------------------------------------------------------------
// Fake Counter implementation
// -----------------------------------------------------------------------------

type fakeCounter struct {
	placeFn       func(*counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error)
	cancelFn      func(*counterrpc.CancelOrderRequest) (*counterrpc.CancelOrderResponse, error)
	queryFn       func(*counterrpc.QueryOrderRequest) (*counterrpc.QueryOrderResponse, error)
	balanceFn     func(*counterrpc.QueryBalanceRequest) (*counterrpc.QueryBalanceResponse, error)
	adminCancelFn func(*counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error)
	myCancelFn    func(*counterrpc.CancelMyOrdersRequest) (*counterrpc.CancelMyOrdersResponse, error)
}

func (f *fakeCounter) PlaceOrder(_ context.Context, req *connect.Request[counterrpc.PlaceOrderRequest]) (*connect.Response[counterrpc.PlaceOrderResponse], error) {
	resp, err := f.placeFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeCounter) CancelOrder(_ context.Context, req *connect.Request[counterrpc.CancelOrderRequest]) (*connect.Response[counterrpc.CancelOrderResponse], error) {
	resp, err := f.cancelFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeCounter) QueryOrder(_ context.Context, req *connect.Request[counterrpc.QueryOrderRequest]) (*connect.Response[counterrpc.QueryOrderResponse], error) {
	resp, err := f.queryFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeCounter) QueryBalance(_ context.Context, req *connect.Request[counterrpc.QueryBalanceRequest]) (*connect.Response[counterrpc.QueryBalanceResponse], error) {
	resp, err := f.balanceFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeCounter) Reserve(_ context.Context, _ *connect.Request[counterrpc.ReserveRequest]) (*connect.Response[counterrpc.ReserveResponse], error) {
	return connect.NewResponse(&counterrpc.ReserveResponse{}), nil
}
func (f *fakeCounter) ReleaseReservation(_ context.Context, _ *connect.Request[counterrpc.ReleaseReservationRequest]) (*connect.Response[counterrpc.ReleaseReservationResponse], error) {
	return connect.NewResponse(&counterrpc.ReleaseReservationResponse{}), nil
}
func (f *fakeCounter) AdminCancelOrders(_ context.Context, req *connect.Request[counterrpc.AdminCancelOrdersRequest]) (*connect.Response[counterrpc.AdminCancelOrdersResponse], error) {
	if f.adminCancelFn == nil {
		return connect.NewResponse(&counterrpc.AdminCancelOrdersResponse{}), nil
	}
	resp, err := f.adminCancelFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeCounter) CancelMyOrders(_ context.Context, req *connect.Request[counterrpc.CancelMyOrdersRequest]) (*connect.Response[counterrpc.CancelMyOrdersResponse], error) {
	if f.myCancelFn == nil {
		return connect.NewResponse(&counterrpc.CancelMyOrdersResponse{}), nil
	}
	resp, err := f.myCancelFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

// fakeAsset is the test double for client.Asset (ADR-0057 M4). Only the
// methods exercised by the REST layer are populated; unwired methods
// return an empty response with no error so tests that don't care about
// them stay terse.
type fakeAsset struct {
	transferFn      func(*assetrpc.TransferRequest) (*assetrpc.TransferResponse, error)
	queryTransferFn func(*assetrpc.QueryTransferRequest) (*assetrpc.QueryTransferResponse, error)
	listTransfersFn func(*assetrpc.ListTransfersRequest) (*assetrpc.ListTransfersResponse, error)
	fundingBalFn    func(*assetrpc.QueryFundingBalanceRequest) (*assetrpc.QueryFundingBalanceResponse, error)
}

func (f *fakeAsset) Transfer(_ context.Context, req *connect.Request[assetrpc.TransferRequest]) (*connect.Response[assetrpc.TransferResponse], error) {
	if f.transferFn == nil {
		return connect.NewResponse(&assetrpc.TransferResponse{TransferId: req.Msg.TransferId}), nil
	}
	resp, err := f.transferFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeAsset) QueryTransfer(_ context.Context, req *connect.Request[assetrpc.QueryTransferRequest]) (*connect.Response[assetrpc.QueryTransferResponse], error) {
	if f.queryTransferFn == nil {
		return connect.NewResponse(&assetrpc.QueryTransferResponse{TransferId: req.Msg.TransferId}), nil
	}
	resp, err := f.queryTransferFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeAsset) ListTransfers(_ context.Context, req *connect.Request[assetrpc.ListTransfersRequest]) (*connect.Response[assetrpc.ListTransfersResponse], error) {
	if f.listTransfersFn == nil {
		return connect.NewResponse(&assetrpc.ListTransfersResponse{}), nil
	}
	resp, err := f.listTransfersFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeAsset) QueryFundingBalance(_ context.Context, req *connect.Request[assetrpc.QueryFundingBalanceRequest]) (*connect.Response[assetrpc.QueryFundingBalanceResponse], error) {
	if f.fundingBalFn == nil {
		return connect.NewResponse(&assetrpc.QueryFundingBalanceResponse{}), nil
	}
	resp, err := f.fundingBalFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func newServer(fc *fakeCounter) *Server {
	return newServerWithAsset(fc, nil)
}

func newServerWithAsset(fc *fakeCounter, fa *fakeAsset) *Server {
	// Pass an interface-typed nil when the caller wants asset disabled;
	// a typed *fakeAsset nil would land as a non-nil interface value
	// and defeat the `s.asset == nil` 503 check.
	var asset client.Asset
	if fa != nil {
		asset = fa
	}
	return NewServer(Config{
		UserRateLimit: 100, UserRateWindow: time.Second,
		IPRateLimit: 100, IPRateWindow: time.Second,
		RequestTimeout: time.Second,
	}, fc, asset, nil, nil, nil, zap.NewNop())
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

func TestPlaceOrderHappyPath(t *testing.T) {
	fc := &fakeCounter{
		placeFn: func(req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			if req.UserId != "u1" || req.Symbol != "BTC-USDT" {
				t.Fatalf("req = %+v", req)
			}
			if req.Side != eventpb.Side_SIDE_BUY {
				t.Fatalf("side = %v", req.Side)
			}
			return &counterrpc.PlaceOrderResponse{
				OrderId:          100,
				ClientOrderId:    req.ClientOrderId,
				Accepted:         true,
				ReceivedTsUnixMs: 12345,
			}, nil
		},
	}
	srv := newServer(fc)

	body := `{"symbol":"BTC-USDT","side":"buy","order_type":"limit","tif":"gtc","price":"100","qty":"1","client_order_id":"c1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["accepted"] != true {
		t.Fatalf("resp = %+v", resp)
	}
	if resp["order_id"].(float64) != 100 {
		t.Fatalf("order_id = %v", resp["order_id"])
	}
}

func TestPlaceOrderMissingAuth(t *testing.T) {
	srv := newServer(&fakeCounter{})
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(`{"symbol":"BTC-USDT","side":"buy","order_type":"limit","tif":"gtc","price":"100","qty":"1"}`))
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d", rr.Code)
	}
}

func TestPlaceOrderInvalidSide(t *testing.T) {
	srv := newServer(&fakeCounter{})
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(`{"symbol":"BTC-USDT","side":"long","order_type":"limit","tif":"gtc","price":"100","qty":"1"}`))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rr.Code)
	}
}

func TestPlaceOrder_MarketBuyNeedsQuoteQty(t *testing.T) {
	// Market buy without quote_qty and without slippage-translation params
	// is ambiguous — ADR-0035 requires the quoteOrderQty shape (or a
	// client-translated LIMIT+IOC). BFF refuses at the REST layer so we
	// don't forward junk to counter.
	fc := &fakeCounter{
		placeFn: func(req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			t.Fatalf("counter should not be called: %+v", req)
			return nil, nil
		},
	}
	srv := newServer(fc)
	body := `{"symbol":"BTC-USDT","side":"buy","order_type":"market","qty":"1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
}

func TestPlaceOrder_MarketBuyByQuoteQtyForwarded(t *testing.T) {
	var seen *counterrpc.PlaceOrderRequest
	fc := &fakeCounter{
		placeFn: func(req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			seen = req
			return &counterrpc.PlaceOrderResponse{OrderId: 1, Accepted: true}, nil
		},
	}
	srv := newServer(fc)
	body := `{"symbol":"BTC-USDT","side":"buy","order_type":"market","quote_qty":"100"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	if seen == nil || seen.OrderType != eventpb.OrderType_ORDER_TYPE_MARKET {
		t.Fatalf("counter didn't see market: %+v", seen)
	}
	if seen.QuoteQty != "100" || seen.Qty != "" {
		t.Errorf("forwarded fields: qty=%q quote_qty=%q", seen.Qty, seen.QuoteQty)
	}
}

func TestPlaceOrder_MarketSellForwarded(t *testing.T) {
	var seen *counterrpc.PlaceOrderRequest
	fc := &fakeCounter{
		placeFn: func(req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			seen = req
			return &counterrpc.PlaceOrderResponse{OrderId: 1, Accepted: true}, nil
		},
	}
	srv := newServer(fc)
	body := `{"symbol":"BTC-USDT","side":"sell","order_type":"market","qty":"0.5"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	if seen.OrderType != eventpb.OrderType_ORDER_TYPE_MARKET || seen.Side != eventpb.Side_SIDE_SELL {
		t.Fatalf("wrong shape: %+v", seen)
	}
	if seen.Qty != "0.5" {
		t.Errorf("qty forwarded = %q", seen.Qty)
	}
}

// ADR-0053 M3.b: BFF pulls best bid/ask from market cache and sends mid
// as reference_price so counter can enforce MARKET-by-base precision.
func TestPlaceOrder_ReferencePriceFilledFromDepthCache(t *testing.T) {
	cache := marketcache.New(marketcache.Config{})
	cache.PutOrderBookFull("BTC-USDT", 1, &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{{Price: "49990", Qty: "0.5"}},
		Asks: []*eventpb.OrderBookLevel{{Price: "50010", Qty: "0.5"}},
	})

	var seen *counterrpc.PlaceOrderRequest
	fc := &fakeCounter{
		placeFn: func(req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			seen = req
			return &counterrpc.PlaceOrderResponse{OrderId: 1, Accepted: true}, nil
		},
	}
	srv := NewServer(Config{
		UserRateLimit: 100, UserRateWindow: time.Second,
		IPRateLimit: 100, IPRateWindow: time.Second,
		RequestTimeout: time.Second,
	}, fc, nil, cache, nil, nil, zap.NewNop())

	body := `{"symbol":"BTC-USDT","side":"sell","order_type":"market","qty":"0.5"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rr.Code, rr.Body.String())
	}
	if seen.ReferencePrice != "50000" {
		t.Errorf("reference_price=%q want 50000 (= (49990+50010)/2)", seen.ReferencePrice)
	}
}

// Cold BFF (no cache at all): reference_price stays empty, counter falls
// back to M3 behaviour of skipping MARKET-by-base precision.
func TestPlaceOrder_ReferencePriceEmptyWhenNoCache(t *testing.T) {
	var seen *counterrpc.PlaceOrderRequest
	fc := &fakeCounter{
		placeFn: func(req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			seen = req
			return &counterrpc.PlaceOrderResponse{OrderId: 1, Accepted: true}, nil
		},
	}
	srv := newServer(fc) // newServer passes market=nil
	body := `{"symbol":"BTC-USDT","side":"sell","order_type":"market","qty":"0.5"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rr.Code, rr.Body.String())
	}
	if seen.ReferencePrice != "" {
		t.Errorf("reference_price=%q, want empty", seen.ReferencePrice)
	}
}

// Cache exists but hasn't observed the symbol yet (e.g. just restarted,
// depth event not replayed): reference_price empty, same fallback.
func TestPlaceOrder_ReferencePriceEmptyWhenSnapshotMissing(t *testing.T) {
	cache := marketcache.New(marketcache.Config{})
	// No PutDepthSnapshot for BTC-USDT.

	var seen *counterrpc.PlaceOrderRequest
	fc := &fakeCounter{
		placeFn: func(req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			seen = req
			return &counterrpc.PlaceOrderResponse{OrderId: 1, Accepted: true}, nil
		},
	}
	srv := NewServer(Config{
		UserRateLimit: 100, UserRateWindow: time.Second,
		IPRateLimit: 100, IPRateWindow: time.Second,
		RequestTimeout: time.Second,
	}, fc, nil, cache, nil, nil, zap.NewNop())

	body := `{"symbol":"BTC-USDT","side":"sell","order_type":"market","qty":"0.5"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rr.Code, rr.Body.String())
	}
	if seen.ReferencePrice != "" {
		t.Errorf("reference_price=%q, want empty", seen.ReferencePrice)
	}
}

func TestPlaceOrder_MarketBuyWithSlippageTranslatesToLimitIOC(t *testing.T) {
	var seen *counterrpc.PlaceOrderRequest
	fc := &fakeCounter{
		placeFn: func(req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			seen = req
			return &counterrpc.PlaceOrderResponse{OrderId: 1, Accepted: true}, nil
		},
	}
	srv := newServer(fc)
	// buy @ last=50000 + 50 bps slippage → protected price = 50250
	body := `{"symbol":"BTC-USDT","side":"buy","order_type":"market","qty":"0.5","last_price":"50000","slippage_bps":50}`
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	if seen.OrderType != eventpb.OrderType_ORDER_TYPE_LIMIT {
		t.Fatalf("should be translated to LIMIT: %+v", seen)
	}
	if seen.Tif != eventpb.TimeInForce_TIME_IN_FORCE_IOC {
		t.Errorf("tif = %v, want IOC", seen.Tif)
	}
	if seen.Price != "50250" {
		t.Errorf("price = %q, want 50250", seen.Price)
	}
}

func TestPlaceOrder_MarketWithSlippageButNoLastPrice(t *testing.T) {
	srv := newServer(&fakeCounter{})
	body := `{"symbol":"BTC-USDT","side":"buy","order_type":"market","qty":"0.5","slippage_bps":50}`
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
}

func TestCancelOrderAndQuery(t *testing.T) {
	fc := &fakeCounter{
		cancelFn: func(req *counterrpc.CancelOrderRequest) (*counterrpc.CancelOrderResponse, error) {
			if req.OrderId != 42 || req.UserId != "u1" {
				t.Fatalf("req = %+v", req)
			}
			return &counterrpc.CancelOrderResponse{OrderId: 42, Accepted: true}, nil
		},
		queryFn: func(req *counterrpc.QueryOrderRequest) (*counterrpc.QueryOrderResponse, error) {
			return &counterrpc.QueryOrderResponse{
				OrderId: req.OrderId, Symbol: "BTC-USDT",
				Side: eventpb.Side_SIDE_BUY, OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT,
				Tif: eventpb.TimeInForce_TIME_IN_FORCE_GTC,
				Price: "100", Qty: "1", FilledQty: "0", FrozenAmt: "100",
				Status: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
			}, nil
		},
	}
	srv := newServer(fc)

	// Cancel
	req := httptest.NewRequest(http.MethodDelete, "/v1/order/42", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("cancel code = %d, body = %s", rr.Code, rr.Body.String())
	}

	// Query (folds PENDING_NEW → "new")
	req = httptest.NewRequest(http.MethodGet, "/v1/order/42", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("query code = %d", rr.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["status"] != "new" {
		t.Fatalf("external status = %v", resp["status"])
	}
	if resp["side"] != "buy" {
		t.Fatalf("side = %v", resp["side"])
	}
}

func TestConnectErrorMapping(t *testing.T) {
	fc := &fakeCounter{
		placeFn: func(_ *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("insufficient balance"))
		},
	}
	srv := newServer(fc)
	req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(`{"symbol":"BTC-USDT","side":"buy","order_type":"limit","tif":"gtc","price":"100","qty":"1"}`))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("code = %d, body = %s", rr.Code, rr.Body.String())
	}
}

func TestRateLimitPerUser(t *testing.T) {
	fc := &fakeCounter{
		placeFn: func(_ *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			return &counterrpc.PlaceOrderResponse{OrderId: 1, Accepted: true}, nil
		},
	}
	srv := NewServer(Config{
		UserRateLimit: 2, UserRateWindow: time.Second,
		IPRateLimit: 100, IPRateWindow: time.Second,
	}, fc, nil, nil, nil, nil, zap.NewNop())
	h := srv.Handler()

	body := `{"symbol":"BTC-USDT","side":"buy","order_type":"limit","tif":"gtc","price":"100","qty":"1"}`
	send := func() int {
		req := httptest.NewRequest(http.MethodPost, "/v1/order", bytes.NewBufferString(body))
		req.Header.Set(auth.HeaderUserID, "u1")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}
	// First two succeed, third 429.
	if send() != http.StatusOK {
		t.Fatal("1st should be OK")
	}
	if send() != http.StatusOK {
		t.Fatal("2nd should be OK")
	}
	if send() != http.StatusTooManyRequests {
		t.Fatal("3rd should be 429")
	}
}

func TestTransferAndBalance(t *testing.T) {
	fc := &fakeCounter{
		balanceFn: func(req *counterrpc.QueryBalanceRequest) (*counterrpc.QueryBalanceResponse, error) {
			return &counterrpc.QueryBalanceResponse{
				Balances: []*counterrpc.Balance{{Asset: "USDT", Available: "100", Frozen: "0"}},
			}, nil
		},
	}
	var seen *assetrpc.TransferRequest
	fa := &fakeAsset{
		transferFn: func(req *assetrpc.TransferRequest) (*assetrpc.TransferResponse, error) {
			seen = req
			return &assetrpc.TransferResponse{
				TransferId: req.TransferId,
				State:      assetrpc.SagaState_SAGA_STATE_COMPLETED,
				Terminal:   true,
			}, nil
		},
	}
	srv := newServerWithAsset(fc, fa)
	h := srv.Handler()

	// Transfer — new ADR-0057 body: from_biz / to_biz instead of type.
	req := httptest.NewRequest(http.MethodPost, "/v1/transfer",
		bytes.NewBufferString(`{"transfer_id":"tx1","from_biz":"funding","to_biz":"spot","asset":"USDT","amount":"100"}`))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("transfer code = %d, body = %s", rr.Code, rr.Body.String())
	}
	var tresp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &tresp)
	if tresp["state"] != "COMPLETED" {
		t.Fatalf("transfer state = %v", tresp["state"])
	}
	if tresp["terminal"] != true {
		t.Fatalf("transfer terminal = %v", tresp["terminal"])
	}
	if seen == nil || seen.UserId != "u1" || seen.FromBiz != "funding" || seen.ToBiz != "spot" {
		t.Fatalf("asset transfer request = %+v", seen)
	}

	// Balance (spot)
	req = httptest.NewRequest(http.MethodGet, "/v1/account?asset=USDT", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("balance code = %d", rr.Code)
	}
}

func TestTransfer_AssetServiceNotConfigured(t *testing.T) {
	srv := newServer(&fakeCounter{})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/v1/transfer",
		bytes.NewBufferString(`{"transfer_id":"tx1","from_biz":"funding","to_biz":"spot","asset":"USDT","amount":"100"}`))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("code = %d, want 503", rr.Code)
	}
}

func TestQueryTransfer_ProxiesToAsset(t *testing.T) {
	fa := &fakeAsset{
		queryTransferFn: func(req *assetrpc.QueryTransferRequest) (*assetrpc.QueryTransferResponse, error) {
			if req.TransferId != "tx1" {
				t.Errorf("transfer_id = %q", req.TransferId)
			}
			return &assetrpc.QueryTransferResponse{
				TransferId: "tx1", UserId: "u1",
				FromBiz: "funding", ToBiz: "spot",
				Asset: "USDT", Amount: "100",
				State: assetrpc.SagaState_SAGA_STATE_DEBITED,
			}, nil
		},
	}
	srv := newServerWithAsset(&fakeCounter{}, fa)
	req := httptest.NewRequest(http.MethodGet, "/v1/transfer/tx1", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["state"] != "DEBITED" {
		t.Errorf("state = %v", resp["state"])
	}
	if resp["from_biz"] != "funding" || resp["to_biz"] != "spot" {
		t.Errorf("routing = %v / %v", resp["from_biz"], resp["to_biz"])
	}
}

func TestQueryFundingBalance_ProxiesToAsset(t *testing.T) {
	fa := &fakeAsset{
		fundingBalFn: func(req *assetrpc.QueryFundingBalanceRequest) (*assetrpc.QueryFundingBalanceResponse, error) {
			return &assetrpc.QueryFundingBalanceResponse{
				Balances: []*assetrpc.FundingBalance{
					{Asset: "USDT", Available: "500", Frozen: "0", Version: 3},
				},
			}, nil
		},
	}
	srv := newServerWithAsset(&fakeCounter{}, fa)
	req := httptest.NewRequest(http.MethodGet, "/v1/funding-balance?asset=USDT", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	bals, _ := resp["balances"].([]any)
	if len(bals) != 1 {
		t.Fatalf("balances = %+v", bals)
	}
	first, _ := bals[0].(map[string]any)
	if first["asset"] != "USDT" || first["available"] != "500" {
		t.Errorf("entry = %+v", first)
	}
}

func TestCancelMyOrders_ForwardsUserAndSymbol(t *testing.T) {
	var seen *counterrpc.CancelMyOrdersRequest
	fc := &fakeCounter{
		myCancelFn: func(req *counterrpc.CancelMyOrdersRequest) (*counterrpc.CancelMyOrdersResponse, error) {
			seen = req
			return &counterrpc.CancelMyOrdersResponse{Cancelled: 3, Skipped: 1}, nil
		},
	}
	srv := newServer(fc)
	req := httptest.NewRequest(http.MethodDelete, "/v1/orders?symbol=BTC-USDT", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	if seen == nil || seen.UserId != "u1" || seen.Symbol != "BTC-USDT" {
		t.Fatalf("forwarded req = %+v", seen)
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["cancelled"].(float64) != 3 || resp["skipped"].(float64) != 1 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestCancelMyOrders_NoSymbolDefaultsToAll(t *testing.T) {
	var seen *counterrpc.CancelMyOrdersRequest
	fc := &fakeCounter{
		myCancelFn: func(req *counterrpc.CancelMyOrdersRequest) (*counterrpc.CancelMyOrdersResponse, error) {
			seen = req
			return &counterrpc.CancelMyOrdersResponse{}, nil
		},
	}
	srv := newServer(fc)
	req := httptest.NewRequest(http.MethodDelete, "/v1/orders", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	if seen == nil || seen.UserId != "u1" || seen.Symbol != "" {
		t.Fatalf("forwarded req = %+v", seen)
	}
}

func TestCancelMyOrders_MissingAuth(t *testing.T) {
	srv := newServer(&fakeCounter{})
	req := httptest.NewRequest(http.MethodDelete, "/v1/orders", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d", rr.Code)
	}
}
