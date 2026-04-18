package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/bff/internal/auth"
)

// -----------------------------------------------------------------------------
// Fake Counter implementation
// -----------------------------------------------------------------------------

type fakeCounter struct {
	placeFn    func(*counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error)
	cancelFn   func(*counterrpc.CancelOrderRequest) (*counterrpc.CancelOrderResponse, error)
	queryFn    func(*counterrpc.QueryOrderRequest) (*counterrpc.QueryOrderResponse, error)
	transferFn func(*counterrpc.TransferRequest) (*counterrpc.TransferResponse, error)
	balanceFn  func(*counterrpc.QueryBalanceRequest) (*counterrpc.QueryBalanceResponse, error)
}

func (f *fakeCounter) PlaceOrder(_ context.Context, req *counterrpc.PlaceOrderRequest, _ ...grpc.CallOption) (*counterrpc.PlaceOrderResponse, error) {
	return f.placeFn(req)
}
func (f *fakeCounter) CancelOrder(_ context.Context, req *counterrpc.CancelOrderRequest, _ ...grpc.CallOption) (*counterrpc.CancelOrderResponse, error) {
	return f.cancelFn(req)
}
func (f *fakeCounter) QueryOrder(_ context.Context, req *counterrpc.QueryOrderRequest, _ ...grpc.CallOption) (*counterrpc.QueryOrderResponse, error) {
	return f.queryFn(req)
}
func (f *fakeCounter) Transfer(_ context.Context, req *counterrpc.TransferRequest, _ ...grpc.CallOption) (*counterrpc.TransferResponse, error) {
	return f.transferFn(req)
}
func (f *fakeCounter) QueryBalance(_ context.Context, req *counterrpc.QueryBalanceRequest, _ ...grpc.CallOption) (*counterrpc.QueryBalanceResponse, error) {
	return f.balanceFn(req)
}

func newServer(fc *fakeCounter) *Server {
	return NewServer(Config{
		UserRateLimit:  100, UserRateWindow: time.Second,
		IPRateLimit:    100, IPRateWindow:   time.Second,
		RequestTimeout: time.Second,
	}, fc, nil, nil, nil, zap.NewNop())
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
				OrderId:         100,
				ClientOrderId:   req.ClientOrderId,
				Accepted:        true,
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

func TestGRPCErrorMapping(t *testing.T) {
	fc := &fakeCounter{
		placeFn: func(_ *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
			return nil, status.Error(codes.FailedPrecondition, "insufficient balance")
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
	}, fc, nil, nil, nil, zap.NewNop())
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
		transferFn: func(req *counterrpc.TransferRequest) (*counterrpc.TransferResponse, error) {
			return &counterrpc.TransferResponse{
				TransferId: req.TransferId,
				Status:     counterrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED,
				AvailableAfter: "100",
				FrozenAfter:    "0",
			}, nil
		},
		balanceFn: func(req *counterrpc.QueryBalanceRequest) (*counterrpc.QueryBalanceResponse, error) {
			return &counterrpc.QueryBalanceResponse{
				Balances: []*counterrpc.Balance{{Asset: "USDT", Available: "100", Frozen: "0"}},
			}, nil
		},
	}
	srv := newServer(fc)
	h := srv.Handler()

	// Transfer
	req := httptest.NewRequest(http.MethodPost, "/v1/transfer",
		bytes.NewBufferString(`{"transfer_id":"tx1","asset":"USDT","amount":"100","type":"deposit"}`))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("transfer code = %d, body = %s", rr.Code, rr.Body.String())
	}
	var tresp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &tresp)
	if tresp["status"] != "confirmed" {
		t.Fatalf("transfer status = %v", tresp["status"])
	}

	// Balance
	req = httptest.NewRequest(http.MethodGet, "/v1/account?asset=USDT", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("balance code = %d", rr.Code)
	}
}
