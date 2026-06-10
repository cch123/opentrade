package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/auth"
)

type fakePerp struct {
	placeFn func(*perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error)
}

func (f *fakePerp) PlaceOrder(_ context.Context, req *connect.Request[perprpc.PlaceOrderRequest]) (*connect.Response[perprpc.PlaceOrderResponse], error) {
	if f.placeFn != nil {
		resp, err := f.placeFn(req.Msg)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(resp), nil
	}
	return connect.NewResponse(&perprpc.PlaceOrderResponse{}), nil
}
func (f *fakePerp) CancelOrder(_ context.Context, req *connect.Request[perprpc.CancelOrderRequest]) (*connect.Response[perprpc.CancelOrderResponse], error) {
	return connect.NewResponse(&perprpc.CancelOrderResponse{OrderId: req.Msg.OrderId, Accepted: true}), nil
}
func (f *fakePerp) QueryOrder(_ context.Context, _ *connect.Request[perprpc.QueryOrderRequest]) (*connect.Response[perprpc.QueryOrderResponse], error) {
	return connect.NewResponse(&perprpc.QueryOrderResponse{}), nil
}
func (f *fakePerp) QueryPositions(_ context.Context, _ *connect.Request[perprpc.QueryPositionsRequest]) (*connect.Response[perprpc.QueryPositionsResponse], error) {
	return connect.NewResponse(&perprpc.QueryPositionsResponse{Positions: []*perprpc.Position{
		{Symbol: "BTC-USDT-PERP", Side: eventpb.Side_SIDE_BUY, Size: "1", EntryPrice: "100"},
	}}), nil
}
func (f *fakePerp) QueryMargin(_ context.Context, _ *connect.Request[perprpc.QueryMarginRequest]) (*connect.Response[perprpc.QueryMarginResponse], error) {
	return connect.NewResponse(&perprpc.QueryMarginResponse{Asset: "USDT", FreeBalance: "990", OrderMarginReserved: "10"}), nil
}
func (f *fakePerp) SetMarginMode(_ context.Context, _ *connect.Request[perprpc.SetMarginModeRequest]) (*connect.Response[perprpc.SetMarginModeResponse], error) {
	return connect.NewResponse(&perprpc.SetMarginModeResponse{Accepted: true}), nil
}
func (f *fakePerp) AdjustIsolatedMargin(_ context.Context, _ *connect.Request[perprpc.AdjustIsolatedMarginRequest]) (*connect.Response[perprpc.AdjustIsolatedMarginResponse], error) {
	return connect.NewResponse(&perprpc.AdjustIsolatedMarginResponse{Accepted: true}), nil
}
func (f *fakePerp) SetAutoAddMargin(_ context.Context, _ *connect.Request[perprpc.SetAutoAddMarginRequest]) (*connect.Response[perprpc.SetAutoAddMarginResponse], error) {
	return connect.NewResponse(&perprpc.SetAutoAddMarginResponse{Accepted: true}), nil
}
func (f *fakePerp) SetPositionMode(_ context.Context, _ *connect.Request[perprpc.SetPositionModeRequest]) (*connect.Response[perprpc.SetPositionModeResponse], error) {
	return connect.NewResponse(&perprpc.SetPositionModeResponse{Accepted: true}), nil
}
func (f *fakePerp) SetPositionLeverage(_ context.Context, _ *connect.Request[perprpc.SetPositionLeverageRequest]) (*connect.Response[perprpc.SetPositionLeverageResponse], error) {
	return connect.NewResponse(&perprpc.SetPositionLeverageResponse{Accepted: true}), nil
}
func (f *fakePerp) SetRiskId(_ context.Context, _ *connect.Request[perprpc.SetRiskIdRequest]) (*connect.Response[perprpc.SetRiskIdResponse], error) {
	return connect.NewResponse(&perprpc.SetRiskIdResponse{Accepted: true}), nil
}
func (f *fakePerp) QueryPositionConfig(_ context.Context, _ *connect.Request[perprpc.QueryPositionConfigRequest]) (*connect.Response[perprpc.QueryPositionConfigResponse], error) {
	return connect.NewResponse(&perprpc.QueryPositionConfigResponse{}), nil
}
func (f *fakePerp) QueryAccountConfig(_ context.Context, _ *connect.Request[perprpc.QueryAccountConfigRequest]) (*connect.Response[perprpc.QueryAccountConfigResponse], error) {
	return connect.NewResponse(&perprpc.QueryAccountConfigResponse{}), nil
}
func (f *fakePerp) SetCustomerLeverageLimit(_ context.Context, _ *connect.Request[perprpc.SetCustomerLeverageLimitRequest]) (*connect.Response[perprpc.SetCustomerLeverageLimitResponse], error) {
	return connect.NewResponse(&perprpc.SetCustomerLeverageLimitResponse{Accepted: true}), nil
}
func (f *fakePerp) ListCustomerLeverageLimits(_ context.Context, _ *connect.Request[perprpc.ListCustomerLeverageLimitsRequest]) (*connect.Response[perprpc.ListCustomerLeverageLimitsResponse], error) {
	return connect.NewResponse(&perprpc.ListCustomerLeverageLimitsResponse{}), nil
}
func (f *fakePerp) SetCustomerFeeRate(_ context.Context, _ *connect.Request[perprpc.SetCustomerFeeRateRequest]) (*connect.Response[perprpc.SetCustomerFeeRateResponse], error) {
	return connect.NewResponse(&perprpc.SetCustomerFeeRateResponse{Accepted: true}), nil
}
func (f *fakePerp) ListCustomerFeeRates(_ context.Context, _ *connect.Request[perprpc.ListCustomerFeeRatesRequest]) (*connect.Response[perprpc.ListCustomerFeeRatesResponse], error) {
	return connect.NewResponse(&perprpc.ListCustomerFeeRatesResponse{}), nil
}
func (f *fakePerp) ProjectRiskConfig(_ context.Context, _ *connect.Request[perprpc.ProjectRiskConfigRequest]) (*connect.Response[perprpc.ProjectRiskConfigResponse], error) {
	return connect.NewResponse(&perprpc.ProjectRiskConfigResponse{}), nil
}

func newPerpServer(fp *fakePerp) *Server {
	srv := NewServer(Config{
		UserRateLimit: 100, UserRateWindow: time.Second,
		IPRateLimit: 100, IPRateWindow: time.Second,
	}, &fakeCounter{}, nil, nil, nil, nil, zap.NewNop())
	if fp != nil {
		srv.SetPerp(fp)
	}
	return srv
}

func TestPerpPlaceOrder_HappyPath(t *testing.T) {
	fp := &fakePerp{placeFn: func(req *perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error) {
		if req.UserId != 1001 || req.Symbol != "BTC-USDT-PERP" || req.Leverage != "10" || !req.ReduceOnly {
			t.Fatalf("req = %+v", req)
		}
		return &perprpc.PlaceOrderResponse{OrderId: 42, Accepted: true, ReceivedTsUnixMs: 7}, nil
	}}
	srv := newPerpServer(fp)

	body := `{"symbol":"BTC-USDT-PERP","side":"sell","order_type":"limit","tif":"gtc","price":"100","qty":"1","leverage":"10","reduce_only":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/perp/order", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "1001")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["accepted"] != true || resp["order_id"].(float64) != 42 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestPerpPositionsAndMargin(t *testing.T) {
	srv := newPerpServer(&fakePerp{})

	req := httptest.NewRequest(http.MethodGet, "/v1/perp/positions", nil)
	req.Header.Set(auth.HeaderUserID, "1001")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("positions code = %d", rr.Code)
	}
	var pr map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &pr)
	if ps, ok := pr["positions"].([]any); !ok || len(ps) != 1 {
		t.Fatalf("positions = %+v", pr)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/perp/margin", nil)
	req.Header.Set(auth.HeaderUserID, "1001")
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	var mr map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &mr)
	if mr["free_balance"] != "990" || mr["order_margin_reserved"] != "10" {
		t.Fatalf("margin = %+v", mr)
	}
}

func TestPerp_503WhenDisabled(t *testing.T) {
	srv := newPerpServer(nil) // perp client not wired
	req := httptest.NewRequest(http.MethodGet, "/v1/perp/margin", nil)
	req.Header.Set(auth.HeaderUserID, "1001")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled perp should 503, got %d", rr.Code)
	}
}
