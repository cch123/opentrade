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

	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	"github.com/xargin/opentrade/pkg/auth"
)

// fakeTrigger lets tests drive the server without a live trigger service.
type fakeTrigger struct {
	placeFn    func(*condrpc.PlaceTriggerRequest) (*condrpc.PlaceTriggerResponse, error)
	cancelFn   func(*condrpc.CancelTriggerRequest) (*condrpc.CancelTriggerResponse, error)
	queryFn    func(*condrpc.QueryTriggerRequest) (*condrpc.QueryTriggerResponse, error)
	listFn     func(*condrpc.ListTriggersRequest) (*condrpc.ListTriggersResponse, error)
	placeOCOFn func(*condrpc.PlaceOCORequest) (*condrpc.PlaceOCOResponse, error)
}

func (f *fakeTrigger) PlaceTrigger(_ context.Context, req *connect.Request[condrpc.PlaceTriggerRequest]) (*connect.Response[condrpc.PlaceTriggerResponse], error) {
	resp, err := f.placeFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeTrigger) CancelTrigger(_ context.Context, req *connect.Request[condrpc.CancelTriggerRequest]) (*connect.Response[condrpc.CancelTriggerResponse], error) {
	resp, err := f.cancelFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeTrigger) QueryTrigger(_ context.Context, req *connect.Request[condrpc.QueryTriggerRequest]) (*connect.Response[condrpc.QueryTriggerResponse], error) {
	resp, err := f.queryFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeTrigger) ListTriggers(_ context.Context, req *connect.Request[condrpc.ListTriggersRequest]) (*connect.Response[condrpc.ListTriggersResponse], error) {
	resp, err := f.listFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
func (f *fakeTrigger) PlaceOCO(_ context.Context, req *connect.Request[condrpc.PlaceOCORequest]) (*connect.Response[condrpc.PlaceOCOResponse], error) {
	resp, err := f.placeOCOFn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func newCondServer(fc *fakeTrigger) *Server {
	return NewServer(Config{
		UserRateLimit: 1000, UserRateWindow: time.Second,
		IPRateLimit: 1000, IPRateWindow: time.Second,
	}, &fakeCounter{}, nil, nil, fc, nil, zap.NewNop())
}

func TestPlaceTrigger_503WhenUnconfigured(t *testing.T) {
	srv := NewServer(Config{
		UserRateLimit: 1000, UserRateWindow: time.Second,
		IPRateLimit: 1000, IPRateWindow: time.Second,
	}, &fakeCounter{}, nil, nil, nil, nil, zap.NewNop())
	req := httptest.NewRequest(http.MethodPost, "/v1/trigger",
		bytes.NewBufferString(`{"symbol":"BTC-USDT","side":"sell","type":"stop_loss","stop_price":"100","qty":"0.5"}`))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", rr.Code)
	}
}

func TestPlaceTrigger_ForwardsToService(t *testing.T) {
	var seen *condrpc.PlaceTriggerRequest
	fc := &fakeTrigger{
		placeFn: func(req *condrpc.PlaceTriggerRequest) (*condrpc.PlaceTriggerResponse, error) {
			seen = req
			return &condrpc.PlaceTriggerResponse{
				Id:       42,
				Status:   condrpc.TriggerStatus_TRIGGER_STATUS_PENDING,
				Accepted: true,
			}, nil
		},
	}
	srv := newCondServer(fc)
	body := `{"symbol":"BTC-USDT","side":"sell","type":"stop_loss","stop_price":"100","qty":"0.5"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/trigger", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &got)
	if got["status"] != "pending" {
		t.Errorf("status: %v", got["status"])
	}
	if got["accepted"] != true {
		t.Errorf("accepted: %v", got["accepted"])
	}
	if seen == nil || seen.UserId != "u1" || seen.StopPrice != "100" {
		t.Fatalf("forwarded: %+v", seen)
	}
	if seen.Type != condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS {
		t.Errorf("type: %v", seen.Type)
	}
}

func TestPlaceTrigger_LimitTypeShape(t *testing.T) {
	var seen *condrpc.PlaceTriggerRequest
	fc := &fakeTrigger{
		placeFn: func(req *condrpc.PlaceTriggerRequest) (*condrpc.PlaceTriggerResponse, error) {
			seen = req
			return &condrpc.PlaceTriggerResponse{Id: 1, Accepted: true}, nil
		},
	}
	srv := newCondServer(fc)
	body := `{"symbol":"BTC-USDT","side":"buy","type":"take_profit_limit","stop_price":"100","limit_price":"99.5","qty":"1","tif":"ioc"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/trigger", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	if seen.Type != condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT_LIMIT {
		t.Errorf("type: %v", seen.Type)
	}
	if seen.LimitPrice != "99.5" {
		t.Errorf("limit_price: %q", seen.LimitPrice)
	}
}

func TestPlaceTrigger_BadTypeRejectedAtBFF(t *testing.T) {
	srv := newCondServer(&fakeTrigger{})
	body := `{"symbol":"BTC-USDT","side":"sell","type":"nonsense","stop_price":"100","qty":"1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/trigger", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rr.Code)
	}
}

func TestCancelTrigger_ForwardsToService(t *testing.T) {
	fc := &fakeTrigger{
		cancelFn: func(req *condrpc.CancelTriggerRequest) (*condrpc.CancelTriggerResponse, error) {
			if req.Id != 7 || req.UserId != "u1" {
				t.Fatalf("unexpected: %+v", req)
			}
			return &condrpc.CancelTriggerResponse{Id: 7, Accepted: true,
				Status: condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED}, nil
		},
	}
	srv := newCondServer(fc)
	req := httptest.NewRequest(http.MethodDelete, "/v1/trigger/7", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &got)
	if got["status"] != "canceled" {
		t.Errorf("status: %v", got["status"])
	}
}

func TestListTriggers_IncludeInactiveFlag(t *testing.T) {
	var seen *condrpc.ListTriggersRequest
	fc := &fakeTrigger{
		listFn: func(req *condrpc.ListTriggersRequest) (*condrpc.ListTriggersResponse, error) {
			seen = req
			return &condrpc.ListTriggersResponse{}, nil
		},
	}
	srv := newCondServer(fc)
	req := httptest.NewRequest(http.MethodGet, "/v1/trigger?include_inactive=true", nil)
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d", rr.Code)
	}
	if !seen.IncludeInactive {
		t.Error("include_inactive flag not forwarded")
	}
}
