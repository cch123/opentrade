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

	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	"github.com/xargin/opentrade/pkg/auth"
)

// fakeConditional lets tests drive the server without a live gRPC service.
type fakeConditional struct {
	placeFn    func(*condrpc.PlaceConditionalRequest) (*condrpc.PlaceConditionalResponse, error)
	cancelFn   func(*condrpc.CancelConditionalRequest) (*condrpc.CancelConditionalResponse, error)
	queryFn    func(*condrpc.QueryConditionalRequest) (*condrpc.QueryConditionalResponse, error)
	listFn     func(*condrpc.ListConditionalsRequest) (*condrpc.ListConditionalsResponse, error)
	placeOCOFn func(*condrpc.PlaceOCORequest) (*condrpc.PlaceOCOResponse, error)
}

func (f *fakeConditional) PlaceConditional(_ context.Context, req *condrpc.PlaceConditionalRequest, _ ...grpc.CallOption) (*condrpc.PlaceConditionalResponse, error) {
	return f.placeFn(req)
}
func (f *fakeConditional) CancelConditional(_ context.Context, req *condrpc.CancelConditionalRequest, _ ...grpc.CallOption) (*condrpc.CancelConditionalResponse, error) {
	return f.cancelFn(req)
}
func (f *fakeConditional) QueryConditional(_ context.Context, req *condrpc.QueryConditionalRequest, _ ...grpc.CallOption) (*condrpc.QueryConditionalResponse, error) {
	return f.queryFn(req)
}
func (f *fakeConditional) ListConditionals(_ context.Context, req *condrpc.ListConditionalsRequest, _ ...grpc.CallOption) (*condrpc.ListConditionalsResponse, error) {
	return f.listFn(req)
}
func (f *fakeConditional) PlaceOCO(_ context.Context, req *condrpc.PlaceOCORequest, _ ...grpc.CallOption) (*condrpc.PlaceOCOResponse, error) {
	return f.placeOCOFn(req)
}

func newCondServer(fc *fakeConditional) *Server {
	return NewServer(Config{
		UserRateLimit: 1000, UserRateWindow: time.Second,
		IPRateLimit: 1000, IPRateWindow: time.Second,
	}, &fakeCounter{}, nil, nil, fc, nil, zap.NewNop())
}

func TestPlaceConditional_503WhenUnconfigured(t *testing.T) {
	srv := NewServer(Config{
		UserRateLimit: 1000, UserRateWindow: time.Second,
		IPRateLimit: 1000, IPRateWindow: time.Second,
	}, &fakeCounter{}, nil, nil, nil, nil, zap.NewNop())
	req := httptest.NewRequest(http.MethodPost, "/v1/conditional",
		bytes.NewBufferString(`{"symbol":"BTC-USDT","side":"sell","type":"stop_loss","stop_price":"100","qty":"0.5"}`))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", rr.Code)
	}
}

func TestPlaceConditional_ForwardsToService(t *testing.T) {
	var seen *condrpc.PlaceConditionalRequest
	fc := &fakeConditional{
		placeFn: func(req *condrpc.PlaceConditionalRequest) (*condrpc.PlaceConditionalResponse, error) {
			seen = req
			return &condrpc.PlaceConditionalResponse{
				Id:       42,
				Status:   condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING,
				Accepted: true,
			}, nil
		},
	}
	srv := newCondServer(fc)
	body := `{"symbol":"BTC-USDT","side":"sell","type":"stop_loss","stop_price":"100","qty":"0.5"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/conditional", bytes.NewBufferString(body))
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
	if seen.Type != condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS {
		t.Errorf("type: %v", seen.Type)
	}
}

func TestPlaceConditional_LimitTypeShape(t *testing.T) {
	var seen *condrpc.PlaceConditionalRequest
	fc := &fakeConditional{
		placeFn: func(req *condrpc.PlaceConditionalRequest) (*condrpc.PlaceConditionalResponse, error) {
			seen = req
			return &condrpc.PlaceConditionalResponse{Id: 1, Accepted: true}, nil
		},
	}
	srv := newCondServer(fc)
	body := `{"symbol":"BTC-USDT","side":"buy","type":"take_profit_limit","stop_price":"100","limit_price":"99.5","qty":"1","tif":"ioc"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/conditional", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	if seen.Type != condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT {
		t.Errorf("type: %v", seen.Type)
	}
	if seen.LimitPrice != "99.5" {
		t.Errorf("limit_price: %q", seen.LimitPrice)
	}
}

func TestPlaceConditional_BadTypeRejectedAtBFF(t *testing.T) {
	srv := newCondServer(&fakeConditional{})
	body := `{"symbol":"BTC-USDT","side":"sell","type":"nonsense","stop_price":"100","qty":"1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/conditional", bytes.NewBufferString(body))
	req.Header.Set(auth.HeaderUserID, "u1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code = %d", rr.Code)
	}
}

func TestCancelConditional_ForwardsToService(t *testing.T) {
	fc := &fakeConditional{
		cancelFn: func(req *condrpc.CancelConditionalRequest) (*condrpc.CancelConditionalResponse, error) {
			if req.Id != 7 || req.UserId != "u1" {
				t.Fatalf("unexpected: %+v", req)
			}
			return &condrpc.CancelConditionalResponse{Id: 7, Accepted: true,
				Status: condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED}, nil
		},
	}
	srv := newCondServer(fc)
	req := httptest.NewRequest(http.MethodDelete, "/v1/conditional/7", nil)
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

func TestListConditionals_IncludeInactiveFlag(t *testing.T) {
	var seen *condrpc.ListConditionalsRequest
	fc := &fakeConditional{
		listFn: func(req *condrpc.ListConditionalsRequest) (*condrpc.ListConditionalsResponse, error) {
			seen = req
			return &condrpc.ListConditionalsResponse{}, nil
		},
	}
	srv := newCondServer(fc)
	req := httptest.NewRequest(http.MethodGet, "/v1/conditional?include_inactive=true", nil)
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
