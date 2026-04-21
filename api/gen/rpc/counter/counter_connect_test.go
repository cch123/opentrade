package counterrpc

import (
	"context"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testCounterService struct {
	UnimplementedCounterServiceServer
	lastPlaceOrder *PlaceOrderRequest
	placeOrderErr  error
}

func (s *testCounterService) PlaceOrder(_ context.Context, req *PlaceOrderRequest) (*PlaceOrderResponse, error) {
	if s.placeOrderErr != nil {
		return nil, s.placeOrderErr
	}
	s.lastPlaceOrder = req
	return &PlaceOrderResponse{OrderId: 42, Accepted: true}, nil
}

func TestCounterServiceConnectClientRoundTrip(t *testing.T) {
	t.Parallel()

	svc := &testCounterService{}
	ts := httptest.NewServer(NewCounterServiceHTTPHandler(svc))
	defer ts.Close()

	client := NewCounterServiceConnectClient(ts.Client(), ts.URL)
	resp, err := client.PlaceOrder(context.Background(), &PlaceOrderRequest{UserId: "u-1", Symbol: "BTCUSDT"})
	if err != nil {
		t.Fatalf("PlaceOrder error = %v", err)
	}
	if resp.GetOrderId() != 42 || !resp.GetAccepted() {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if svc.lastPlaceOrder == nil || svc.lastPlaceOrder.GetUserId() != "u-1" || svc.lastPlaceOrder.GetSymbol() != "BTCUSDT" {
		t.Fatalf("server saw unexpected request: %+v", svc.lastPlaceOrder)
	}
}

func TestCounterServiceConnectClientPreservesGRPCStatus(t *testing.T) {
	t.Parallel()

	svc := &testCounterService{placeOrderErr: status.Error(codes.InvalidArgument, "bad order")}
	ts := httptest.NewServer(NewCounterServiceHTTPHandler(svc))
	defer ts.Close()

	client := NewCounterServiceConnectClient(ts.Client(), ts.URL)
	_, err := client.PlaceOrder(context.Background(), &PlaceOrderRequest{UserId: "u-1"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status.Code(err) = %v, want %v (err=%v)", status.Code(err), codes.InvalidArgument, err)
	}
}
