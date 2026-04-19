package counterclient

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
)

type fakeCounter struct {
	id     int
	hits   int
	result *counterrpc.AdminCancelOrdersResponse
	err    error
}

func (f *fakeCounter) AdminCancelOrders(_ context.Context, _ *counterrpc.AdminCancelOrdersRequest, _ ...grpc.CallOption) (*counterrpc.AdminCancelOrdersResponse, error) {
	f.hits++
	if f.err != nil {
		return nil, f.err
	}
	if f.result != nil {
		return f.result, nil
	}
	return &counterrpc.AdminCancelOrdersResponse{ShardId: int32(f.id)}, nil
}

func TestNewSharded_Validation(t *testing.T) {
	if _, err := NewSharded(nil); err == nil {
		t.Error("empty list accepted")
	}
	if _, err := NewSharded([]Counter{nil}); err == nil {
		t.Error("nil entry accepted")
	}
}

func TestBroadcastAggregatesAndSurfacesFirstError(t *testing.T) {
	f0 := &fakeCounter{id: 0, err: errors.New("shard 0 down")}
	f1 := &fakeCounter{id: 1, result: &counterrpc.AdminCancelOrdersResponse{Cancelled: 5, Skipped: 1, ShardId: 1}}
	f2 := &fakeCounter{id: 2, result: &counterrpc.AdminCancelOrdersResponse{Cancelled: 3, Skipped: 0, ShardId: 2}}
	s, _ := NewSharded([]Counter{f0, f1, f2})

	results, err := s.BroadcastAdminCancelOrders(context.Background(), &counterrpc.AdminCancelOrdersRequest{Symbol: "BTC-USDT"})
	if err == nil {
		t.Fatal("expected first error")
	}
	if len(results) != 3 {
		t.Fatalf("len = %d", len(results))
	}
	if results[0] != nil {
		t.Errorf("errored shard must be nil")
	}
	if results[1] == nil || results[1].Cancelled != 5 {
		t.Errorf("shard 1: %+v", results[1])
	}
	if results[2] == nil || results[2].Cancelled != 3 {
		t.Errorf("shard 2: %+v", results[2])
	}
	if f0.hits != 1 || f1.hits != 1 || f2.hits != 1 {
		t.Errorf("fan-out hits: %d %d %d", f0.hits, f1.hits, f2.hits)
	}
}

func TestShard_ReturnsOrderedClients(t *testing.T) {
	a := &fakeCounter{id: 0}
	b := &fakeCounter{id: 1}
	s, _ := NewSharded([]Counter{a, b})
	if s.Shards() != 2 {
		t.Fatalf("Shards() = %d", s.Shards())
	}
	if s.Shard(0) != a || s.Shard(1) != b {
		t.Fatal("ordering wrong")
	}
}
