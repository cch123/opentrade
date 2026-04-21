package client

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/shard"
)

// fakeCounter records which shard was invoked.
type fakeCounter struct {
	id          int
	placeHits   int
	cancelHits  int
	queryHits   int
	balanceHits int
	// adminCancelResult is returned from AdminCancelOrders. Tests set it
	// per-shard to exercise fan-out aggregation.
	adminCancelResult *counterrpc.AdminCancelOrdersResponse
	adminCancelErr    error
	adminCancelHits   int
	myCancelHits      int
	lastMyCancelReq   *counterrpc.CancelMyOrdersRequest
}

func (f *fakeCounter) PlaceOrder(ctx context.Context, in *counterrpc.PlaceOrderRequest, opts ...grpc.CallOption) (*counterrpc.PlaceOrderResponse, error) {
	f.placeHits++
	return &counterrpc.PlaceOrderResponse{OrderId: uint64(f.id)}, nil
}

func (f *fakeCounter) CancelOrder(ctx context.Context, in *counterrpc.CancelOrderRequest, opts ...grpc.CallOption) (*counterrpc.CancelOrderResponse, error) {
	f.cancelHits++
	return &counterrpc.CancelOrderResponse{OrderId: in.OrderId}, nil
}

func (f *fakeCounter) QueryOrder(ctx context.Context, in *counterrpc.QueryOrderRequest, opts ...grpc.CallOption) (*counterrpc.QueryOrderResponse, error) {
	f.queryHits++
	return &counterrpc.QueryOrderResponse{OrderId: in.OrderId}, nil
}

func (f *fakeCounter) QueryBalance(ctx context.Context, in *counterrpc.QueryBalanceRequest, opts ...grpc.CallOption) (*counterrpc.QueryBalanceResponse, error) {
	f.balanceHits++
	return &counterrpc.QueryBalanceResponse{}, nil
}

func (f *fakeCounter) AdminCancelOrders(ctx context.Context, in *counterrpc.AdminCancelOrdersRequest, opts ...grpc.CallOption) (*counterrpc.AdminCancelOrdersResponse, error) {
	f.adminCancelHits++
	if f.adminCancelErr != nil {
		return nil, f.adminCancelErr
	}
	if f.adminCancelResult != nil {
		return f.adminCancelResult, nil
	}
	return &counterrpc.AdminCancelOrdersResponse{ShardId: int32(f.id)}, nil
}

func (f *fakeCounter) CancelMyOrders(ctx context.Context, in *counterrpc.CancelMyOrdersRequest, opts ...grpc.CallOption) (*counterrpc.CancelMyOrdersResponse, error) {
	f.myCancelHits++
	f.lastMyCancelReq = in
	return &counterrpc.CancelMyOrdersResponse{}, nil
}

func TestNewSharded_RejectsEmptyAndNil(t *testing.T) {
	if _, err := NewSharded(nil); err == nil {
		t.Error("nil slice should fail")
	}
	if _, err := NewSharded([]Counter{nil}); err == nil {
		t.Error("nil element should fail")
	}
}

func TestShardedCounter_RoutesByUserID(t *testing.T) {
	const total = 10
	shards := make([]*fakeCounter, total)
	routes := make([]Counter, total)
	for i := 0; i < total; i++ {
		shards[i] = &fakeCounter{id: i}
		routes[i] = shards[i]
	}
	sc, err := NewSharded(routes)
	if err != nil {
		t.Fatalf("NewSharded: %v", err)
	}
	if sc.Shards() != total {
		t.Errorf("Shards(): %d", sc.Shards())
	}

	users := []string{"alice", "bob", "carol", "dave", "eve"}
	for _, u := range users {
		expected := shard.Index(u, total)
		before := shards[expected].placeHits
		if _, err := sc.PlaceOrder(context.Background(),
			&counterrpc.PlaceOrderRequest{UserId: u}); err != nil {
			t.Fatalf("PlaceOrder(%s): %v", u, err)
		}
		if shards[expected].placeHits != before+1 {
			t.Errorf("user %q: expected shard %d to get the hit (was %d, now %d)",
				u, expected, before, shards[expected].placeHits)
		}
	}
}

func TestShardedCounter_AllMethodsDispatch(t *testing.T) {
	shards := []Counter{&fakeCounter{}, &fakeCounter{}}
	sc, _ := NewSharded(shards)
	ctx := context.Background()
	userID := "someone"
	expected := shard.Index(userID, 2)
	owner := shards[expected].(*fakeCounter)

	_, _ = sc.PlaceOrder(ctx, &counterrpc.PlaceOrderRequest{UserId: userID})
	_, _ = sc.CancelOrder(ctx, &counterrpc.CancelOrderRequest{UserId: userID})
	_, _ = sc.QueryOrder(ctx, &counterrpc.QueryOrderRequest{UserId: userID})
	_, _ = sc.QueryBalance(ctx, &counterrpc.QueryBalanceRequest{UserId: userID})

	if owner.placeHits != 1 || owner.cancelHits != 1 || owner.queryHits != 1 ||
		owner.balanceHits != 1 {
		t.Errorf("not every method routed to owner: %+v", owner)
	}
}

func TestShardedCounter_EmptyUserIDPanics(t *testing.T) {
	sc, _ := NewSharded([]Counter{&fakeCounter{}})
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on empty user id")
		}
	}()
	_, _ = sc.PlaceOrder(context.Background(), &counterrpc.PlaceOrderRequest{UserId: ""})
}

// Ensure ShardedCounter satisfies the Counter interface.
var _ Counter = (*ShardedCounter)(nil)

// Pacify unused import warnings when running with -run-none.
var _ = errors.New

func TestShardedCounter_AdminCancelByUserRoutesToOneShard(t *testing.T) {
	shards := []Counter{&fakeCounter{id: 0}, &fakeCounter{id: 1}}
	sc, _ := NewSharded(shards)
	userID := "ada"
	expected := shard.Index(userID, 2)
	_, err := sc.AdminCancelOrders(context.Background(), &counterrpc.AdminCancelOrdersRequest{UserId: userID, Symbol: "BTC-USDT"})
	if err != nil {
		t.Fatalf("admin cancel: %v", err)
	}
	owner := shards[expected].(*fakeCounter)
	other := shards[1-expected].(*fakeCounter)
	if owner.adminCancelHits != 1 || other.adminCancelHits != 0 {
		t.Fatalf("hits: owner=%d other=%d", owner.adminCancelHits, other.adminCancelHits)
	}
}

func TestShardedCounter_AdminCancelBySymbolFansOut(t *testing.T) {
	f0 := &fakeCounter{id: 0, adminCancelResult: &counterrpc.AdminCancelOrdersResponse{Cancelled: 3, Skipped: 1, ShardId: 0}}
	f1 := &fakeCounter{id: 1, adminCancelResult: &counterrpc.AdminCancelOrdersResponse{Cancelled: 5, Skipped: 0, ShardId: 1}}
	sc, _ := NewSharded([]Counter{f0, f1})

	agg, err := sc.AdminCancelOrders(context.Background(), &counterrpc.AdminCancelOrdersRequest{Symbol: "BTC-USDT"})
	if err != nil {
		t.Fatalf("admin cancel: %v", err)
	}
	if agg.Cancelled != 8 || agg.Skipped != 1 {
		t.Fatalf("agg = %+v", agg)
	}
	if f0.adminCancelHits != 1 || f1.adminCancelHits != 1 {
		t.Fatalf("fan-out miss: f0=%d f1=%d", f0.adminCancelHits, f1.adminCancelHits)
	}
}

func TestShardedCounter_BroadcastSurfacesFirstError(t *testing.T) {
	f0 := &fakeCounter{id: 0, adminCancelErr: errors.New("shard 0 down")}
	f1 := &fakeCounter{id: 1, adminCancelResult: &counterrpc.AdminCancelOrdersResponse{Cancelled: 2}}
	sc, _ := NewSharded([]Counter{f0, f1})
	results, err := sc.BroadcastAdminCancelOrders(context.Background(), &counterrpc.AdminCancelOrdersRequest{Symbol: "X"})
	if err == nil {
		t.Fatal("expected error")
	}
	if len(results) != 2 {
		t.Fatalf("results len = %d", len(results))
	}
	if results[0] != nil {
		t.Errorf("errored shard must be nil, got %+v", results[0])
	}
	if results[1] == nil || results[1].Cancelled != 2 {
		t.Errorf("ok shard: %+v", results[1])
	}
}
