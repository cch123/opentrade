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
	id        int
	placeHits int
	cancelHits int
	queryHits  int
	xferHits   int
	balanceHits int
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

func (f *fakeCounter) Transfer(ctx context.Context, in *counterrpc.TransferRequest, opts ...grpc.CallOption) (*counterrpc.TransferResponse, error) {
	f.xferHits++
	return &counterrpc.TransferResponse{}, nil
}

func (f *fakeCounter) QueryBalance(ctx context.Context, in *counterrpc.QueryBalanceRequest, opts ...grpc.CallOption) (*counterrpc.QueryBalanceResponse, error) {
	f.balanceHits++
	return &counterrpc.QueryBalanceResponse{}, nil
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
	_, _ = sc.Transfer(ctx, &counterrpc.TransferRequest{UserId: userID})
	_, _ = sc.QueryBalance(ctx, &counterrpc.QueryBalanceRequest{UserId: userID})

	if owner.placeHits != 1 || owner.cancelHits != 1 || owner.queryHits != 1 ||
		owner.xferHits != 1 || owner.balanceHits != 1 {
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
