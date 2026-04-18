package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/shard"
)

// ShardedCounter implements Counter by routing each call to the shard owning
// the request's UserId (ADR-0010 / ADR-0027).
//
// Construction is order-sensitive: clients[i] must be the shard whose
// shard_id == i under the cluster's totalShards. BFF reads the endpoint list
// left-to-right and dials them in order, so main.go controls this alignment.
type ShardedCounter struct {
	clients []Counter
}

// NewSharded wraps an ordered list of per-shard Counter clients. Empty
// clients is invalid.
func NewSharded(clients []Counter) (*ShardedCounter, error) {
	if len(clients) == 0 {
		return nil, fmt.Errorf("sharded counter: at least one shard required")
	}
	for i, c := range clients {
		if c == nil {
			return nil, fmt.Errorf("sharded counter: shard %d is nil", i)
		}
	}
	return &ShardedCounter{clients: clients}, nil
}

// Shards returns the configured shard count.
func (s *ShardedCounter) Shards() int { return len(s.clients) }

// pick returns the client owning userID. Panics if userID is empty — BFF
// handlers always resolve auth before calling, so an empty id here is a bug
// in the handler chain.
func (s *ShardedCounter) pick(userID string) Counter {
	if userID == "" {
		panic("sharded counter: empty user id — auth middleware missed a request")
	}
	return s.clients[shard.Index(userID, len(s.clients))]
}

// PlaceOrder dispatches to the user's shard.
func (s *ShardedCounter) PlaceOrder(ctx context.Context, in *counterrpc.PlaceOrderRequest, opts ...grpc.CallOption) (*counterrpc.PlaceOrderResponse, error) {
	return s.pick(in.UserId).PlaceOrder(ctx, in, opts...)
}

// CancelOrder dispatches to the user's shard.
func (s *ShardedCounter) CancelOrder(ctx context.Context, in *counterrpc.CancelOrderRequest, opts ...grpc.CallOption) (*counterrpc.CancelOrderResponse, error) {
	return s.pick(in.UserId).CancelOrder(ctx, in, opts...)
}

// QueryOrder dispatches to the user's shard.
func (s *ShardedCounter) QueryOrder(ctx context.Context, in *counterrpc.QueryOrderRequest, opts ...grpc.CallOption) (*counterrpc.QueryOrderResponse, error) {
	return s.pick(in.UserId).QueryOrder(ctx, in, opts...)
}

// Transfer dispatches to the user's shard.
func (s *ShardedCounter) Transfer(ctx context.Context, in *counterrpc.TransferRequest, opts ...grpc.CallOption) (*counterrpc.TransferResponse, error) {
	return s.pick(in.UserId).Transfer(ctx, in, opts...)
}

// QueryBalance dispatches to the user's shard.
func (s *ShardedCounter) QueryBalance(ctx context.Context, in *counterrpc.QueryBalanceRequest, opts ...grpc.CallOption) (*counterrpc.QueryBalanceResponse, error) {
	return s.pick(in.UserId).QueryBalance(ctx, in, opts...)
}
