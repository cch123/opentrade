package client

import (
	"context"
	"fmt"
	"sync"

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

// AdminCancelOrders dispatches admin batch-cancel. With user_id it goes to
// that user's single shard; without a user filter it fans out across every
// shard so a symbol-only cancel drains the book. The caller sees an
// aggregated response; per-shard counts are discarded (BFF handler adds
// them back from ShardResults).
func (s *ShardedCounter) AdminCancelOrders(ctx context.Context, in *counterrpc.AdminCancelOrdersRequest, opts ...grpc.CallOption) (*counterrpc.AdminCancelOrdersResponse, error) {
	if in.UserId != "" {
		return s.pick(in.UserId).AdminCancelOrders(ctx, in, opts...)
	}
	results, err := s.BroadcastAdminCancelOrders(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	agg := &counterrpc.AdminCancelOrdersResponse{}
	for _, r := range results {
		if r == nil {
			continue
		}
		agg.Cancelled += r.Cancelled
		agg.Skipped += r.Skipped
	}
	// ShardId on the aggregate is meaningless; leave zero.
	return agg, nil
}

// CancelMyOrders dispatches to the user's shard. Unlike AdminCancelOrders,
// there is no fan-out path: the RPC requires a user_id and a single shard
// owns every live order for that user.
func (s *ShardedCounter) CancelMyOrders(ctx context.Context, in *counterrpc.CancelMyOrdersRequest, opts ...grpc.CallOption) (*counterrpc.CancelMyOrdersResponse, error) {
	return s.pick(in.UserId).CancelMyOrders(ctx, in, opts...)
}

// BroadcastAdminCancelOrders calls every shard in parallel and returns the
// per-shard responses in shard-id order. Shards that error return nil at
// their slot and their error gets surfaced via the accompanying error;
// this keeps partial-success visible to the caller.
func (s *ShardedCounter) BroadcastAdminCancelOrders(ctx context.Context, in *counterrpc.AdminCancelOrdersRequest, opts ...grpc.CallOption) ([]*counterrpc.AdminCancelOrdersResponse, error) {
	out := make([]*counterrpc.AdminCancelOrdersResponse, len(s.clients))
	errs := make([]error, len(s.clients))
	var wg sync.WaitGroup
	wg.Add(len(s.clients))
	for i, c := range s.clients {
		i, c := i, c
		go func() {
			defer wg.Done()
			resp, err := c.AdminCancelOrders(ctx, in, opts...)
			if err != nil {
				errs[i] = err
				return
			}
			out[i] = resp
		}()
	}
	wg.Wait()
	var first error
	for _, e := range errs {
		if e != nil {
			first = e
			break
		}
	}
	return out, first
}
