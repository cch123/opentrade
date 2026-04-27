package client

import (
	"context"
	"fmt"
	"sync"

	"connectrpc.com/connect"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/shard"
)

// ShardedCounter implements Counter by routing each call to the shard
// owning the request's UserId (ADR-0010 / ADR-0027).
//
// Construction is order-sensitive: clients[i] must be the shard whose
// shard_id == i under the cluster's totalShards. BFF reads the endpoint
// list left-to-right and dials them in order, so main.go controls this
// alignment.
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

// pick returns the client owning userID. Panics if userID is empty —
// BFF handlers always resolve auth before calling, so an empty id here
// is a bug in the handler chain.
func (s *ShardedCounter) pick(userID string) Counter {
	if userID == "" {
		panic("sharded counter: empty user id — auth middleware missed a request")
	}
	return s.clients[shard.Index(userID, len(s.clients))]
}

// PlaceOrder dispatches to the user's shard.
func (s *ShardedCounter) PlaceOrder(ctx context.Context, req *connect.Request[counterrpc.PlaceOrderRequest]) (*connect.Response[counterrpc.PlaceOrderResponse], error) {
	return s.pick(req.Msg.UserId).PlaceOrder(ctx, req)
}

// CancelOrder dispatches to the user's shard.
func (s *ShardedCounter) CancelOrder(ctx context.Context, req *connect.Request[counterrpc.CancelOrderRequest]) (*connect.Response[counterrpc.CancelOrderResponse], error) {
	return s.pick(req.Msg.UserId).CancelOrder(ctx, req)
}

// QueryOrder dispatches to the user's shard.
func (s *ShardedCounter) QueryOrder(ctx context.Context, req *connect.Request[counterrpc.QueryOrderRequest]) (*connect.Response[counterrpc.QueryOrderResponse], error) {
	return s.pick(req.Msg.UserId).QueryOrder(ctx, req)
}

// QueryBalance dispatches to the user's shard.
func (s *ShardedCounter) QueryBalance(ctx context.Context, req *connect.Request[counterrpc.QueryBalanceRequest]) (*connect.Response[counterrpc.QueryBalanceResponse], error) {
	return s.pick(req.Msg.UserId).QueryBalance(ctx, req)
}

// Reserve dispatches to the user's shard.
func (s *ShardedCounter) Reserve(ctx context.Context, req *connect.Request[counterrpc.ReserveRequest]) (*connect.Response[counterrpc.ReserveResponse], error) {
	return s.pick(req.Msg.UserId).Reserve(ctx, req)
}

// ReleaseReservation dispatches to the user's shard.
func (s *ShardedCounter) ReleaseReservation(ctx context.Context, req *connect.Request[counterrpc.ReleaseReservationRequest]) (*connect.Response[counterrpc.ReleaseReservationResponse], error) {
	return s.pick(req.Msg.UserId).ReleaseReservation(ctx, req)
}

// AdminCancelOrders dispatches admin batch-cancel. With user_id it goes
// to that user's single shard; without a user filter it fans out across
// every shard so a symbol-only cancel drains the book. The caller sees
// an aggregated response; per-shard counts are discarded (BFF handler
// adds them back from ShardResults).
func (s *ShardedCounter) AdminCancelOrders(ctx context.Context, req *connect.Request[counterrpc.AdminCancelOrdersRequest]) (*connect.Response[counterrpc.AdminCancelOrdersResponse], error) {
	if req.Msg.UserId != "" {
		return s.pick(req.Msg.UserId).AdminCancelOrders(ctx, req)
	}
	results, err := s.BroadcastAdminCancelOrders(ctx, req)
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
	return connect.NewResponse(agg), nil
}

// CancelMyOrders dispatches to the user's shard. Unlike
// AdminCancelOrders, there is no fan-out path: the RPC requires a
// user_id and a single shard owns every live order for that user.
func (s *ShardedCounter) CancelMyOrders(ctx context.Context, req *connect.Request[counterrpc.CancelMyOrdersRequest]) (*connect.Response[counterrpc.CancelMyOrdersResponse], error) {
	return s.pick(req.Msg.UserId).CancelMyOrders(ctx, req)
}

// BroadcastAdminCancelOrders calls every shard in parallel and returns
// the per-shard responses (unwrapped from connect.Response) in shard-id
// order. Shards that error return nil at their slot and their error
// gets surfaced via the accompanying error; this keeps partial-success
// visible to the caller.
func (s *ShardedCounter) BroadcastAdminCancelOrders(ctx context.Context, req *connect.Request[counterrpc.AdminCancelOrdersRequest]) ([]*counterrpc.AdminCancelOrdersResponse, error) {
	out := make([]*counterrpc.AdminCancelOrdersResponse, len(s.clients))
	errs := make([]error, len(s.clients))
	var wg sync.WaitGroup
	wg.Add(len(s.clients))
	for i, c := range s.clients {
		i, c := i, c
		go func() {
			defer wg.Done()
			resp, err := c.AdminCancelOrders(ctx, req)
			if err != nil {
				errs[i] = err
				return
			}
			out[i] = resp.Msg
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
