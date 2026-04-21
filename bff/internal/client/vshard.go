package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/bff/internal/clusterview"
)

// VShardCounter is the ADR-0058 replacement for ShardedCounter: it
// routes each RPC to whichever node currently owns the user's vshard,
// using the BFF's etcd watcher (clusterview.Watcher). On a
// FailedPrecondition reply (counter says "not my vshard any more") it
// triggers a watcher resync and retries exactly once — that covers the
// 1-5s migration window where old owner has stepped down but this BFF
// hasn't caught the update yet.
//
// Connections are lazily dialled per node endpoint and cached for the
// life of the BFF process. Nodes that disappear still hold an open
// conn until process restart; that leak is bounded by the cluster
// size and fine for the MVP.
type VShardCounter struct {
	watcher *clusterview.Watcher

	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn
	clients map[string]counterrpc.CounterServiceClient
}

// NewVShardCounter wraps a running Watcher. Call watcher.Run separately
// — this constructor doesn't start anything.
func NewVShardCounter(watcher *clusterview.Watcher) (*VShardCounter, error) {
	if watcher == nil {
		return nil, errors.New("vshard counter: watcher required")
	}
	return &VShardCounter{
		watcher: watcher,
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]counterrpc.CounterServiceClient),
	}, nil
}

// Close terminates every cached gRPC connection. After Close the
// VShardCounter must not receive more dispatch calls.
func (v *VShardCounter) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	var firstErr error
	for _, c := range v.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	v.conns = nil
	v.clients = nil
	return firstErr
}

// clientFor returns the cached client for endpoint, dialling lazily.
func (v *VShardCounter) clientFor(endpoint string) (counterrpc.CounterServiceClient, error) {
	v.mu.RLock()
	if c, ok := v.clients[endpoint]; ok {
		v.mu.RUnlock()
		return c, nil
	}
	v.mu.RUnlock()

	v.mu.Lock()
	defer v.mu.Unlock()
	if c, ok := v.clients[endpoint]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", endpoint, err)
	}
	client := counterrpc.NewCounterServiceClient(conn)
	v.conns[endpoint] = conn
	v.clients[endpoint] = client
	return client, nil
}

// dispatch resolves userID → endpoint → client, then invokes fn. On
// FailedPrecondition (counter node tells us we're on the wrong vshard)
// it kicks the watcher to refresh and retries fn exactly once.
func (v *VShardCounter) dispatch(
	userID string,
	fn func(counterrpc.CounterServiceClient) error,
) error {
	if userID == "" {
		return status.Error(codes.InvalidArgument, "user_id required")
	}
	for attempt := 0; attempt < 2; attempt++ {
		endpoint, ok := v.watcher.Lookup(userID)
		if !ok {
			if attempt == 0 {
				v.watcher.Resync()
				continue
			}
			return status.Errorf(codes.FailedPrecondition,
				"no active vshard owner for user")
		}
		cli, err := v.clientFor(endpoint)
		if err != nil {
			return err
		}
		err = fn(cli)
		if err == nil {
			return nil
		}
		// Epoch-mismatch / migration handoff window: node says it
		// doesn't own this vshard any more. Refresh + retry once.
		if attempt == 0 && status.Code(err) == codes.FailedPrecondition {
			v.watcher.Resync()
			continue
		}
		return err
	}
	return status.Error(codes.Internal, "vshard dispatch exhausted retries")
}

// --- Counter interface implementations ---

func (v *VShardCounter) PlaceOrder(ctx context.Context, in *counterrpc.PlaceOrderRequest, opts ...grpc.CallOption) (*counterrpc.PlaceOrderResponse, error) {
	var resp *counterrpc.PlaceOrderResponse
	err := v.dispatch(in.UserId, func(c counterrpc.CounterServiceClient) error {
		r, err := c.PlaceOrder(ctx, in, opts...)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) CancelOrder(ctx context.Context, in *counterrpc.CancelOrderRequest, opts ...grpc.CallOption) (*counterrpc.CancelOrderResponse, error) {
	var resp *counterrpc.CancelOrderResponse
	err := v.dispatch(in.UserId, func(c counterrpc.CounterServiceClient) error {
		r, err := c.CancelOrder(ctx, in, opts...)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) QueryOrder(ctx context.Context, in *counterrpc.QueryOrderRequest, opts ...grpc.CallOption) (*counterrpc.QueryOrderResponse, error) {
	var resp *counterrpc.QueryOrderResponse
	err := v.dispatch(in.UserId, func(c counterrpc.CounterServiceClient) error {
		r, err := c.QueryOrder(ctx, in, opts...)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) QueryBalance(ctx context.Context, in *counterrpc.QueryBalanceRequest, opts ...grpc.CallOption) (*counterrpc.QueryBalanceResponse, error) {
	var resp *counterrpc.QueryBalanceResponse
	err := v.dispatch(in.UserId, func(c counterrpc.CounterServiceClient) error {
		r, err := c.QueryBalance(ctx, in, opts...)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

// AdminCancelOrders under vshard routing requires a UserId — the counter
// side (ADR-0058) no longer supports symbol-only fan-out. Callers that
// need a cluster-wide sweep should drive it from an operator tool.
func (v *VShardCounter) AdminCancelOrders(ctx context.Context, in *counterrpc.AdminCancelOrdersRequest, opts ...grpc.CallOption) (*counterrpc.AdminCancelOrdersResponse, error) {
	if in.UserId == "" {
		return nil, status.Error(codes.InvalidArgument,
			"user_id required under vshard routing (symbol-only fan-out moves to operator tooling)")
	}
	var resp *counterrpc.AdminCancelOrdersResponse
	err := v.dispatch(in.UserId, func(c counterrpc.CounterServiceClient) error {
		r, err := c.AdminCancelOrders(ctx, in, opts...)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) CancelMyOrders(ctx context.Context, in *counterrpc.CancelMyOrdersRequest, opts ...grpc.CallOption) (*counterrpc.CancelMyOrdersResponse, error) {
	var resp *counterrpc.CancelMyOrdersResponse
	err := v.dispatch(in.UserId, func(c counterrpc.CounterServiceClient) error {
		r, err := c.CancelMyOrders(ctx, in, opts...)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}
