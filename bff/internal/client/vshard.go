package client

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"connectrpc.com/connect"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/api/gen/rpc/counter/counterrpcconnect"
	"github.com/xargin/opentrade/bff/internal/clusterview"
	"github.com/xargin/opentrade/pkg/connectx"
)

// VShardCounter is the ADR-0058 replacement for ShardedCounter: it
// routes each RPC to whichever node currently owns the user's vshard,
// using the BFF's etcd watcher (clusterview.Watcher). On a
// FailedPrecondition reply (counter says "not my vshard any more") it
// triggers a watcher resync and retries exactly once — that covers the
// 1-5s migration window where old owner has stepped down but this BFF
// hasn't caught the update yet.
//
// Connect clients are lazily constructed per node endpoint and cached
// for the life of the BFF process. All clients share one underlying
// h2c http.Client (its connection pool keys by host:port internally).
// Nodes that disappear leave their cached client behind until process
// restart; that leak is bounded by the cluster size and fine for MVP.
type VShardCounter struct {
	watcher *clusterview.Watcher

	httpClient *http.Client

	mu      sync.RWMutex
	clients map[string]counterrpcconnect.CounterServiceClient
}

// NewVShardCounter wraps a running Watcher. Call watcher.Run separately
// — this constructor doesn't start anything.
func NewVShardCounter(watcher *clusterview.Watcher) (*VShardCounter, error) {
	if watcher == nil {
		return nil, errors.New("vshard counter: watcher required")
	}
	return &VShardCounter{
		watcher:    watcher,
		httpClient: connectx.NewH2CClient(),
		clients:    make(map[string]counterrpcconnect.CounterServiceClient),
	}, nil
}

// Close drops idle h2c connections. After Close the VShardCounter must
// not receive more dispatch calls.
func (v *VShardCounter) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.httpClient != nil {
		v.httpClient.CloseIdleConnections()
	}
	v.clients = nil
	return nil
}

// clientFor returns the cached Connect client for endpoint, building
// it lazily.
func (v *VShardCounter) clientFor(endpoint string) (counterrpcconnect.CounterServiceClient, error) {
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
	client := counterrpcconnect.NewCounterServiceClient(
		v.httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	v.clients[endpoint] = client
	return client, nil
}

// dispatch resolves userID → endpoint → client, then invokes fn. On
// FailedPrecondition (counter node tells us we're on the wrong vshard)
// it kicks the watcher to refresh and retries fn exactly once.
func (v *VShardCounter) dispatch(
	userID string,
	fn func(counterrpcconnect.CounterServiceClient) error,
) error {
	if userID == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	for attempt := 0; attempt < 2; attempt++ {
		endpoint, ok := v.watcher.Lookup(userID)
		if !ok {
			if attempt == 0 {
				v.watcher.Resync()
				continue
			}
			return connect.NewError(connect.CodeFailedPrecondition,
				errors.New("no active vshard owner for user"))
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
		if attempt == 0 && connect.CodeOf(err) == connect.CodeFailedPrecondition {
			v.watcher.Resync()
			continue
		}
		return err
	}
	return connect.NewError(connect.CodeInternal, errors.New("vshard dispatch exhausted retries"))
}

// --- Counter interface implementations ---

func (v *VShardCounter) PlaceOrder(ctx context.Context, req *connect.Request[counterrpc.PlaceOrderRequest]) (*connect.Response[counterrpc.PlaceOrderResponse], error) {
	var resp *connect.Response[counterrpc.PlaceOrderResponse]
	err := v.dispatch(req.Msg.UserId, func(c counterrpcconnect.CounterServiceClient) error {
		r, err := c.PlaceOrder(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) CancelOrder(ctx context.Context, req *connect.Request[counterrpc.CancelOrderRequest]) (*connect.Response[counterrpc.CancelOrderResponse], error) {
	var resp *connect.Response[counterrpc.CancelOrderResponse]
	err := v.dispatch(req.Msg.UserId, func(c counterrpcconnect.CounterServiceClient) error {
		r, err := c.CancelOrder(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) QueryOrder(ctx context.Context, req *connect.Request[counterrpc.QueryOrderRequest]) (*connect.Response[counterrpc.QueryOrderResponse], error) {
	var resp *connect.Response[counterrpc.QueryOrderResponse]
	err := v.dispatch(req.Msg.UserId, func(c counterrpcconnect.CounterServiceClient) error {
		r, err := c.QueryOrder(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) QueryBalance(ctx context.Context, req *connect.Request[counterrpc.QueryBalanceRequest]) (*connect.Response[counterrpc.QueryBalanceResponse], error) {
	var resp *connect.Response[counterrpc.QueryBalanceResponse]
	err := v.dispatch(req.Msg.UserId, func(c counterrpcconnect.CounterServiceClient) error {
		r, err := c.QueryBalance(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) Reserve(ctx context.Context, req *connect.Request[counterrpc.ReserveRequest]) (*connect.Response[counterrpc.ReserveResponse], error) {
	var resp *connect.Response[counterrpc.ReserveResponse]
	err := v.dispatch(req.Msg.UserId, func(c counterrpcconnect.CounterServiceClient) error {
		r, err := c.Reserve(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) ReleaseReservation(ctx context.Context, req *connect.Request[counterrpc.ReleaseReservationRequest]) (*connect.Response[counterrpc.ReleaseReservationResponse], error) {
	var resp *connect.Response[counterrpc.ReleaseReservationResponse]
	err := v.dispatch(req.Msg.UserId, func(c counterrpcconnect.CounterServiceClient) error {
		r, err := c.ReleaseReservation(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

// AdminCancelOrders under vshard routing requires a UserId — the
// counter side (ADR-0058) no longer supports symbol-only fan-out.
// Callers that need a cluster-wide sweep should drive it from an
// operator tool.
func (v *VShardCounter) AdminCancelOrders(ctx context.Context, req *connect.Request[counterrpc.AdminCancelOrdersRequest]) (*connect.Response[counterrpc.AdminCancelOrdersResponse], error) {
	if req.Msg.UserId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("user_id required under vshard routing (symbol-only fan-out moves to operator tooling)"))
	}
	var resp *connect.Response[counterrpc.AdminCancelOrdersResponse]
	err := v.dispatch(req.Msg.UserId, func(c counterrpcconnect.CounterServiceClient) error {
		r, err := c.AdminCancelOrders(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (v *VShardCounter) CancelMyOrders(ctx context.Context, req *connect.Request[counterrpc.CancelMyOrdersRequest]) (*connect.Response[counterrpc.CancelMyOrdersResponse], error) {
	var resp *connect.Response[counterrpc.CancelMyOrdersResponse]
	err := v.dispatch(req.Msg.UserId, func(c counterrpcconnect.CounterServiceClient) error {
		r, err := c.CancelMyOrders(ctx, req)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}
