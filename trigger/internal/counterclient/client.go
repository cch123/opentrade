// Package counterclient wraps Counter Connect-Go shard connections so the
// engine only sees the narrow OrderPlacer surface it needs. Routing
// mirrors BFF (pkg/shard.Index by user_id) — we don't depend on
// bff/internal/client to keep module boundaries clean.
package counterclient

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/api/gen/rpc/counter/counterrpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
	"github.com/xargin/opentrade/pkg/shard"
)

// Dial returns a Counter Connect client for endpoint. The returned
// *http.Client is shared by future calls; callers may close idle
// connections via Transport.CloseIdleConnections, but there is no
// explicit Close — tearing down the binary is enough. mTLS / auth
// credentials arrive with the broader auth work later.
func Dial(_ context.Context, endpoint string) (*http.Client, counterrpcconnect.CounterServiceClient, error) {
	if endpoint == "" {
		return nil, nil, fmt.Errorf("dial: empty endpoint")
	}
	httpClient := connectx.NewH2CClient()
	cli := counterrpcconnect.NewCounterServiceClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	return httpClient, cli, nil
}

// Sharded implements engine.OrderPlacer by routing each PlaceOrder to the
// shard owning req.UserId. clients[i] must be the shard whose id == i.
type Sharded struct {
	clients []counterrpcconnect.CounterServiceClient
}

// NewSharded wraps an ordered slice of shard clients.
func NewSharded(clients []counterrpcconnect.CounterServiceClient) (*Sharded, error) {
	if len(clients) == 0 {
		return nil, fmt.Errorf("sharded counter: at least one shard required")
	}
	for i, c := range clients {
		if c == nil {
			return nil, fmt.Errorf("sharded counter: shard %d is nil", i)
		}
	}
	return &Sharded{clients: clients}, nil
}

// Shards reports the configured shard count.
func (s *Sharded) Shards() int { return len(s.clients) }

// PlaceOrder routes to the user's shard. Empty user_id is a caller bug
// (engine.buildPlaceOrderReq should never emit one).
func (s *Sharded) PlaceOrder(ctx context.Context, req *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error) {
	if req.UserId == "" {
		return nil, fmt.Errorf("sharded counter: empty user id")
	}
	idx := shard.Index(req.UserId, len(s.clients))
	resp, err := s.clients[idx].PlaceOrder(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Reserve routes a ReserveRequest to the user's shard (ADR-0041).
func (s *Sharded) Reserve(ctx context.Context, req *counterrpc.ReserveRequest) (*counterrpc.ReserveResponse, error) {
	if req.UserId == "" {
		return nil, fmt.Errorf("sharded counter: empty user id")
	}
	idx := shard.Index(req.UserId, len(s.clients))
	resp, err := s.clients[idx].Reserve(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ReleaseReservation routes to the user's shard. UserId is required so we
// can pick a shard; the engine always passes it.
func (s *Sharded) ReleaseReservation(ctx context.Context, req *counterrpc.ReleaseReservationRequest) (*counterrpc.ReleaseReservationResponse, error) {
	if req.UserId == "" {
		return nil, fmt.Errorf("sharded counter: empty user id")
	}
	idx := shard.Index(req.UserId, len(s.clients))
	resp, err := s.clients[idx].ReleaseReservation(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}
