// Package counterclient wraps Counter gRPC shard connections so the
// engine only sees the narrow OrderPlacer surface it needs. Routing
// mirrors BFF (pkg/shard.Index by user_id) — we don't depend on
// bff/internal/client to keep module boundaries clean.
package counterclient

import (
	"context"
	"fmt"
	"io"

	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/shard"
)

// Dial opens one plaintext RPC transport. mTLS / auth credentials arrive
// with the broader auth work later.
func Dial(_ context.Context, endpoint string) (io.Closer, counterrpc.CounterServiceClient, error) {
	httpClient, closer := connectutil.NewHTTPClient()
	return closer, counterrpc.NewCounterServiceConnectClient(httpClient, endpoint), nil
}

// Sharded implements engine.OrderPlacer by routing each PlaceOrder to the
// shard owning req.UserId. clients[i] must be the shard whose id == i.
type Sharded struct {
	clients []counterrpc.CounterServiceClient
}

// NewSharded wraps an ordered slice of shard clients.
func NewSharded(clients []counterrpc.CounterServiceClient) (*Sharded, error) {
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
	return s.clients[idx].PlaceOrder(ctx, req)
}

// Reserve routes a ReserveRequest to the user's shard (ADR-0041).
func (s *Sharded) Reserve(ctx context.Context, req *counterrpc.ReserveRequest) (*counterrpc.ReserveResponse, error) {
	if req.UserId == "" {
		return nil, fmt.Errorf("sharded counter: empty user id")
	}
	idx := shard.Index(req.UserId, len(s.clients))
	return s.clients[idx].Reserve(ctx, req)
}

// ReleaseReservation routes to the user's shard. UserId is required so we
// can pick a shard; the engine always passes it.
func (s *Sharded) ReleaseReservation(ctx context.Context, req *counterrpc.ReleaseReservationRequest) (*counterrpc.ReleaseReservationResponse, error) {
	if req.UserId == "" {
		return nil, fmt.Errorf("sharded counter: empty user id")
	}
	idx := shard.Index(req.UserId, len(s.clients))
	return s.clients[idx].ReleaseReservation(ctx, req)
}
