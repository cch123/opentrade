// Package client wraps the Counter gRPC client so REST handlers depend on an
// interface (easy to fake in tests) rather than on the generated stub
// directly.
package client

import (
	"context"
	"io"

	"google.golang.org/grpc"

	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
)

// Counter is the minimal surface BFF needs. Tests may substitute a fake.
// ADR-0057 M4: Transfer was removed — all user-facing fund movement goes
// through asset-service now (see client.Asset).
type Counter interface {
	PlaceOrder(ctx context.Context, in *counterrpc.PlaceOrderRequest, opts ...grpc.CallOption) (*counterrpc.PlaceOrderResponse, error)
	CancelOrder(ctx context.Context, in *counterrpc.CancelOrderRequest, opts ...grpc.CallOption) (*counterrpc.CancelOrderResponse, error)
	QueryOrder(ctx context.Context, in *counterrpc.QueryOrderRequest, opts ...grpc.CallOption) (*counterrpc.QueryOrderResponse, error)
	QueryBalance(ctx context.Context, in *counterrpc.QueryBalanceRequest, opts ...grpc.CallOption) (*counterrpc.QueryBalanceResponse, error)
	AdminCancelOrders(ctx context.Context, in *counterrpc.AdminCancelOrdersRequest, opts ...grpc.CallOption) (*counterrpc.AdminCancelOrdersResponse, error)
	CancelMyOrders(ctx context.Context, in *counterrpc.CancelMyOrdersRequest, opts ...grpc.CallOption) (*counterrpc.CancelMyOrdersResponse, error)
}

// Dial opens a plaintext RPC transport to a Counter shard endpoint.
// mTLS / auth credentials arrive in a later MVP.
func Dial(_ context.Context, endpoint string) (io.Closer, Counter, error) {
	httpClient, closer := connectutil.NewHTTPClient()
	return closer, counterrpc.NewCounterServiceConnectClient(httpClient, endpoint), nil
}
