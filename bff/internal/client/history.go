package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
)

// History is the narrow surface BFF needs from the history service (MVP-15,
// ADR-0046). Mirrors the HistoryService gRPC interface one-to-one so tests
// can substitute a fake without pulling the grpc package.
type History interface {
	GetOrder(ctx context.Context, in *historypb.GetOrderRequest, opts ...grpc.CallOption) (*historypb.GetOrderResponse, error)
	ListOrders(ctx context.Context, in *historypb.ListOrdersRequest, opts ...grpc.CallOption) (*historypb.ListOrdersResponse, error)
	ListTrades(ctx context.Context, in *historypb.ListTradesRequest, opts ...grpc.CallOption) (*historypb.ListTradesResponse, error)
	ListAccountLogs(ctx context.Context, in *historypb.ListAccountLogsRequest, opts ...grpc.CallOption) (*historypb.ListAccountLogsResponse, error)
}

// DialHistory opens a plaintext gRPC connection to the history service.
// mTLS / auth land with the broader credentials work later.
func DialHistory(_ context.Context, endpoint string) (*grpc.ClientConn, History, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc Dial %s: %w", endpoint, err)
	}
	return conn, historypb.NewHistoryServiceClient(conn), nil
}
