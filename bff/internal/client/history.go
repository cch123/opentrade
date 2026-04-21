package client

import (
	"context"
	"io"

	"google.golang.org/grpc"

	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
)

// History is the narrow surface BFF needs from the history service (MVP-15,
// ADR-0046 / ADR-0057 M4). Mirrors the HistoryService gRPC interface
// one-to-one so tests can substitute a fake without pulling the grpc
// package.
type History interface {
	GetOrder(ctx context.Context, in *historypb.GetOrderRequest, opts ...grpc.CallOption) (*historypb.GetOrderResponse, error)
	ListOrders(ctx context.Context, in *historypb.ListOrdersRequest, opts ...grpc.CallOption) (*historypb.ListOrdersResponse, error)
	ListTrades(ctx context.Context, in *historypb.ListTradesRequest, opts ...grpc.CallOption) (*historypb.ListTradesResponse, error)
	ListAccountLogs(ctx context.Context, in *historypb.ListAccountLogsRequest, opts ...grpc.CallOption) (*historypb.ListAccountLogsResponse, error)
	GetConditional(ctx context.Context, in *historypb.GetConditionalRequest, opts ...grpc.CallOption) (*historypb.GetConditionalResponse, error)
	ListConditionals(ctx context.Context, in *historypb.ListConditionalsRequest, opts ...grpc.CallOption) (*historypb.ListConditionalsResponse, error)
	GetTransfer(ctx context.Context, in *historypb.GetTransferRequest, opts ...grpc.CallOption) (*historypb.GetTransferResponse, error)
	ListTransfers(ctx context.Context, in *historypb.ListTransfersRequest, opts ...grpc.CallOption) (*historypb.ListTransfersResponse, error)
}

// DialHistory opens a plaintext RPC transport to the history service.
// mTLS / auth land with the broader credentials work later.
func DialHistory(_ context.Context, endpoint string) (io.Closer, History, error) {
	httpClient, closer := connectutil.NewHTTPClient()
	return closer, historypb.NewHistoryServiceConnectClient(httpClient, endpoint), nil
}
