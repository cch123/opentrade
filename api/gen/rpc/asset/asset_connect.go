package assetrpc

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/grpc"

	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
)

type assetServiceConnectClient struct {
	transfer            *connect.Client[TransferRequest, TransferResponse]
	queryFundingBalance *connect.Client[QueryFundingBalanceRequest, QueryFundingBalanceResponse]
	queryTransfer       *connect.Client[QueryTransferRequest, QueryTransferResponse]
}

func NewAssetServiceConnectClient(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) AssetServiceClient {
	return &assetServiceConnectClient{
		transfer:            connect.NewClient[TransferRequest, TransferResponse](httpClient, connectutil.ProcedureURL(baseURL, AssetService_Transfer_FullMethodName), opts...),
		queryFundingBalance: connect.NewClient[QueryFundingBalanceRequest, QueryFundingBalanceResponse](httpClient, connectutil.ProcedureURL(baseURL, AssetService_QueryFundingBalance_FullMethodName), opts...),
		queryTransfer:       connect.NewClient[QueryTransferRequest, QueryTransferResponse](httpClient, connectutil.ProcedureURL(baseURL, AssetService_QueryTransfer_FullMethodName), opts...),
	}
}

func (c *assetServiceConnectClient) Transfer(ctx context.Context, in *TransferRequest, _ ...grpc.CallOption) (*TransferResponse, error) {
	return connectutil.ResponseMsg(c.transfer.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *assetServiceConnectClient) QueryFundingBalance(ctx context.Context, in *QueryFundingBalanceRequest, _ ...grpc.CallOption) (*QueryFundingBalanceResponse, error) {
	return connectutil.ResponseMsg(c.queryFundingBalance.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *assetServiceConnectClient) QueryTransfer(ctx context.Context, in *QueryTransferRequest, _ ...grpc.CallOption) (*QueryTransferResponse, error) {
	return connectutil.ResponseMsg(c.queryTransfer.CallUnary(ctx, connect.NewRequest(in)))
}

func NewAssetServiceHTTPHandler(svc AssetServiceServer, opts ...connect.HandlerOption) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(AssetService_Transfer_FullMethodName, connect.NewUnaryHandler(AssetService_Transfer_FullMethodName, func(ctx context.Context, req *connect.Request[TransferRequest]) (*connect.Response[TransferResponse], error) {
		resp, err := svc.Transfer(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(AssetService_QueryFundingBalance_FullMethodName, connect.NewUnaryHandler(AssetService_QueryFundingBalance_FullMethodName, func(ctx context.Context, req *connect.Request[QueryFundingBalanceRequest]) (*connect.Response[QueryFundingBalanceResponse], error) {
		resp, err := svc.QueryFundingBalance(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(AssetService_QueryTransfer_FullMethodName, connect.NewUnaryHandler(AssetService_QueryTransfer_FullMethodName, func(ctx context.Context, req *connect.Request[QueryTransferRequest]) (*connect.Response[QueryTransferResponse], error) {
		resp, err := svc.QueryTransfer(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	return mux
}
