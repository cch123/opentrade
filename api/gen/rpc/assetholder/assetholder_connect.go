package assetholderrpc

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/grpc"

	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
)

type assetHolderConnectClient struct {
	transferOut           *connect.Client[TransferOutRequest, TransferOutResponse]
	transferIn            *connect.Client[TransferInRequest, TransferInResponse]
	compensateTransferOut *connect.Client[CompensateTransferOutRequest, CompensateTransferOutResponse]
}

func NewAssetHolderConnectClient(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) AssetHolderClient {
	return &assetHolderConnectClient{
		transferOut:           connect.NewClient[TransferOutRequest, TransferOutResponse](httpClient, connectutil.ProcedureURL(baseURL, AssetHolder_TransferOut_FullMethodName), opts...),
		transferIn:            connect.NewClient[TransferInRequest, TransferInResponse](httpClient, connectutil.ProcedureURL(baseURL, AssetHolder_TransferIn_FullMethodName), opts...),
		compensateTransferOut: connect.NewClient[CompensateTransferOutRequest, CompensateTransferOutResponse](httpClient, connectutil.ProcedureURL(baseURL, AssetHolder_CompensateTransferOut_FullMethodName), opts...),
	}
}

func (c *assetHolderConnectClient) TransferOut(ctx context.Context, in *TransferOutRequest, _ ...grpc.CallOption) (*TransferOutResponse, error) {
	return connectutil.ResponseMsg(c.transferOut.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *assetHolderConnectClient) TransferIn(ctx context.Context, in *TransferInRequest, _ ...grpc.CallOption) (*TransferInResponse, error) {
	return connectutil.ResponseMsg(c.transferIn.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *assetHolderConnectClient) CompensateTransferOut(ctx context.Context, in *CompensateTransferOutRequest, _ ...grpc.CallOption) (*CompensateTransferOutResponse, error) {
	return connectutil.ResponseMsg(c.compensateTransferOut.CallUnary(ctx, connect.NewRequest(in)))
}

func NewAssetHolderHTTPHandler(svc AssetHolderServer, opts ...connect.HandlerOption) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(AssetHolder_TransferOut_FullMethodName, connect.NewUnaryHandler(AssetHolder_TransferOut_FullMethodName, func(ctx context.Context, req *connect.Request[TransferOutRequest]) (*connect.Response[TransferOutResponse], error) {
		resp, err := svc.TransferOut(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(AssetHolder_TransferIn_FullMethodName, connect.NewUnaryHandler(AssetHolder_TransferIn_FullMethodName, func(ctx context.Context, req *connect.Request[TransferInRequest]) (*connect.Response[TransferInResponse], error) {
		resp, err := svc.TransferIn(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(AssetHolder_CompensateTransferOut_FullMethodName, connect.NewUnaryHandler(AssetHolder_CompensateTransferOut_FullMethodName, func(ctx context.Context, req *connect.Request[CompensateTransferOutRequest]) (*connect.Response[CompensateTransferOutResponse], error) {
		resp, err := svc.CompensateTransferOut(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	return mux
}
