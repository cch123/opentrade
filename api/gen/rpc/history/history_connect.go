package historyrpc

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/grpc"

	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
)

type historyServiceConnectClient struct {
	getOrder         *connect.Client[GetOrderRequest, GetOrderResponse]
	listOrders       *connect.Client[ListOrdersRequest, ListOrdersResponse]
	listTrades       *connect.Client[ListTradesRequest, ListTradesResponse]
	listAccountLogs  *connect.Client[ListAccountLogsRequest, ListAccountLogsResponse]
	getConditional   *connect.Client[GetConditionalRequest, GetConditionalResponse]
	listConditionals *connect.Client[ListConditionalsRequest, ListConditionalsResponse]
	getTransfer      *connect.Client[GetTransferRequest, GetTransferResponse]
	listTransfers    *connect.Client[ListTransfersRequest, ListTransfersResponse]
}

func NewHistoryServiceConnectClient(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) HistoryServiceClient {
	return &historyServiceConnectClient{
		getOrder:         connect.NewClient[GetOrderRequest, GetOrderResponse](httpClient, connectutil.ProcedureURL(baseURL, HistoryService_GetOrder_FullMethodName), opts...),
		listOrders:       connect.NewClient[ListOrdersRequest, ListOrdersResponse](httpClient, connectutil.ProcedureURL(baseURL, HistoryService_ListOrders_FullMethodName), opts...),
		listTrades:       connect.NewClient[ListTradesRequest, ListTradesResponse](httpClient, connectutil.ProcedureURL(baseURL, HistoryService_ListTrades_FullMethodName), opts...),
		listAccountLogs:  connect.NewClient[ListAccountLogsRequest, ListAccountLogsResponse](httpClient, connectutil.ProcedureURL(baseURL, HistoryService_ListAccountLogs_FullMethodName), opts...),
		getConditional:   connect.NewClient[GetConditionalRequest, GetConditionalResponse](httpClient, connectutil.ProcedureURL(baseURL, HistoryService_GetConditional_FullMethodName), opts...),
		listConditionals: connect.NewClient[ListConditionalsRequest, ListConditionalsResponse](httpClient, connectutil.ProcedureURL(baseURL, HistoryService_ListConditionals_FullMethodName), opts...),
		getTransfer:      connect.NewClient[GetTransferRequest, GetTransferResponse](httpClient, connectutil.ProcedureURL(baseURL, HistoryService_GetTransfer_FullMethodName), opts...),
		listTransfers:    connect.NewClient[ListTransfersRequest, ListTransfersResponse](httpClient, connectutil.ProcedureURL(baseURL, HistoryService_ListTransfers_FullMethodName), opts...),
	}
}

func (c *historyServiceConnectClient) GetOrder(ctx context.Context, in *GetOrderRequest, _ ...grpc.CallOption) (*GetOrderResponse, error) {
	return connectutil.ResponseMsg(c.getOrder.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *historyServiceConnectClient) ListOrders(ctx context.Context, in *ListOrdersRequest, _ ...grpc.CallOption) (*ListOrdersResponse, error) {
	return connectutil.ResponseMsg(c.listOrders.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *historyServiceConnectClient) ListTrades(ctx context.Context, in *ListTradesRequest, _ ...grpc.CallOption) (*ListTradesResponse, error) {
	return connectutil.ResponseMsg(c.listTrades.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *historyServiceConnectClient) ListAccountLogs(ctx context.Context, in *ListAccountLogsRequest, _ ...grpc.CallOption) (*ListAccountLogsResponse, error) {
	return connectutil.ResponseMsg(c.listAccountLogs.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *historyServiceConnectClient) GetConditional(ctx context.Context, in *GetConditionalRequest, _ ...grpc.CallOption) (*GetConditionalResponse, error) {
	return connectutil.ResponseMsg(c.getConditional.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *historyServiceConnectClient) ListConditionals(ctx context.Context, in *ListConditionalsRequest, _ ...grpc.CallOption) (*ListConditionalsResponse, error) {
	return connectutil.ResponseMsg(c.listConditionals.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *historyServiceConnectClient) GetTransfer(ctx context.Context, in *GetTransferRequest, _ ...grpc.CallOption) (*GetTransferResponse, error) {
	return connectutil.ResponseMsg(c.getTransfer.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *historyServiceConnectClient) ListTransfers(ctx context.Context, in *ListTransfersRequest, _ ...grpc.CallOption) (*ListTransfersResponse, error) {
	return connectutil.ResponseMsg(c.listTransfers.CallUnary(ctx, connect.NewRequest(in)))
}

func NewHistoryServiceHTTPHandler(svc HistoryServiceServer, opts ...connect.HandlerOption) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(HistoryService_GetOrder_FullMethodName, connect.NewUnaryHandler(HistoryService_GetOrder_FullMethodName, func(ctx context.Context, req *connect.Request[GetOrderRequest]) (*connect.Response[GetOrderResponse], error) {
		resp, err := svc.GetOrder(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(HistoryService_ListOrders_FullMethodName, connect.NewUnaryHandler(HistoryService_ListOrders_FullMethodName, func(ctx context.Context, req *connect.Request[ListOrdersRequest]) (*connect.Response[ListOrdersResponse], error) {
		resp, err := svc.ListOrders(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(HistoryService_ListTrades_FullMethodName, connect.NewUnaryHandler(HistoryService_ListTrades_FullMethodName, func(ctx context.Context, req *connect.Request[ListTradesRequest]) (*connect.Response[ListTradesResponse], error) {
		resp, err := svc.ListTrades(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(HistoryService_ListAccountLogs_FullMethodName, connect.NewUnaryHandler(HistoryService_ListAccountLogs_FullMethodName, func(ctx context.Context, req *connect.Request[ListAccountLogsRequest]) (*connect.Response[ListAccountLogsResponse], error) {
		resp, err := svc.ListAccountLogs(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(HistoryService_GetConditional_FullMethodName, connect.NewUnaryHandler(HistoryService_GetConditional_FullMethodName, func(ctx context.Context, req *connect.Request[GetConditionalRequest]) (*connect.Response[GetConditionalResponse], error) {
		resp, err := svc.GetConditional(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(HistoryService_ListConditionals_FullMethodName, connect.NewUnaryHandler(HistoryService_ListConditionals_FullMethodName, func(ctx context.Context, req *connect.Request[ListConditionalsRequest]) (*connect.Response[ListConditionalsResponse], error) {
		resp, err := svc.ListConditionals(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(HistoryService_GetTransfer_FullMethodName, connect.NewUnaryHandler(HistoryService_GetTransfer_FullMethodName, func(ctx context.Context, req *connect.Request[GetTransferRequest]) (*connect.Response[GetTransferResponse], error) {
		resp, err := svc.GetTransfer(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(HistoryService_ListTransfers_FullMethodName, connect.NewUnaryHandler(HistoryService_ListTransfers_FullMethodName, func(ctx context.Context, req *connect.Request[ListTransfersRequest]) (*connect.Response[ListTransfersResponse], error) {
		resp, err := svc.ListTransfers(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	return mux
}
