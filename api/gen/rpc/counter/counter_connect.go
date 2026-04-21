package counterrpc

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/grpc"

	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
)

type counterServiceConnectClient struct {
	placeOrder         *connect.Client[PlaceOrderRequest, PlaceOrderResponse]
	cancelOrder        *connect.Client[CancelOrderRequest, CancelOrderResponse]
	queryOrder         *connect.Client[QueryOrderRequest, QueryOrderResponse]
	queryBalance       *connect.Client[QueryBalanceRequest, QueryBalanceResponse]
	reserve            *connect.Client[ReserveRequest, ReserveResponse]
	releaseReservation *connect.Client[ReleaseReservationRequest, ReleaseReservationResponse]
	adminCancelOrders  *connect.Client[AdminCancelOrdersRequest, AdminCancelOrdersResponse]
	cancelMyOrders     *connect.Client[CancelMyOrdersRequest, CancelMyOrdersResponse]
}

// NewCounterServiceConnectClient builds a connect-go-backed client that still
// satisfies the generated CounterServiceClient interface.
func NewCounterServiceConnectClient(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) CounterServiceClient {
	return &counterServiceConnectClient{
		placeOrder:         connect.NewClient[PlaceOrderRequest, PlaceOrderResponse](httpClient, connectutil.ProcedureURL(baseURL, CounterService_PlaceOrder_FullMethodName), opts...),
		cancelOrder:        connect.NewClient[CancelOrderRequest, CancelOrderResponse](httpClient, connectutil.ProcedureURL(baseURL, CounterService_CancelOrder_FullMethodName), opts...),
		queryOrder:         connect.NewClient[QueryOrderRequest, QueryOrderResponse](httpClient, connectutil.ProcedureURL(baseURL, CounterService_QueryOrder_FullMethodName), opts...),
		queryBalance:       connect.NewClient[QueryBalanceRequest, QueryBalanceResponse](httpClient, connectutil.ProcedureURL(baseURL, CounterService_QueryBalance_FullMethodName), opts...),
		reserve:            connect.NewClient[ReserveRequest, ReserveResponse](httpClient, connectutil.ProcedureURL(baseURL, CounterService_Reserve_FullMethodName), opts...),
		releaseReservation: connect.NewClient[ReleaseReservationRequest, ReleaseReservationResponse](httpClient, connectutil.ProcedureURL(baseURL, CounterService_ReleaseReservation_FullMethodName), opts...),
		adminCancelOrders:  connect.NewClient[AdminCancelOrdersRequest, AdminCancelOrdersResponse](httpClient, connectutil.ProcedureURL(baseURL, CounterService_AdminCancelOrders_FullMethodName), opts...),
		cancelMyOrders:     connect.NewClient[CancelMyOrdersRequest, CancelMyOrdersResponse](httpClient, connectutil.ProcedureURL(baseURL, CounterService_CancelMyOrders_FullMethodName), opts...),
	}
}

func (c *counterServiceConnectClient) PlaceOrder(ctx context.Context, in *PlaceOrderRequest, _ ...grpc.CallOption) (*PlaceOrderResponse, error) {
	return connectutil.ResponseMsg(c.placeOrder.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *counterServiceConnectClient) CancelOrder(ctx context.Context, in *CancelOrderRequest, _ ...grpc.CallOption) (*CancelOrderResponse, error) {
	return connectutil.ResponseMsg(c.cancelOrder.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *counterServiceConnectClient) QueryOrder(ctx context.Context, in *QueryOrderRequest, _ ...grpc.CallOption) (*QueryOrderResponse, error) {
	return connectutil.ResponseMsg(c.queryOrder.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *counterServiceConnectClient) QueryBalance(ctx context.Context, in *QueryBalanceRequest, _ ...grpc.CallOption) (*QueryBalanceResponse, error) {
	return connectutil.ResponseMsg(c.queryBalance.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *counterServiceConnectClient) Reserve(ctx context.Context, in *ReserveRequest, _ ...grpc.CallOption) (*ReserveResponse, error) {
	return connectutil.ResponseMsg(c.reserve.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *counterServiceConnectClient) ReleaseReservation(ctx context.Context, in *ReleaseReservationRequest, _ ...grpc.CallOption) (*ReleaseReservationResponse, error) {
	return connectutil.ResponseMsg(c.releaseReservation.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *counterServiceConnectClient) AdminCancelOrders(ctx context.Context, in *AdminCancelOrdersRequest, _ ...grpc.CallOption) (*AdminCancelOrdersResponse, error) {
	return connectutil.ResponseMsg(c.adminCancelOrders.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *counterServiceConnectClient) CancelMyOrders(ctx context.Context, in *CancelMyOrdersRequest, _ ...grpc.CallOption) (*CancelMyOrdersResponse, error) {
	return connectutil.ResponseMsg(c.cancelMyOrders.CallUnary(ctx, connect.NewRequest(in)))
}

// NewCounterServiceHTTPHandler exposes the generated CounterServiceServer over
// connect-go without changing the existing business-layer method signatures.
func NewCounterServiceHTTPHandler(svc CounterServiceServer, opts ...connect.HandlerOption) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(CounterService_PlaceOrder_FullMethodName, connect.NewUnaryHandler(CounterService_PlaceOrder_FullMethodName, func(ctx context.Context, req *connect.Request[PlaceOrderRequest]) (*connect.Response[PlaceOrderResponse], error) {
		resp, err := svc.PlaceOrder(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(CounterService_CancelOrder_FullMethodName, connect.NewUnaryHandler(CounterService_CancelOrder_FullMethodName, func(ctx context.Context, req *connect.Request[CancelOrderRequest]) (*connect.Response[CancelOrderResponse], error) {
		resp, err := svc.CancelOrder(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(CounterService_QueryOrder_FullMethodName, connect.NewUnaryHandler(CounterService_QueryOrder_FullMethodName, func(ctx context.Context, req *connect.Request[QueryOrderRequest]) (*connect.Response[QueryOrderResponse], error) {
		resp, err := svc.QueryOrder(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(CounterService_QueryBalance_FullMethodName, connect.NewUnaryHandler(CounterService_QueryBalance_FullMethodName, func(ctx context.Context, req *connect.Request[QueryBalanceRequest]) (*connect.Response[QueryBalanceResponse], error) {
		resp, err := svc.QueryBalance(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(CounterService_Reserve_FullMethodName, connect.NewUnaryHandler(CounterService_Reserve_FullMethodName, func(ctx context.Context, req *connect.Request[ReserveRequest]) (*connect.Response[ReserveResponse], error) {
		resp, err := svc.Reserve(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(CounterService_ReleaseReservation_FullMethodName, connect.NewUnaryHandler(CounterService_ReleaseReservation_FullMethodName, func(ctx context.Context, req *connect.Request[ReleaseReservationRequest]) (*connect.Response[ReleaseReservationResponse], error) {
		resp, err := svc.ReleaseReservation(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(CounterService_AdminCancelOrders_FullMethodName, connect.NewUnaryHandler(CounterService_AdminCancelOrders_FullMethodName, func(ctx context.Context, req *connect.Request[AdminCancelOrdersRequest]) (*connect.Response[AdminCancelOrdersResponse], error) {
		resp, err := svc.AdminCancelOrders(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(CounterService_CancelMyOrders_FullMethodName, connect.NewUnaryHandler(CounterService_CancelMyOrders_FullMethodName, func(ctx context.Context, req *connect.Request[CancelMyOrdersRequest]) (*connect.Response[CancelMyOrdersResponse], error) {
		resp, err := svc.CancelMyOrders(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	return mux
}
