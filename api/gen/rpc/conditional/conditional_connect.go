package conditionalrpc

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/grpc"

	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
)

type conditionalServiceConnectClient struct {
	placeConditional  *connect.Client[PlaceConditionalRequest, PlaceConditionalResponse]
	cancelConditional *connect.Client[CancelConditionalRequest, CancelConditionalResponse]
	queryConditional  *connect.Client[QueryConditionalRequest, QueryConditionalResponse]
	listConditionals  *connect.Client[ListConditionalsRequest, ListConditionalsResponse]
	placeOCO          *connect.Client[PlaceOCORequest, PlaceOCOResponse]
}

func NewConditionalServiceConnectClient(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) ConditionalServiceClient {
	return &conditionalServiceConnectClient{
		placeConditional:  connect.NewClient[PlaceConditionalRequest, PlaceConditionalResponse](httpClient, connectutil.ProcedureURL(baseURL, ConditionalService_PlaceConditional_FullMethodName), opts...),
		cancelConditional: connect.NewClient[CancelConditionalRequest, CancelConditionalResponse](httpClient, connectutil.ProcedureURL(baseURL, ConditionalService_CancelConditional_FullMethodName), opts...),
		queryConditional:  connect.NewClient[QueryConditionalRequest, QueryConditionalResponse](httpClient, connectutil.ProcedureURL(baseURL, ConditionalService_QueryConditional_FullMethodName), opts...),
		listConditionals:  connect.NewClient[ListConditionalsRequest, ListConditionalsResponse](httpClient, connectutil.ProcedureURL(baseURL, ConditionalService_ListConditionals_FullMethodName), opts...),
		placeOCO:          connect.NewClient[PlaceOCORequest, PlaceOCOResponse](httpClient, connectutil.ProcedureURL(baseURL, ConditionalService_PlaceOCO_FullMethodName), opts...),
	}
}

func (c *conditionalServiceConnectClient) PlaceConditional(ctx context.Context, in *PlaceConditionalRequest, _ ...grpc.CallOption) (*PlaceConditionalResponse, error) {
	return connectutil.ResponseMsg(c.placeConditional.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *conditionalServiceConnectClient) CancelConditional(ctx context.Context, in *CancelConditionalRequest, _ ...grpc.CallOption) (*CancelConditionalResponse, error) {
	return connectutil.ResponseMsg(c.cancelConditional.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *conditionalServiceConnectClient) QueryConditional(ctx context.Context, in *QueryConditionalRequest, _ ...grpc.CallOption) (*QueryConditionalResponse, error) {
	return connectutil.ResponseMsg(c.queryConditional.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *conditionalServiceConnectClient) ListConditionals(ctx context.Context, in *ListConditionalsRequest, _ ...grpc.CallOption) (*ListConditionalsResponse, error) {
	return connectutil.ResponseMsg(c.listConditionals.CallUnary(ctx, connect.NewRequest(in)))
}

func (c *conditionalServiceConnectClient) PlaceOCO(ctx context.Context, in *PlaceOCORequest, _ ...grpc.CallOption) (*PlaceOCOResponse, error) {
	return connectutil.ResponseMsg(c.placeOCO.CallUnary(ctx, connect.NewRequest(in)))
}

func NewConditionalServiceHTTPHandler(svc ConditionalServiceServer, opts ...connect.HandlerOption) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(ConditionalService_PlaceConditional_FullMethodName, connect.NewUnaryHandler(ConditionalService_PlaceConditional_FullMethodName, func(ctx context.Context, req *connect.Request[PlaceConditionalRequest]) (*connect.Response[PlaceConditionalResponse], error) {
		resp, err := svc.PlaceConditional(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(ConditionalService_CancelConditional_FullMethodName, connect.NewUnaryHandler(ConditionalService_CancelConditional_FullMethodName, func(ctx context.Context, req *connect.Request[CancelConditionalRequest]) (*connect.Response[CancelConditionalResponse], error) {
		resp, err := svc.CancelConditional(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(ConditionalService_QueryConditional_FullMethodName, connect.NewUnaryHandler(ConditionalService_QueryConditional_FullMethodName, func(ctx context.Context, req *connect.Request[QueryConditionalRequest]) (*connect.Response[QueryConditionalResponse], error) {
		resp, err := svc.QueryConditional(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(ConditionalService_ListConditionals_FullMethodName, connect.NewUnaryHandler(ConditionalService_ListConditionals_FullMethodName, func(ctx context.Context, req *connect.Request[ListConditionalsRequest]) (*connect.Response[ListConditionalsResponse], error) {
		resp, err := svc.ListConditionals(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	mux.Handle(ConditionalService_PlaceOCO_FullMethodName, connect.NewUnaryHandler(ConditionalService_PlaceOCO_FullMethodName, func(ctx context.Context, req *connect.Request[PlaceOCORequest]) (*connect.Response[PlaceOCOResponse], error) {
		resp, err := svc.PlaceOCO(ctx, req.Msg)
		if err != nil {
			return nil, connectutil.AsConnectError(err)
		}
		return connect.NewResponse(resp), nil
	}, opts...))
	return mux
}
