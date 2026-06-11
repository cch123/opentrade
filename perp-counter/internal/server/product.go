// product.go is the ADR-0078 Connect surface: amend / batch / cancel-all /
// pre-check / close-all (user plane, routed by BFF) and force-adjust / block
// trade (admin plane, routed by admin-gateway only). All thin pass-throughs:
// validation + sequencing live in the service.
package server

import (
	"context"

	"connectrpc.com/connect"

	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
)

func (s *Server) AmendOrder(_ context.Context, req *connect.Request[perprpc.AmendOrderRequest]) (*connect.Response[perprpc.AmendOrderResponse], error) {
	resp, err := s.svc.AmendOrder(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) BatchPlaceOrders(_ context.Context, req *connect.Request[perprpc.BatchPlaceOrdersRequest]) (*connect.Response[perprpc.BatchPlaceOrdersResponse], error) {
	resp, err := s.svc.BatchPlaceOrders(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) BatchCancelOrders(_ context.Context, req *connect.Request[perprpc.BatchCancelOrdersRequest]) (*connect.Response[perprpc.BatchCancelOrdersResponse], error) {
	resp, err := s.svc.BatchCancelOrders(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) CancelAllOrders(_ context.Context, req *connect.Request[perprpc.CancelAllOrdersRequest]) (*connect.Response[perprpc.CancelAllOrdersResponse], error) {
	resp, err := s.svc.CancelAllOrders(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) PreCheckOrder(_ context.Context, req *connect.Request[perprpc.PreCheckOrderRequest]) (*connect.Response[perprpc.PreCheckOrderResponse], error) {
	resp, err := s.svc.PreCheckOrder(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) CloseAllPositions(_ context.Context, req *connect.Request[perprpc.CloseAllPositionsRequest]) (*connect.Response[perprpc.CloseAllPositionsResponse], error) {
	resp, err := s.svc.CloseAllPositions(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) ForceAdjustPosition(_ context.Context, req *connect.Request[perprpc.ForceAdjustPositionRequest]) (*connect.Response[perprpc.ForceAdjustPositionResponse], error) {
	resp, err := s.svc.ForceAdjustPosition(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) BlockTrade(_ context.Context, req *connect.Request[perprpc.BlockTradeRequest]) (*connect.Response[perprpc.BlockTradeResponse], error) {
	resp, err := s.svc.BlockTrade(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewResponse(resp), nil
}
