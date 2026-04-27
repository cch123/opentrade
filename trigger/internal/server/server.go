// Package server is the Connect-Go entry point into the trigger service.
// It translates between the generated handler interface and the narrower
// service + engine errors.
package server

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	"github.com/xargin/opentrade/trigger/engine"
	"github.com/xargin/opentrade/trigger/internal/service"
)

// Server satisfies triggerrpcconnect.TriggerServiceHandler.
type Server struct {
	svc   *service.Service
	clock func() time.Time
}

// New wires a Server. A nil clock defaults to time.Now.
func New(svc *service.Service, clock func() time.Time) *Server {
	if clock == nil {
		clock = time.Now
	}
	return &Server{svc: svc, clock: clock}
}

func (s *Server) PlaceTrigger(ctx context.Context, req *connect.Request[condrpc.PlaceTriggerRequest]) (*connect.Response[condrpc.PlaceTriggerResponse], error) {
	resp, err := s.svc.Place(ctx, req.Msg, s.clock().UnixMilli())
	if err != nil {
		return nil, toConnectErr(err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) CancelTrigger(ctx context.Context, req *connect.Request[condrpc.CancelTriggerRequest]) (*connect.Response[condrpc.CancelTriggerResponse], error) {
	resp, err := s.svc.Cancel(ctx, req.Msg)
	if err != nil {
		return nil, toConnectErr(err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) QueryTrigger(_ context.Context, req *connect.Request[condrpc.QueryTriggerRequest]) (*connect.Response[condrpc.QueryTriggerResponse], error) {
	resp, err := s.svc.Query(req.Msg)
	if err != nil {
		return nil, toConnectErr(err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) ListTriggers(_ context.Context, req *connect.Request[condrpc.ListTriggersRequest]) (*connect.Response[condrpc.ListTriggersResponse], error) {
	resp, err := s.svc.List(req.Msg)
	if err != nil {
		return nil, toConnectErr(err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) PlaceOCO(ctx context.Context, req *connect.Request[condrpc.PlaceOCORequest]) (*connect.Response[condrpc.PlaceOCOResponse], error) {
	resp, err := s.svc.PlaceOCO(ctx, req.Msg, s.clock().UnixMilli())
	if err != nil {
		return nil, toConnectErr(err)
	}
	return connect.NewResponse(resp), nil
}

// toConnectErr maps engine-layer errors into canonical Connect codes.
// Validation failures become InvalidArgument; missing records become
// NotFound; the catch-all is Internal.
func toConnectErr(err error) error {
	switch {
	case errors.Is(err, engine.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, engine.ErrNotOwner):
		return connect.NewError(connect.CodePermissionDenied, err)
	case errors.Is(err, engine.ErrMissingUserID),
		errors.Is(err, engine.ErrMissingSymbol),
		errors.Is(err, engine.ErrInvalidType),
		errors.Is(err, engine.ErrInvalidSide),
		errors.Is(err, engine.ErrInvalidStopPrice),
		errors.Is(err, engine.ErrLimitPriceRequired),
		errors.Is(err, engine.ErrLimitPriceForbidden),
		errors.Is(err, engine.ErrQtyRequired),
		errors.Is(err, engine.ErrQuoteQtyShape),
		errors.Is(err, engine.ErrBothQtyAndQuoteQty),
		errors.Is(err, engine.ErrExpiryInPast),
		errors.Is(err, engine.ErrOCONeedsTwoLegs),
		errors.Is(err, engine.ErrOCOSymbolMismatch),
		errors.Is(err, engine.ErrOCOSideMismatch),
		errors.Is(err, engine.ErrOCOUserMismatch),
		errors.Is(err, engine.ErrTrailingDeltaNeeded),
		errors.Is(err, engine.ErrTrailingDeltaForbidden),
		errors.Is(err, engine.ErrTrailingDeltaRange),
		errors.Is(err, engine.ErrActivationPriceShape),
		errors.Is(err, engine.ErrStopPriceForbidden):
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}
