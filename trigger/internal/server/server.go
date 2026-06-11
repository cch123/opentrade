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

// CountActiveTriggers is the ADR-0077 §3 / ADR-0078 §6 mode-switch guard
// query perp-counter's TriggerChecker seam calls.
func (s *Server) CountActiveTriggers(_ context.Context, req *connect.Request[condrpc.CountActiveTriggersRequest]) (*connect.Response[condrpc.CountActiveTriggersResponse], error) {
	if req.Msg.GetUserId() == 0 || req.Msg.GetSymbol() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id and symbol required"))
	}
	n := s.svc.Engine().CountActiveTriggers(req.Msg.GetUserId(), req.Msg.GetSymbol())
	return connect.NewResponse(&condrpc.CountActiveTriggersResponse{Count: uint32(n)}), nil
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
		errors.Is(err, engine.ErrStopPriceForbidden),
		errors.Is(err, engine.ErrOCOPerpMismatch),
		errors.Is(err, engine.ErrPerpFieldsForbidden),
		errors.Is(err, engine.ErrPerpQuoteQtyForbidden),
		errors.Is(err, engine.ErrPerpPositionIdxRange),
		errors.Is(err, engine.ErrPerpSlippageShape):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, engine.ErrPerpNotEnabled):
		// Deployment-state precondition, not a request-shape problem
		// (ADR-0078 §6: perp triggers fail closed without a perp placer).
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}
