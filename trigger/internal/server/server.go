// Package server is the gRPC entry point into the trigger service. It
// translates between condrpc.* and the narrower service + engine errors.
package server

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	"github.com/xargin/opentrade/trigger/engine"
	"github.com/xargin/opentrade/trigger/internal/service"
)

// Server satisfies condrpc.TriggerServiceServer.
type Server struct {
	condrpc.UnimplementedTriggerServiceServer
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

// PlaceTrigger is the gRPC entry. Errors are mapped to canonical codes.
func (s *Server) PlaceTrigger(ctx context.Context, req *condrpc.PlaceTriggerRequest) (*condrpc.PlaceTriggerResponse, error) {
	resp, err := s.svc.Place(ctx, req, s.clock().UnixMilli())
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return resp, nil
}

// CancelTrigger is the gRPC entry.
func (s *Server) CancelTrigger(ctx context.Context, req *condrpc.CancelTriggerRequest) (*condrpc.CancelTriggerResponse, error) {
	resp, err := s.svc.Cancel(ctx, req)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return resp, nil
}

// QueryTrigger is the gRPC entry.
func (s *Server) QueryTrigger(_ context.Context, req *condrpc.QueryTriggerRequest) (*condrpc.QueryTriggerResponse, error) {
	resp, err := s.svc.Query(req)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return resp, nil
}

// ListTriggers is the gRPC entry.
func (s *Server) ListTriggers(_ context.Context, req *condrpc.ListTriggersRequest) (*condrpc.ListTriggersResponse, error) {
	resp, err := s.svc.List(req)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return resp, nil
}

// PlaceOCO is the gRPC entry.
func (s *Server) PlaceOCO(ctx context.Context, req *condrpc.PlaceOCORequest) (*condrpc.PlaceOCOResponse, error) {
	resp, err := s.svc.PlaceOCO(ctx, req, s.clock().UnixMilli())
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return resp, nil
}

// toGRPCErr maps engine-layer errors into canonical codes. Validation
// failures become InvalidArgument; missing records become NotFound; the
// catch-all is Internal (operators should see these in logs and fix).
func toGRPCErr(err error) error {
	switch {
	case errors.Is(err, engine.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrNotOwner):
		return status.Error(codes.PermissionDenied, err.Error())
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
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
