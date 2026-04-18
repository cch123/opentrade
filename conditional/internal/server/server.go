// Package server is the gRPC entry point into the conditional service. It
// translates between condrpc.* and the narrower service + engine errors.
package server

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	"github.com/xargin/opentrade/conditional/internal/engine"
	"github.com/xargin/opentrade/conditional/internal/service"
)

// Server satisfies condrpc.ConditionalServiceServer.
type Server struct {
	condrpc.UnimplementedConditionalServiceServer
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

// PlaceConditional is the gRPC entry. Errors are mapped to canonical codes.
func (s *Server) PlaceConditional(ctx context.Context, req *condrpc.PlaceConditionalRequest) (*condrpc.PlaceConditionalResponse, error) {
	resp, err := s.svc.Place(ctx, req, s.clock().UnixMilli())
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return resp, nil
}

// CancelConditional is the gRPC entry.
func (s *Server) CancelConditional(ctx context.Context, req *condrpc.CancelConditionalRequest) (*condrpc.CancelConditionalResponse, error) {
	resp, err := s.svc.Cancel(ctx, req)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return resp, nil
}

// QueryConditional is the gRPC entry.
func (s *Server) QueryConditional(_ context.Context, req *condrpc.QueryConditionalRequest) (*condrpc.QueryConditionalResponse, error) {
	resp, err := s.svc.Query(req)
	if err != nil {
		return nil, toGRPCErr(err)
	}
	return resp, nil
}

// ListConditionals is the gRPC entry.
func (s *Server) ListConditionals(_ context.Context, req *condrpc.ListConditionalsRequest) (*condrpc.ListConditionalsResponse, error) {
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
