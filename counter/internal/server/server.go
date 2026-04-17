// Package server adapts counter/internal/service.Service to the protobuf
// CounterService gRPC interface.
package server

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
)

// Server implements counterrpc.CounterServiceServer. PlaceOrder / CancelOrder /
// QueryOrder land in MVP-3; they currently return Unimplemented via the
// embedded UnimplementedCounterServiceServer.
type Server struct {
	counterrpc.UnimplementedCounterServiceServer

	svc    *service.Service
	logger *zap.Logger
}

// New creates a Server.
func New(svc *service.Service, logger *zap.Logger) *Server {
	return &Server{svc: svc, logger: logger}
}

// Transfer implements CounterService.Transfer.
func (s *Server) Transfer(ctx context.Context, req *counterrpc.TransferRequest) (*counterrpc.TransferResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	internalReq, err := transferRequestFromProto(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	res, err := s.svc.Transfer(ctx, internalReq)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return transferResponseToProto(res), nil
}

// QueryBalance implements CounterService.QueryBalance. If Asset is empty,
// returns all assets for the user.
func (s *Server) QueryBalance(_ context.Context, req *counterrpc.QueryBalanceRequest) (*counterrpc.QueryBalanceResponse, error) {
	if req == nil || req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	resp := &counterrpc.QueryBalanceResponse{}
	if req.Asset != "" {
		bal := s.svc.QueryBalance(req.UserId, req.Asset)
		resp.Balances = []*counterrpc.Balance{{
			Asset:     req.Asset,
			Available: bal.Available.String(),
			Frozen:    bal.Frozen.String(),
		}}
		return resp, nil
	}
	for asset, bal := range s.svc.QueryAccount(req.UserId) {
		resp.Balances = append(resp.Balances, &counterrpc.Balance{
			Asset:     asset,
			Available: bal.Available.String(),
			Frozen:    bal.Frozen.String(),
		})
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// proto <-> internal helpers
// ---------------------------------------------------------------------------

func transferRequestFromProto(req *counterrpc.TransferRequest) (engine.TransferRequest, error) {
	amount, err := dec.Parse(req.Amount)
	if err != nil {
		return engine.TransferRequest{}, fmt.Errorf("invalid amount %q: %w", req.Amount, err)
	}
	t, err := transferTypeFromProto(req.Type)
	if err != nil {
		return engine.TransferRequest{}, err
	}
	return engine.TransferRequest{
		TransferID: req.TransferId,
		UserID:     req.UserId,
		Asset:      req.Asset,
		Amount:     amount,
		Type:       t,
		BizRefID:   req.BizRefId,
		Memo:       req.Memo,
	}, nil
}

func transferTypeFromProto(t counterrpc.TransferType) (engine.TransferType, error) {
	switch t {
	case counterrpc.TransferType_TRANSFER_TYPE_DEPOSIT:
		return engine.TransferDeposit, nil
	case counterrpc.TransferType_TRANSFER_TYPE_WITHDRAW:
		return engine.TransferWithdraw, nil
	case counterrpc.TransferType_TRANSFER_TYPE_FREEZE:
		return engine.TransferFreeze, nil
	case counterrpc.TransferType_TRANSFER_TYPE_UNFREEZE:
		return engine.TransferUnfreeze, nil
	default:
		return 0, fmt.Errorf("invalid transfer type: %v", t)
	}
}

func transferResponseToProto(res *engine.TransferResult) *counterrpc.TransferResponse {
	return &counterrpc.TransferResponse{
		TransferId:     res.TransferID,
		Status:         transferStatusToProto(res.Status),
		RejectReason:   res.RejectReason,
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}
}

func transferStatusToProto(s engine.TransferStatus) counterrpc.TransferStatus {
	switch s {
	case engine.TransferStatusConfirmed:
		return counterrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED
	case engine.TransferStatusRejected:
		return counterrpc.TransferStatus_TRANSFER_STATUS_REJECTED
	case engine.TransferStatusDuplicated:
		return counterrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED
	default:
		return counterrpc.TransferStatus_TRANSFER_STATUS_UNSPECIFIED
	}
}

// mapServiceError converts service-layer errors to gRPC status codes.
func mapServiceError(err error) error {
	switch {
	case errors.Is(err, service.ErrMissingUserID),
		errors.Is(err, service.ErrMissingTransferID),
		errors.Is(err, service.ErrMissingAsset):
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
