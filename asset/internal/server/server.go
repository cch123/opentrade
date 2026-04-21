// Package server adapts asset-service's internal Service to two gRPC
// surfaces:
//
//   - AssetHolder  (api/rpc/assetholder): TransferOut / TransferIn /
//                  CompensateTransferOut. asset-service implements this
//                  as the biz_line=funding holder, on equal footing
//                  with counter's biz_line=spot implementation.
//   - AssetService (api/rpc/asset): QueryFundingBalance + Transfer (the
//                  saga orchestrator entrypoint). M3a wires QueryFunding-
//                  Balance only; Transfer + QueryTransfer land in M3b.
package server

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	assetrpc "github.com/xargin/opentrade/api/gen/rpc/asset"
	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
)

// AssetHolderServer implements assetholderrpc.AssetHolderServer.
type AssetHolderServer struct {
	assetholderrpc.UnimplementedAssetHolderServer
	svc *service.Service
}

// NewAssetHolderServer wires an AssetHolderServer over svc.
func NewAssetHolderServer(svc *service.Service) *AssetHolderServer {
	return &AssetHolderServer{svc: svc}
}

// TransferOut debits the funding wallet for (user_id, asset).
func (s *AssetHolderServer) TransferOut(ctx context.Context, req *assetholderrpc.TransferOutRequest) (*assetholderrpc.TransferOutResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	hreq, err := buildHolderReq(req.UserId, req.TransferId, req.Asset, req.Amount, req.PeerBiz, req.Memo, "")
	if err != nil {
		return nil, err
	}
	res, err := s.svc.TransferOut(ctx, hreq)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &assetholderrpc.TransferOutResponse{
		Status:         holderStatusToProto(res.Status),
		RejectReason:   rejectReasonToProto(res.RejectReason),
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}, nil
}

// TransferIn credits the funding wallet.
func (s *AssetHolderServer) TransferIn(ctx context.Context, req *assetholderrpc.TransferInRequest) (*assetholderrpc.TransferInResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	hreq, err := buildHolderReq(req.UserId, req.TransferId, req.Asset, req.Amount, req.PeerBiz, req.Memo, "")
	if err != nil {
		return nil, err
	}
	res, err := s.svc.TransferIn(ctx, hreq)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &assetholderrpc.TransferInResponse{
		Status:         holderStatusToProto(res.Status),
		RejectReason:   rejectReasonToProto(res.RejectReason),
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}, nil
}

// CompensateTransferOut reverses a previously-debited TransferOut.
func (s *AssetHolderServer) CompensateTransferOut(ctx context.Context, req *assetholderrpc.CompensateTransferOutRequest) (*assetholderrpc.CompensateTransferOutResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	hreq, err := buildHolderReq(req.UserId, req.TransferId, req.Asset, req.Amount, req.PeerBiz, "", req.CompensateCause)
	if err != nil {
		return nil, err
	}
	res, err := s.svc.Compensate(ctx, hreq)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &assetholderrpc.CompensateTransferOutResponse{
		Status:         holderStatusToProto(res.Status),
		RejectReason:   rejectReasonToProto(res.RejectReason),
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}, nil
}

// ---------------------------------------------------------------------------
// AssetService surface (M3a: QueryFundingBalance only)
// ---------------------------------------------------------------------------

// AssetServer implements assetrpc.AssetServiceServer. Transfer +
// QueryTransfer return Unimplemented until M3b wires the saga
// orchestrator.
type AssetServer struct {
	assetrpc.UnimplementedAssetServiceServer
	svc *service.Service
}

// NewAssetServer wires an AssetServer.
func NewAssetServer(svc *service.Service) *AssetServer {
	return &AssetServer{svc: svc}
}

// QueryFundingBalance returns funding balances for the user. asset ==
// "" returns every tracked asset; when asset is given and the user has
// no record the response includes a single zero-valued entry (same
// shape as counter's QueryBalance).
func (s *AssetServer) QueryFundingBalance(_ context.Context, req *assetrpc.QueryFundingBalanceRequest) (*assetrpc.QueryFundingBalanceResponse, error) {
	if req == nil || req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	all := s.svc.QueryFundingBalance(req.UserId, req.Asset)
	out := &assetrpc.QueryFundingBalanceResponse{
		Balances: make([]*assetrpc.FundingBalance, 0, len(all)),
	}
	for _, b := range all {
		out.Balances = append(out.Balances, &assetrpc.FundingBalance{
			Asset:     b.Asset,
			Available: b.Balance.Available.String(),
			Frozen:    b.Balance.Frozen.String(),
			Version:   b.Balance.Version,
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// buildHolderReq validates the RPC shape and returns a service-layer
// HolderRequest. Input-validation errors surface as InvalidArgument;
// business rejections (e.g. insufficient balance) ride on the
// TransferStatus/RejectReason response body instead (so idempotent
// retries can observe the same terminal outcome without exception
// handling).
func buildHolderReq(userID, transferID, asset, amount, peerBiz, memo, compensateCause string) (service.HolderRequest, error) {
	if userID == "" {
		return service.HolderRequest{}, status.Error(codes.InvalidArgument, "user_id required")
	}
	if transferID == "" {
		return service.HolderRequest{}, status.Error(codes.InvalidArgument, "transfer_id required")
	}
	if asset == "" {
		return service.HolderRequest{}, status.Error(codes.InvalidArgument, "asset required")
	}
	amt, err := dec.Parse(amount)
	if err != nil {
		return service.HolderRequest{}, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid amount %q: %v", amount, err))
	}
	if amt.Sign() <= 0 {
		return service.HolderRequest{}, status.Error(codes.InvalidArgument, "amount must be positive")
	}
	return service.HolderRequest{
		UserID:          userID,
		TransferID:      transferID,
		Asset:           asset,
		PeerBiz:         peerBiz,
		Memo:            memo,
		CompensateCause: compensateCause,
		Amount: engine.TransferRequest{
			UserID:     userID,
			TransferID: transferID,
			Asset:      asset,
			Amount:     amt,
		},
	}, nil
}

func holderStatusToProto(s service.Status) assetholderrpc.TransferStatus {
	switch s {
	case service.StatusConfirmed:
		return assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED
	case service.StatusRejected:
		return assetholderrpc.TransferStatus_TRANSFER_STATUS_REJECTED
	case service.StatusDuplicated:
		return assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED
	}
	return assetholderrpc.TransferStatus_TRANSFER_STATUS_UNSPECIFIED
}

// rejectReasonToProto folds engine errors into the AssetHolder
// RejectReason enum. Engine exposes a narrow set (insufficient balance,
// invalid amount, missing fields); validation errors short-circuit at
// buildHolderReq before reaching the engine.
func rejectReasonToProto(err error) assetholderrpc.RejectReason {
	switch {
	case err == nil:
		return assetholderrpc.RejectReason_REJECT_REASON_UNSPECIFIED
	case errors.Is(err, engine.ErrInsufficientAvailable):
		return assetholderrpc.RejectReason_REJECT_REASON_INSUFFICIENT_BALANCE
	case errors.Is(err, engine.ErrInvalidAmount):
		return assetholderrpc.RejectReason_REJECT_REASON_AMOUNT_INVALID
	}
	return assetholderrpc.RejectReason_REJECT_REASON_INTERNAL
}
