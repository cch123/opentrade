package server

// assetholder.go exposes perp-counter as the biz_line=futures AssetHolder
// (ADR-0057): the asset-service saga calls TransferOut / TransferIn /
// CompensateTransferOut here as legs of a cross-biz_line transfer, so a user's
// funding→futures margin deposit lands in the perp wallet with no new transfer
// protocol (ADR-0068 §2). The service layer owns idempotency (transfer_id
// dedup), the per-user sequencer, and the PerpMarginEvent journal.

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/perp-counter/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
)

// AssetHolderServer adapts perp-counter's service to the AssetHolder contract.
type AssetHolderServer struct {
	svc *service.Service
}

// NewAssetHolderServer wires the handler to the perp service.
func NewAssetHolderServer(svc *service.Service) *AssetHolderServer {
	return &AssetHolderServer{svc: svc}
}

type holderInput struct {
	user                      uint64
	transferID, asset, amount string
}

func parseHolder(user uint64, transferID, asset, amount string) (holderInput, dec.Decimal, error) {
	if user == 0 {
		return holderInput{}, dec.Decimal{}, errors.New("user_id required")
	}
	if transferID == "" {
		return holderInput{}, dec.Decimal{}, errors.New("transfer_id required")
	}
	if asset == "" {
		return holderInput{}, dec.Decimal{}, errors.New("asset required")
	}
	amt, err := dec.Parse(amount)
	if err != nil || amt.Sign() <= 0 {
		return holderInput{}, dec.Decimal{}, errors.New("amount must be a positive decimal")
	}
	return holderInput{user, transferID, asset, amount}, amt, nil
}

func (s *AssetHolderServer) TransferOut(_ context.Context, req *connect.Request[assetholderrpc.TransferOutRequest]) (*connect.Response[assetholderrpc.TransferOutResponse], error) {
	m := req.Msg
	in, amt, err := parseHolder(m.GetUserId(), m.GetTransferId(), m.GetAsset(), m.GetAmount())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	r := s.svc.FuturesTransferOut(in.user, in.transferID, in.asset, amt)
	return connect.NewResponse(&assetholderrpc.TransferOutResponse{
		Status: holderStatus(r.Status), RejectReason: holderReject(r.RejectReason),
		AvailableAfter: r.AvailableAfter.String(), FrozenAfter: r.ReservedAfter.String(),
	}), nil
}

func (s *AssetHolderServer) TransferIn(_ context.Context, req *connect.Request[assetholderrpc.TransferInRequest]) (*connect.Response[assetholderrpc.TransferInResponse], error) {
	m := req.Msg
	in, amt, err := parseHolder(m.GetUserId(), m.GetTransferId(), m.GetAsset(), m.GetAmount())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	r := s.svc.FuturesTransferIn(in.user, in.transferID, in.asset, amt)
	return connect.NewResponse(&assetholderrpc.TransferInResponse{
		Status: holderStatus(r.Status), RejectReason: holderReject(r.RejectReason),
		AvailableAfter: r.AvailableAfter.String(), FrozenAfter: r.ReservedAfter.String(),
	}), nil
}

func (s *AssetHolderServer) CompensateTransferOut(_ context.Context, req *connect.Request[assetholderrpc.CompensateTransferOutRequest]) (*connect.Response[assetholderrpc.CompensateTransferOutResponse], error) {
	m := req.Msg
	in, amt, err := parseHolder(m.GetUserId(), m.GetTransferId(), m.GetAsset(), m.GetAmount())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	r := s.svc.FuturesCompensateTransferOut(in.user, in.transferID, in.asset, amt)
	return connect.NewResponse(&assetholderrpc.CompensateTransferOutResponse{
		Status: holderStatus(r.Status), RejectReason: holderReject(r.RejectReason),
		AvailableAfter: r.AvailableAfter.String(), FrozenAfter: r.ReservedAfter.String(),
	}), nil
}

func holderStatus(s service.TransferStatus) assetholderrpc.TransferStatus {
	switch s {
	case service.TransferConfirmed:
		return assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED
	case service.TransferRejected:
		return assetholderrpc.TransferStatus_TRANSFER_STATUS_REJECTED
	case service.TransferDuplicated:
		return assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED
	}
	return assetholderrpc.TransferStatus_TRANSFER_STATUS_UNSPECIFIED
}

func holderReject(reason string) assetholderrpc.RejectReason {
	switch reason {
	case "":
		return assetholderrpc.RejectReason_REJECT_REASON_UNSPECIFIED
	case "insufficient_available":
		return assetholderrpc.RejectReason_REJECT_REASON_INSUFFICIENT_BALANCE
	}
	return assetholderrpc.RejectReason_REJECT_REASON_INTERNAL
}
