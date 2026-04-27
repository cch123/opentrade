package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/pkg/counterstate"
	"github.com/xargin/opentrade/pkg/dec"
)

// AssetHolderServer wires Counter's internal service into the
// AssetHolder Connect handler defined in api/rpc/assetholder/
// assetholder.proto (ADR-0057). Counter implements the biz_line=spot
// slot of the saga; asset-service orchestrates the cross-biz_line flow
// and calls these methods on Counter as one leg of each saga.
//
// All three methods share a common shape: they translate the incoming
// request into a counter counterstate.TransferRequest (with SagaTransferID
// filled from the incoming transfer_id) and call svc.Transfer. Counter's
// existing per-user sequencer + ring-buffer dedup + counter-journal
// publisher take care of idempotency and durability.
type AssetHolderServer struct {
	router Router
}

// NewAssetHolderServer constructs an AssetHolderServer backed by the
// same Router as the primary CounterService endpoint, so each user's
// AssetHolder traffic lands on the same vshard that owns their
// sequencer + state + dedup.
func NewAssetHolderServer(router Router) *AssetHolderServer {
	return &AssetHolderServer{router: router}
}

// routeOrFail is the AssetHolder counterpart of Server.routeOrFail:
// resolves user_id → Service or replies FailedPrecondition when this
// node doesn't (yet) own the user's vshard.
func (s *AssetHolderServer) routeOrFail(userID string) (*service.Service, error) {
	if userID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	svc, ok := s.router.Lookup(userID)
	if !ok {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("service: user does not belong to this node"))
	}
	return svc, nil
}

// TransferOut debits (user_id, asset) by amount. It is the "from" leg of
// an asset-service saga when Counter is the source biz_line. The RPC
// translates to counterstate.TransferRequest{Type: Withdraw, SagaTransferID:
// req.TransferId}; Counter's existing business logic handles precision,
// underflow rejection, and counter-journal emission.
func (s *AssetHolderServer) TransferOut(ctx context.Context, req *connect.Request[assetholderrpc.TransferOutRequest]) (*connect.Response[assetholderrpc.TransferOutResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	internalReq, err := holderRequestToEngine(holderRequestInput{
		UserID:     m.UserId,
		TransferID: m.TransferId,
		Asset:      m.Asset,
		Amount:     m.Amount,
		PeerBiz:    m.PeerBiz,
		Memo:       m.Memo,
		Direction:  counterstate.TransferWithdraw,
	})
	if err != nil {
		return nil, holderArgumentError(err)
	}
	svc, err := s.routeOrFail(internalReq.UserID)
	if err != nil {
		return nil, err
	}
	res, err := svc.Transfer(ctx, internalReq)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return connect.NewResponse(&assetholderrpc.TransferOutResponse{
		Status:         holderStatusToProto(res.Status),
		RejectReason:   rejectReasonToProto(res.RejectReason),
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}), nil
}

// TransferIn credits (user_id, asset) by amount. It is the "to" leg of
// an asset-service saga when Counter is the destination biz_line.
// Internally mirrors TransferOut with Direction = Deposit.
func (s *AssetHolderServer) TransferIn(ctx context.Context, req *connect.Request[assetholderrpc.TransferInRequest]) (*connect.Response[assetholderrpc.TransferInResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	internalReq, err := holderRequestToEngine(holderRequestInput{
		UserID:     m.UserId,
		TransferID: m.TransferId,
		Asset:      m.Asset,
		Amount:     m.Amount,
		PeerBiz:    m.PeerBiz,
		Memo:       m.Memo,
		Direction:  counterstate.TransferDeposit,
	})
	if err != nil {
		return nil, holderArgumentError(err)
	}
	svc, err := s.routeOrFail(internalReq.UserID)
	if err != nil {
		return nil, err
	}
	res, err := svc.Transfer(ctx, internalReq)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return connect.NewResponse(&assetholderrpc.TransferInResponse{
		Status:         holderStatusToProto(res.Status),
		RejectReason:   rejectReasonToProto(res.RejectReason),
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}), nil
}

// CompensateTransferOut reverses a previously-CONFIRMED TransferOut on
// the same saga. Semantically equivalent to TransferIn but the memo is
// prefixed with a compensate marker so audit consumers (trade-dump,
// reconciliation jobs) can distinguish normal credits from saga
// compensations. The SagaTransferID MUST equal the original TransferOut
// transfer_id; Counter's ring-buffer dedup then makes repeated
// CompensateTransferOut calls idempotent.
//
// Note: although this RPC shares Counter's dedup space with TransferIn /
// TransferOut (all keyed by req.TransferId), asset-service is expected
// to use a derived, distinct transfer_id for the compensate leg (e.g.
// "<saga>-compensate") so the compensate credit is not collapsed with a
// possible earlier in/out under the same id. That policy lives in
// asset-service; Counter just honours whatever transfer_id it receives.
func (s *AssetHolderServer) CompensateTransferOut(ctx context.Context, req *connect.Request[assetholderrpc.CompensateTransferOutRequest]) (*connect.Response[assetholderrpc.CompensateTransferOutResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	memo := fmt.Sprintf("compensate: peer=%s cause=%s", m.PeerBiz, m.CompensateCause)
	internalReq, err := holderRequestToEngine(holderRequestInput{
		UserID:     m.UserId,
		TransferID: m.TransferId,
		Asset:      m.Asset,
		Amount:     m.Amount,
		PeerBiz:    m.PeerBiz,
		Memo:       memo,
		Direction:  counterstate.TransferDeposit,
	})
	if err != nil {
		return nil, holderArgumentError(err)
	}
	svc, err := s.routeOrFail(internalReq.UserID)
	if err != nil {
		return nil, err
	}
	res, err := svc.Transfer(ctx, internalReq)
	if err != nil {
		return nil, mapServiceError(err)
	}
	return connect.NewResponse(&assetholderrpc.CompensateTransferOutResponse{
		Status:         holderStatusToProto(res.Status),
		RejectReason:   rejectReasonToProto(res.RejectReason),
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}), nil
}

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

type holderRequestInput struct {
	UserID     string
	TransferID string
	Asset      string
	Amount     string
	PeerBiz    string
	Memo       string
	Direction  counterstate.TransferType // TransferWithdraw (out) or TransferDeposit (in / compensate)
}

// holderRequestToEngine validates the RPC shape and returns the
// corresponding counterstate.TransferRequest. The saga's transfer_id is
// stamped into BOTH the dedup key (counterstate.TransferRequest.TransferID)
// AND the cross-ref field (counterstate.TransferRequest.SagaTransferID) so
// downstream trade-dump projections can correlate either way.
func holderRequestToEngine(in holderRequestInput) (counterstate.TransferRequest, error) {
	if in.UserID == "" {
		return counterstate.TransferRequest{}, errors.New("user_id required")
	}
	if in.TransferID == "" {
		return counterstate.TransferRequest{}, errors.New("transfer_id required")
	}
	if in.Asset == "" {
		return counterstate.TransferRequest{}, errors.New("asset required")
	}
	amount, err := dec.Parse(in.Amount)
	if err != nil {
		return counterstate.TransferRequest{}, fmt.Errorf("invalid amount %q: %w", in.Amount, err)
	}
	if amount.Sign() <= 0 {
		return counterstate.TransferRequest{}, errors.New("amount must be positive")
	}
	return counterstate.TransferRequest{
		TransferID:     in.TransferID,
		UserID:         in.UserID,
		Asset:          in.Asset,
		Amount:         amount,
		Type:           in.Direction,
		BizRefID:       in.PeerBiz, // saga counterparty; audit-only
		Memo:           in.Memo,
		SagaTransferID: in.TransferID,
	}, nil
}

func holderArgumentError(err error) error {
	return connect.NewError(connect.CodeInvalidArgument, err)
}

// holderStatusToProto maps counterstate.TransferStatus to the AssetHolder
// TransferStatus enum (which is identical in shape but declared in a
// different proto package).
func holderStatusToProto(s counterstate.TransferStatus) assetholderrpc.TransferStatus {
	switch s {
	case counterstate.TransferStatusConfirmed:
		return assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED
	case counterstate.TransferStatusRejected:
		return assetholderrpc.TransferStatus_TRANSFER_STATUS_REJECTED
	case counterstate.TransferStatusDuplicated:
		return assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED
	}
	return assetholderrpc.TransferStatus_TRANSFER_STATUS_UNSPECIFIED
}

// rejectReasonToProto folds Counter's free-form RejectReason string into
// the AssetHolder RejectReason enum. Counter's reject reasons come from
// counterstate.ErrInsufficientAvailable / ErrInsufficientFrozen /
// ErrInvalidAmount etc. — we match on the string form because Counter
// currently surfaces them via cerr.Error() (see service.Transfer).
func rejectReasonToProto(s string) assetholderrpc.RejectReason {
	switch s {
	case "":
		return assetholderrpc.RejectReason_REJECT_REASON_UNSPECIFIED
	case counterstate.ErrInsufficientAvailable.Error(),
		counterstate.ErrInsufficientFrozen.Error():
		return assetholderrpc.RejectReason_REJECT_REASON_INSUFFICIENT_BALANCE
	case counterstate.ErrInvalidAmount.Error():
		return assetholderrpc.RejectReason_REJECT_REASON_AMOUNT_INVALID
	}
	return assetholderrpc.RejectReason_REJECT_REASON_INTERNAL
}
