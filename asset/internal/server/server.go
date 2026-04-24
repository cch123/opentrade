// Package server adapts asset-service's internal Service to two gRPC
// surfaces:
//
//   - AssetHolder  (api/rpc/assetholder): TransferOut / TransferIn /
//     CompensateTransferOut. asset-service implements this
//     as the biz_line=funding holder, on equal footing
//     with counter's biz_line=spot implementation.
//   - AssetService (api/rpc/asset): QueryFundingBalance + Transfer (the
//     saga orchestrator entrypoint). M3a wires QueryFunding-
//     Balance only; Transfer + QueryTransfer land in M3b.
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
	"github.com/xargin/opentrade/asset/internal/saga"
	"github.com/xargin/opentrade/asset/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/transferledger"
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
		return nil, holderServiceErrToStatus(err)
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
		return nil, holderServiceErrToStatus(err)
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
		return nil, holderServiceErrToStatus(err)
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
// QueryTransfer are wired in M3b via the saga orchestrator;
// QueryFundingBalance uses the funding service directly.
type AssetServer struct {
	assetrpc.UnimplementedAssetServiceServer
	svc  *service.Service
	orch *saga.Orchestrator
}

// NewAssetServer wires an AssetServer. orch may be nil for test setups
// that only exercise QueryFundingBalance; Transfer / QueryTransfer
// return FailedPrecondition in that case.
func NewAssetServer(svc *service.Service, orch *saga.Orchestrator) *AssetServer {
	return &AssetServer{svc: svc, orch: orch}
}

// Transfer implements the cross-biz_line saga orchestrator entrypoint.
// The call blocks until the saga reaches a terminal state (COMPLETED /
// FAILED / COMPENSATED / COMPENSATE_STUCK) or the RPC context is
// cancelled — the caller observes the outcome in TransferResponse.state
// on the same request. Non-terminal outcomes still return successfully
// with terminal=false so the client can follow up via QueryTransfer.
func (s *AssetServer) Transfer(ctx context.Context, req *assetrpc.TransferRequest) (*assetrpc.TransferResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	if s.orch == nil {
		return nil, status.Error(codes.FailedPrecondition, "saga orchestrator not configured")
	}
	if err := validateTransferAmount(req.Amount); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	out, err := s.orch.Transfer(ctx, saga.TransferInput{
		UserID:     req.UserId,
		TransferID: req.TransferId,
		FromBiz:    req.FromBiz,
		ToBiz:      req.ToBiz,
		Asset:      req.Asset,
		Amount:     req.Amount,
		Memo:       req.Memo,
	})
	if err != nil {
		return nil, sagaErrToStatus(err)
	}
	return &assetrpc.TransferResponse{
		TransferId:   out.TransferID,
		State:        sagaStateToProto(out.State),
		RejectReason: out.Reason,
		Terminal:     out.Terminal,
	}, nil
}

// QueryTransfer returns the current state of a saga. Used by BFF
// polling after a Transfer RPC returned terminal=false, and by
// reconciliation jobs. When req.UserId is non-empty, the result is
// guarded against cross-user access — a row belonging to a different
// user returns NOT_FOUND.
func (s *AssetServer) QueryTransfer(ctx context.Context, req *assetrpc.QueryTransferRequest) (*assetrpc.QueryTransferResponse, error) {
	if req == nil || req.TransferId == "" {
		return nil, status.Error(codes.InvalidArgument, "transfer_id required")
	}
	if s.orch == nil {
		return nil, status.Error(codes.FailedPrecondition, "saga orchestrator not configured")
	}
	e, err := s.orch.QueryEntry(ctx, req.TransferId)
	if err != nil {
		if errors.Is(err, transferledger.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "unknown transfer_id")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if req.UserId != "" && e.UserID != req.UserId {
		return nil, status.Error(codes.NotFound, "unknown transfer_id")
	}
	return &assetrpc.QueryTransferResponse{
		TransferId:      e.TransferID,
		UserId:          e.UserID,
		FromBiz:         e.FromBiz,
		ToBiz:           e.ToBiz,
		Asset:           e.Asset,
		Amount:          e.Amount,
		State:           ledgerStateToProto(e.State),
		RejectReason:    e.RejectReason,
		CreatedAtUnixMs: e.CreatedAtMs,
		UpdatedAtUnixMs: e.UpdatedAtMs,
	}, nil
}

// ListTransfers pages a user's saga rows newest-first by created_at_ms.
// Replaced the trade-dump `transfers` projection post ADR-0065.
func (s *AssetServer) ListTransfers(ctx context.Context, req *assetrpc.ListTransfersRequest) (*assetrpc.ListTransfersResponse, error) {
	if req == nil || req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	if s.orch == nil {
		return nil, status.Error(codes.FailedPrecondition, "saga orchestrator not configured")
	}
	states, err := buildListStates(req)
	if err != nil {
		return nil, err
	}
	filter := transferledger.ListFilter{
		UserID:  req.UserId,
		FromBiz: req.FromBiz,
		ToBiz:   req.ToBiz,
		Asset:   req.Asset,
		States:  states,
		SinceMs: req.SinceMs,
		UntilMs: req.UntilMs,
	}
	entries, next, err := s.orch.ListTransfers(ctx, filter, req.Cursor, int(req.Limit))
	if err != nil {
		if errors.Is(err, transferledger.ErrInvalidCursor) {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &assetrpc.ListTransfersResponse{
		Transfers: make([]*assetrpc.Transfer, 0, len(entries)),
	}
	for _, e := range entries {
		out.Transfers = append(out.Transfers, entryToProtoTransfer(e))
	}
	if next != (transferledger.Cursor{}) {
		cur, err := transferledger.EncodeCursor(next)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.NextCursor = cur
	}
	return out, nil
}

// buildListStates folds the scope enum to concrete states and merges
// any explicit states filter. Explicit states take precedence; scope
// folding only applies when states is empty.
func buildListStates(req *assetrpc.ListTransfersRequest) ([]transferledger.State, error) {
	if len(req.States) > 0 {
		out := make([]transferledger.State, 0, len(req.States))
		for _, s := range req.States {
			if !isKnownState(s) {
				return nil, status.Errorf(codes.InvalidArgument, "unknown state %q", s)
			}
			out = append(out, transferledger.State(s))
		}
		return out, nil
	}
	switch req.Scope {
	case assetrpc.TransferScope_TRANSFER_SCOPE_IN_FLIGHT:
		return append([]transferledger.State(nil), transferledger.InFlightStates...), nil
	case assetrpc.TransferScope_TRANSFER_SCOPE_TERMINAL:
		return append([]transferledger.State(nil), transferledger.TerminalStates...), nil
	}
	return nil, nil
}

func isKnownState(s string) bool {
	for _, v := range transferledger.AllStates {
		if string(v) == s {
			return true
		}
	}
	return false
}

func entryToProtoTransfer(e transferledger.Entry) *assetrpc.Transfer {
	return &assetrpc.Transfer{
		TransferId:      e.TransferID,
		UserId:          e.UserID,
		FromBiz:         e.FromBiz,
		ToBiz:           e.ToBiz,
		Asset:           e.Asset,
		Amount:          e.Amount,
		State:           string(e.State),
		RejectReason:    e.RejectReason,
		CreatedAtUnixMs: e.CreatedAtMs,
		UpdatedAtUnixMs: e.UpdatedAtMs,
	}
}

// QueryFundingBalance returns funding balances for the user. asset ==
// "" returns every tracked asset; when asset is given and the user has
// no record the response includes a single zero-valued entry (same
// shape as counter's QueryBalance).
func (s *AssetServer) QueryFundingBalance(ctx context.Context, req *assetrpc.QueryFundingBalanceRequest) (*assetrpc.QueryFundingBalanceResponse, error) {
	if req == nil || req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	all, err := s.svc.QueryFundingBalance(ctx, req.UserId, req.Asset)
	if err != nil {
		return nil, holderServiceErrToStatus(err)
	}
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

func holderServiceErrToStatus(err error) error {
	if errors.Is(err, service.ErrIdempotencyConflict) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// ---------------------------------------------------------------------------
// Saga helpers
// ---------------------------------------------------------------------------

// validateTransferAmount is a light pre-flight check so obviously bad
// amounts fail fast at the RPC boundary. The saga driver also relies
// on the holder's own validation; we duplicate it here for clearer
// error codes (InvalidArgument vs Internal).
func validateTransferAmount(amount string) error {
	if amount == "" {
		return fmt.Errorf("amount required")
	}
	amt, err := dec.Parse(amount)
	if err != nil {
		return fmt.Errorf("invalid amount %q: %w", amount, err)
	}
	if amt.Sign() <= 0 {
		return fmt.Errorf("amount must be positive")
	}
	return nil
}

// sagaStateToProto / ledgerStateToProto map the internal State enum
// (string-valued in transferledger) to the proto SagaState enum. Kept
// as two functions so each call site is explicit about whether it is
// projecting a TransferOutput or a raw ledger Entry.
func sagaStateToProto(s transferledger.State) assetrpc.SagaState {
	return ledgerStateToProto(s)
}

func ledgerStateToProto(s transferledger.State) assetrpc.SagaState {
	switch s {
	case transferledger.StateInit:
		return assetrpc.SagaState_SAGA_STATE_INIT
	case transferledger.StateDebited:
		return assetrpc.SagaState_SAGA_STATE_DEBITED
	case transferledger.StateCompleted:
		return assetrpc.SagaState_SAGA_STATE_COMPLETED
	case transferledger.StateFailed:
		return assetrpc.SagaState_SAGA_STATE_FAILED
	case transferledger.StateCompensating:
		return assetrpc.SagaState_SAGA_STATE_COMPENSATING
	case transferledger.StateCompensated:
		return assetrpc.SagaState_SAGA_STATE_COMPENSATED
	case transferledger.StateCompensateStuck:
		return assetrpc.SagaState_SAGA_STATE_COMPENSATE_STUCK
	}
	return assetrpc.SagaState_SAGA_STATE_UNSPECIFIED
}

// sagaErrToStatus maps orchestrator errors to gRPC status codes.
// Business errors (invalid shape) are InvalidArgument; unknown
// biz_line surfaces as FailedPrecondition; anything else is Internal.
func sagaErrToStatus(err error) error {
	switch {
	case errors.Is(err, saga.ErrInvalidRequest), errors.Is(err, saga.ErrSameBiz):
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
