// Package server adapts asset-service's internal Service to two
// Connect-Go surfaces:
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

	"connectrpc.com/connect"

	assetrpc "github.com/xargin/opentrade/api/gen/rpc/asset"
	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/saga"
	"github.com/xargin/opentrade/asset/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/transferledger"
)

// AssetHolderServer satisfies assetholderrpcconnect.AssetHolderHandler.
type AssetHolderServer struct {
	svc *service.Service
}

// NewAssetHolderServer wires an AssetHolderServer over svc.
func NewAssetHolderServer(svc *service.Service) *AssetHolderServer {
	return &AssetHolderServer{svc: svc}
}

// TransferOut debits the funding wallet for (user_id, asset).
func (s *AssetHolderServer) TransferOut(ctx context.Context, req *connect.Request[assetholderrpc.TransferOutRequest]) (*connect.Response[assetholderrpc.TransferOutResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	hreq, err := buildHolderReq(m.UserId, m.TransferId, m.Asset, m.Amount, m.PeerBiz, m.Memo, "")
	if err != nil {
		return nil, err
	}
	res, err := s.svc.TransferOut(ctx, hreq)
	if err != nil {
		return nil, holderServiceErrToConnect(err)
	}
	return connect.NewResponse(&assetholderrpc.TransferOutResponse{
		Status:         holderStatusToProto(res.Status),
		RejectReason:   rejectReasonToProto(res.RejectReason),
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}), nil
}

// TransferIn credits the funding wallet.
func (s *AssetHolderServer) TransferIn(ctx context.Context, req *connect.Request[assetholderrpc.TransferInRequest]) (*connect.Response[assetholderrpc.TransferInResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	hreq, err := buildHolderReq(m.UserId, m.TransferId, m.Asset, m.Amount, m.PeerBiz, m.Memo, "")
	if err != nil {
		return nil, err
	}
	res, err := s.svc.TransferIn(ctx, hreq)
	if err != nil {
		return nil, holderServiceErrToConnect(err)
	}
	return connect.NewResponse(&assetholderrpc.TransferInResponse{
		Status:         holderStatusToProto(res.Status),
		RejectReason:   rejectReasonToProto(res.RejectReason),
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}), nil
}

// CompensateTransferOut reverses a previously-debited TransferOut.
func (s *AssetHolderServer) CompensateTransferOut(ctx context.Context, req *connect.Request[assetholderrpc.CompensateTransferOutRequest]) (*connect.Response[assetholderrpc.CompensateTransferOutResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	hreq, err := buildHolderReq(m.UserId, m.TransferId, m.Asset, m.Amount, m.PeerBiz, "", m.CompensateCause)
	if err != nil {
		return nil, err
	}
	res, err := s.svc.Compensate(ctx, hreq)
	if err != nil {
		return nil, holderServiceErrToConnect(err)
	}
	return connect.NewResponse(&assetholderrpc.CompensateTransferOutResponse{
		Status:         holderStatusToProto(res.Status),
		RejectReason:   rejectReasonToProto(res.RejectReason),
		AvailableAfter: res.BalanceAfter.Available.String(),
		FrozenAfter:    res.BalanceAfter.Frozen.String(),
	}), nil
}

// ---------------------------------------------------------------------------
// AssetService surface (M3a: QueryFundingBalance only)
// ---------------------------------------------------------------------------

// AssetServer satisfies assetrpcconnect.AssetServiceHandler. Transfer +
// QueryTransfer are wired in M3b via the saga orchestrator;
// QueryFundingBalance uses the funding service directly.
type AssetServer struct {
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
func (s *AssetServer) Transfer(ctx context.Context, req *connect.Request[assetrpc.TransferRequest]) (*connect.Response[assetrpc.TransferResponse], error) {
	m := req.Msg
	if m == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("nil request"))
	}
	if s.orch == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("saga orchestrator not configured"))
	}
	if err := validateTransferAmount(m.Amount); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	out, err := s.orch.Transfer(ctx, saga.TransferInput{
		UserID:     m.UserId,
		TransferID: m.TransferId,
		FromBiz:    m.FromBiz,
		ToBiz:      m.ToBiz,
		Asset:      m.Asset,
		Amount:     m.Amount,
		Memo:       m.Memo,
	})
	if err != nil {
		return nil, sagaErrToConnect(err)
	}
	return connect.NewResponse(&assetrpc.TransferResponse{
		TransferId:   out.TransferID,
		State:        sagaStateToProto(out.State),
		RejectReason: out.Reason,
		Terminal:     out.Terminal,
	}), nil
}

// QueryTransfer returns the current state of a saga. Used by BFF
// polling after a Transfer RPC returned terminal=false, and by
// reconciliation jobs. When req.UserId is non-empty, the result is
// guarded against cross-user access — a row belonging to a different
// user returns NOT_FOUND.
func (s *AssetServer) QueryTransfer(ctx context.Context, req *connect.Request[assetrpc.QueryTransferRequest]) (*connect.Response[assetrpc.QueryTransferResponse], error) {
	m := req.Msg
	if m == nil || m.TransferId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("transfer_id required"))
	}
	if s.orch == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("saga orchestrator not configured"))
	}
	e, err := s.orch.QueryEntry(ctx, m.TransferId)
	if err != nil {
		if errors.Is(err, transferledger.ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("unknown transfer_id"))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if m.UserId != "" && e.UserID != m.UserId {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("unknown transfer_id"))
	}
	return connect.NewResponse(&assetrpc.QueryTransferResponse{
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
	}), nil
}

// ListTransfers pages a user's saga rows newest-first by created_at_ms.
// Replaced the trade-dump `transfers` projection post ADR-0065.
func (s *AssetServer) ListTransfers(ctx context.Context, req *connect.Request[assetrpc.ListTransfersRequest]) (*connect.Response[assetrpc.ListTransfersResponse], error) {
	m := req.Msg
	if m == nil || m.UserId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	if s.orch == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("saga orchestrator not configured"))
	}
	states, err := buildListStates(m)
	if err != nil {
		return nil, err
	}
	filter := transferledger.ListFilter{
		UserID:  m.UserId,
		FromBiz: m.FromBiz,
		ToBiz:   m.ToBiz,
		Asset:   m.Asset,
		States:  states,
		SinceMs: m.SinceMs,
		UntilMs: m.UntilMs,
	}
	entries, next, err := s.orch.ListTransfers(ctx, filter, m.Cursor, int(m.Limit))
	if err != nil {
		if errors.Is(err, transferledger.ErrInvalidCursor) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid cursor"))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
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
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		out.NextCursor = cur
	}
	return connect.NewResponse(out), nil
}

// buildListStates folds the scope enum to concrete states and merges
// any explicit states filter. Explicit states take precedence; scope
// folding only applies when states is empty.
func buildListStates(req *assetrpc.ListTransfersRequest) ([]transferledger.State, error) {
	if len(req.States) > 0 {
		out := make([]transferledger.State, 0, len(req.States))
		for _, s := range req.States {
			if !isKnownState(s) {
				return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown state %q", s))
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
func (s *AssetServer) QueryFundingBalance(ctx context.Context, req *connect.Request[assetrpc.QueryFundingBalanceRequest]) (*connect.Response[assetrpc.QueryFundingBalanceResponse], error) {
	m := req.Msg
	if m == nil || m.UserId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id is required"))
	}
	all, err := s.svc.QueryFundingBalance(ctx, m.UserId, m.Asset)
	if err != nil {
		return nil, holderServiceErrToConnect(err)
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
	return connect.NewResponse(out), nil
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
		return service.HolderRequest{}, connect.NewError(connect.CodeInvalidArgument, errors.New("user_id required"))
	}
	if transferID == "" {
		return service.HolderRequest{}, connect.NewError(connect.CodeInvalidArgument, errors.New("transfer_id required"))
	}
	if asset == "" {
		return service.HolderRequest{}, connect.NewError(connect.CodeInvalidArgument, errors.New("asset required"))
	}
	amt, err := dec.Parse(amount)
	if err != nil {
		return service.HolderRequest{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid amount %q: %v", amount, err))
	}
	if amt.Sign() <= 0 {
		return service.HolderRequest{}, connect.NewError(connect.CodeInvalidArgument, errors.New("amount must be positive"))
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

func holderServiceErrToConnect(err error) error {
	if errors.Is(err, service.ErrIdempotencyConflict) {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewError(connect.CodeInternal, err)
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

// sagaErrToConnect maps orchestrator errors to Connect codes.
// Business errors (invalid shape) are InvalidArgument; unknown
// biz_line surfaces as FailedPrecondition; anything else is Internal.
func sagaErrToConnect(err error) error {
	switch {
	case errors.Is(err, saga.ErrInvalidRequest), errors.Is(err, saga.ErrSameBiz):
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}
