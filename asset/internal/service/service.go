// Package service is asset-service's business boundary between gRPC and the
// funding wallet store. ADR-0065 makes MySQL the authority for funding
// balances, so the service no longer owns in-memory wallet state or event
// publishing.
package service

import (
	"context"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/store"
)

// Status mirrors api/rpc/assetholder.TransferStatus values. The server
// layer translates between this enum and the proto one.
type Status int

const (
	StatusUnspecified Status = 0
	StatusConfirmed   Status = 1
	StatusRejected    Status = 2
	StatusDuplicated  Status = 3
)

// Result is what the service returns to the server layer.
type Result struct {
	Status         Status
	RejectReason   error // non-nil when Status == StatusRejected
	BalanceAfter   engine.Balance
	FundingVersion uint64
}

// ErrIdempotencyConflict is surfaced when a caller retries the same
// (transfer_id, op_type) with a different request shape.
var ErrIdempotencyConflict = store.ErrIdempotencyConflict

// FundingStore is the durable funding wallet contract used by Service.
type FundingStore interface {
	TransferOut(context.Context, store.Request) (store.Result, error)
	TransferIn(context.Context, store.Request) (store.Result, error)
	Compensate(context.Context, store.Request) (store.Result, error)
	QueryFundingBalance(context.Context, string, string) ([]store.FundingBalance, error)
}

// Service orchestrates the funding-wallet AssetHolder path.
type Service struct {
	funding FundingStore
	logger  *zap.Logger
}

// New constructs a Service.
func New(funding FundingStore, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{funding: funding, logger: logger}
}

// ---------------------------------------------------------------------------
// AssetHolder path
// ---------------------------------------------------------------------------

// HolderRequest captures everything the service needs to process any of
// the three AssetHolder RPCs. The server layer validates the amount
// string BEFORE calling into the service.
type HolderRequest struct {
	UserID          string
	TransferID      string
	Asset           string
	Amount          engine.TransferRequest // engine-typed amount carrier
	PeerBiz         string
	Memo            string
	CompensateCause string // only set on Compensate path
}

// TransferOut debits the funding wallet in MySQL. Returns StatusRejected
// when the store persists a business rejection, and StatusDuplicated when
// funding_mutations already has the same confirmed transfer.
func (s *Service) TransferOut(ctx context.Context, req HolderRequest) (Result, error) {
	res, err := s.funding.TransferOut(ctx, toStoreRequest(req))
	if err != nil {
		return Result{}, err
	}
	return fromStoreResult(res), nil
}

// TransferIn credits the funding wallet in MySQL.
func (s *Service) TransferIn(ctx context.Context, req HolderRequest) (Result, error) {
	res, err := s.funding.TransferIn(ctx, toStoreRequest(req))
	if err != nil {
		return Result{}, err
	}
	return fromStoreResult(res), nil
}

// Compensate credits the funding wallet using a distinct idempotency op_type.
func (s *Service) Compensate(ctx context.Context, req HolderRequest) (Result, error) {
	res, err := s.funding.Compensate(ctx, toStoreRequest(req))
	if err != nil {
		return Result{}, err
	}
	return fromStoreResult(res), nil
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

// QueryFundingBalance returns balances for the user. asset == "" returns all
// persisted assets.
func (s *Service) QueryFundingBalance(ctx context.Context, userID, asset string) ([]FundingBalance, error) {
	return s.funding.QueryFundingBalance(ctx, userID, asset)
}

// FundingBalance is the pair (asset, Balance). Used only by the query path.
type FundingBalance = store.FundingBalance

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func toStoreRequest(req HolderRequest) store.Request {
	return store.Request{
		UserID:          req.UserID,
		TransferID:      req.TransferID,
		Asset:           req.Asset,
		Amount:          req.Amount.Amount,
		PeerBiz:         req.PeerBiz,
		Memo:            req.Memo,
		CompensateCause: req.CompensateCause,
	}
}

func fromStoreResult(res store.Result) Result {
	return Result{
		Status:         fromStoreStatus(res.Status),
		RejectReason:   res.RejectReason,
		BalanceAfter:   res.BalanceAfter,
		FundingVersion: res.FundingVersion,
	}
}

func fromStoreStatus(s store.Status) Status {
	switch s {
	case store.StatusConfirmed:
		return StatusConfirmed
	case store.StatusRejected:
		return StatusRejected
	case store.StatusDuplicated:
		return StatusDuplicated
	}
	return StatusUnspecified
}
