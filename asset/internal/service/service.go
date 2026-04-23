// Package service is asset-service's business orchestration layer. It
// sits between the gRPC server (API boundary) and the engine (in-memory
// state) and handles the "validate → apply → journal" sequence shared
// by every AssetHolder method.
//
// For M3a the service only exposes the AssetHolder path (TransferOut /
// TransferIn / Compensate). The cross-biz_line saga orchestrator
// (AssetService.Transfer driving Counter's AssetHolder) lives in M3b
// and will sit alongside this file.
package service

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/journal"
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

// Config wires the service. ProducerID is stamped into every journal
// event's EventMeta so downstream consumers can attribute writes to a
// specific asset-service instance.
type Config struct {
	ProducerID string
}

// Service orchestrates the funding-wallet AssetHolder path.
type Service struct {
	cfg       Config
	state     *engine.State
	publisher journal.Publisher
	logger    *zap.Logger
}

// New constructs a Service.
func New(cfg Config, state *engine.State, pub journal.Publisher, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{cfg: cfg, state: state, publisher: pub, logger: logger}
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

// TransferOut debits the funding wallet and publishes a FundingTransferOut
// journal event. The balance mutation commits only after Kafka acks the
// journal event, so a failed publish leaves the transfer_id retryable.
// Returns StatusRejected with the engine error when the balance would go
// negative; StatusDuplicated when the transfer_id hit the idempotency ring.
func (s *Service) TransferOut(ctx context.Context, req HolderRequest) (Result, error) {
	var published Result
	res, err := s.state.ApplyTransferOutCommitted(req.Amount, func(res engine.Result) error {
		var err error
		published, err = s.publish(ctx, req, journal.KindTransferOut, res)
		return err
	})
	if err != nil {
		if isEngineReject(err) {
			return rejectFor(err), nil
		}
		s.logger.Warn("asset-journal publish failed",
			zap.String("transfer_id", req.TransferID),
			zap.Error(err))
		return Result{}, err
	}
	if res.Duplicated {
		return duplicated(res), nil
	}
	return published, nil
}

// TransferIn credits the funding wallet and publishes a FundingTransferIn
// journal event.
func (s *Service) TransferIn(ctx context.Context, req HolderRequest) (Result, error) {
	var published Result
	res, err := s.state.ApplyTransferInCommitted(req.Amount, func(res engine.Result) error {
		var err error
		published, err = s.publish(ctx, req, journal.KindTransferIn, res)
		return err
	})
	if err != nil {
		if isEngineReject(err) {
			return rejectFor(err), nil
		}
		s.logger.Warn("asset-journal publish failed",
			zap.String("transfer_id", req.TransferID),
			zap.Error(err))
		return Result{}, err
	}
	if res.Duplicated {
		return duplicated(res), nil
	}
	return published, nil
}

// Compensate credits the funding wallet (same math as TransferIn) but
// publishes a FundingCompensate journal event so audit /
// reconciliation can distinguish compensations from normal credits.
func (s *Service) Compensate(ctx context.Context, req HolderRequest) (Result, error) {
	var published Result
	res, err := s.state.ApplyCompensateCommitted(req.Amount, func(res engine.Result) error {
		var err error
		published, err = s.publish(ctx, req, journal.KindCompensate, res)
		return err
	})
	if err != nil {
		if isEngineReject(err) {
			return rejectFor(err), nil
		}
		s.logger.Warn("asset-journal publish failed",
			zap.String("transfer_id", req.TransferID),
			zap.Error(err))
		return Result{}, err
	}
	if res.Duplicated {
		return duplicated(res), nil
	}
	return published, nil
}

func isEngineReject(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, engine.ErrInsufficientAvailable),
		errors.Is(err, engine.ErrInvalidAmount),
		errors.Is(err, engine.ErrMissingUserID),
		errors.Is(err, engine.ErrMissingAsset),
		errors.Is(err, engine.ErrMissingTransferID):
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

// QueryFundingBalance returns balances for the user. asset == "" returns
// all assets.
func (s *Service) QueryFundingBalance(userID, asset string) []FundingBalance {
	acc := s.state.Account(userID)
	if asset != "" {
		bal := acc.Balance(asset)
		return []FundingBalance{{Asset: asset, Balance: bal}}
	}
	all := acc.Copy()
	out := make([]FundingBalance, 0, len(all))
	for a, b := range all {
		out = append(out, FundingBalance{Asset: a, Balance: b})
	}
	return out
}

// FundingBalance is the pair (asset, Balance). Used only by the query
// path.
type FundingBalance struct {
	Asset   string
	Balance engine.Balance
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// publish emits the journal event and wraps engine.Result into a
// service.Result. Callers run this from the engine's committed apply path, so
// a failed publish prevents the in-memory balance mutation from committing.
func (s *Service) publish(ctx context.Context, req HolderRequest, kind journal.EventKind, res engine.Result) (Result, error) {
	evt := journal.Build(journal.BuildInput{
		Kind:            kind,
		AssetSeqID:      s.publisher.NextSeq(),
		ProducerID:      s.cfg.ProducerID,
		FundingVersion:  res.FundingVersion,
		UserID:          req.UserID,
		TransferID:      req.TransferID,
		Asset:           req.Asset,
		Amount:          req.Amount.Amount.String(),
		PeerBiz:         req.PeerBiz,
		Memo:            req.Memo,
		CompensateCause: req.CompensateCause,
		BalanceAfter:    res.BalanceAfter,
	})
	if err := s.publisher.Publish(ctx, req.UserID, evt); err != nil {
		return Result{}, err
	}
	return Result{
		Status:         StatusConfirmed,
		BalanceAfter:   res.BalanceAfter,
		FundingVersion: res.FundingVersion,
	}, nil
}

func rejectFor(err error) Result {
	return Result{
		Status:       StatusRejected,
		RejectReason: err,
	}
}

func duplicated(res engine.Result) Result {
	return Result{
		Status:         StatusDuplicated,
		BalanceAfter:   res.BalanceAfter,
		FundingVersion: res.FundingVersion,
	}
}
