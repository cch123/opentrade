// Package service is the business layer of Counter. It ties together:
//
//   - engine.ShardState    — in-memory accounts and transfer logic
//   - sequencer.UserSequencer — per-user FIFO serializer (ADR-0018)
//   - dedup.Table          — transfer_id idempotency (ADR-0011)
//   - Publisher            — Kafka counter-journal producer (ADR-0004)
//
// The gRPC server in internal/server adapts this to protobuf.
package service

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/counter/internal/sequencer"
)

// Input-validation errors. Surfaced as gRPC InvalidArgument by the server
// layer.
var (
	ErrMissingUserID     = errors.New("service: user_id is required")
	ErrMissingTransferID = errors.New("service: transfer_id is required")
	ErrMissingAsset      = errors.New("service: asset is required")
)

// Publisher is the minimal Kafka interface Service depends on. Having an
// interface here keeps Service unit-testable without a live Kafka.
type Publisher interface {
	Publish(ctx context.Context, partitionKey string, evt *eventpb.CounterJournalEvent) error
}

// Config configures the Service.
type Config struct {
	ShardID    int    // numeric shard id (0..N-1)
	ProducerID string // stamped on every CounterJournalEvent (ADR-0017 for future use)
}

// Service executes Counter business operations.
type Service struct {
	cfg       Config
	state     *engine.ShardState
	seq       *sequencer.UserSequencer
	dedup     *dedup.Table
	publisher Publisher
	logger    *zap.Logger
}

// New wires a Service. All dependencies must be non-nil.
func New(cfg Config, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, publisher Publisher, logger *zap.Logger) *Service {
	return &Service{
		cfg:       cfg,
		state:     state,
		seq:       seq,
		dedup:     dt,
		publisher: publisher,
		logger:    logger,
	}
}

// Transfer is the unified deposit / withdraw / freeze / unfreeze entry point
// (ADR-0011). Flow:
//
//  1. Validate input.
//  2. Fast-path dedup: if transfer_id was already confirmed, return cached.
//  3. Enter the user's FIFO sequencer.
//  4. Inside the sequencer:
//     a. Re-check dedup (race protection).
//     b. Compute post-balance (pure; no state mutation).
//     c. Return REJECTED if balance would underflow / args invalid.
//     d. Build journal event and publish to Kafka.
//     e. On Kafka success: commit balance in memory, cache result.
//
// The "compute → publish → commit" order ensures Kafka is always strictly
// ahead of in-memory state, matching ADR-0001.
func (s *Service) Transfer(ctx context.Context, req engine.TransferRequest) (*engine.TransferResult, error) {
	if err := validateTransfer(req); err != nil {
		return nil, err
	}

	// Fast-path: return cached response without entering the sequencer.
	if cached := s.dedupHit(req.TransferID); cached != nil {
		return cached, nil
	}

	v, err := s.seq.Execute(req.UserID, func(seq uint64) (any, error) {
		// Re-check dedup in case two concurrent requests raced into the queue.
		if cached := s.dedupHit(req.TransferID); cached != nil {
			return cached, nil
		}

		// Compute.
		newBalance, cerr := s.state.ComputeTransfer(req)
		if cerr != nil {
			// Business rejection — no Kafka write, no dedup cache. The caller
			// may retry with fixed params.
			return &engine.TransferResult{
				TransferID:   req.TransferID,
				Status:       engine.TransferStatusRejected,
				RejectReason: cerr.Error(),
				SeqID:        seq,
			}, nil
		}

		// Build + publish counter-journal event.
		evt, err := journal.BuildTransferEvent(journal.TransferEventInput{
			SeqID:        seq,
			ProducerID:   s.cfg.ProducerID,
			Req:          req,
			BalanceAfter: newBalance,
		})
		if err != nil {
			return nil, fmt.Errorf("build event: %w", err)
		}
		if err := s.publisher.Publish(ctx, req.UserID, evt); err != nil {
			// Kafka failed — state untouched, caller may retry with same
			// transfer_id.
			return nil, fmt.Errorf("publish: %w", err)
		}

		// Commit.
		s.state.CommitBalance(req.UserID, req.Asset, newBalance)
		result := &engine.TransferResult{
			TransferID:   req.TransferID,
			Status:       engine.TransferStatusConfirmed,
			BalanceAfter: newBalance,
			SeqID:        seq,
		}
		s.dedup.Set(req.TransferID, result)
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*engine.TransferResult), nil
}

// QueryBalance returns the current balance snapshot for (userID, asset).
// It reads state without going through the sequencer; readers may observe a
// balance while a concurrent write for the same user is in-flight. This is
// intentional — ADR-0007 treats query as a best-effort view; authoritative
// state is Kafka journal.
func (s *Service) QueryBalance(userID, asset string) engine.Balance {
	return s.state.Balance(userID, asset)
}

// QueryAccount returns the full map of (asset -> balance) for a user.
func (s *Service) QueryAccount(userID string) map[string]engine.Balance {
	return s.state.Account(userID).Copy()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func validateTransfer(req engine.TransferRequest) error {
	if req.UserID == "" {
		return ErrMissingUserID
	}
	if req.TransferID == "" {
		return ErrMissingTransferID
	}
	if req.Asset == "" {
		return ErrMissingAsset
	}
	return nil
}

func (s *Service) dedupHit(transferID string) *engine.TransferResult {
	if v, ok := s.dedup.Get(transferID); ok {
		if r, ok := v.(*engine.TransferResult); ok && r != nil {
			// Surface status as DUPLICATED so the caller can distinguish an
			// idempotent hit from a fresh CONFIRMED.
			dup := *r
			dup.Status = engine.TransferStatusDuplicated
			return &dup
		}
	}
	return nil
}
