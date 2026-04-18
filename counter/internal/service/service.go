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
	"github.com/xargin/opentrade/pkg/shard"
)

// Input-validation errors. Surfaced as gRPC InvalidArgument by the server
// layer.
var (
	ErrMissingUserID     = errors.New("service: user_id is required")
	ErrMissingTransferID = errors.New("service: transfer_id is required")
	ErrMissingAsset      = errors.New("service: asset is required")
	// ErrWrongShard means BFF routed a request for a user that does not
	// belong to this Counter shard. Surfaced as FailedPrecondition so clients
	// retry through the routing layer instead of against this instance.
	ErrWrongShard = errors.New("service: user does not belong to this shard")
)

// Publisher is the minimal non-transactional Kafka interface used by the
// Transfer path (single counter-journal produce).
type Publisher interface {
	Publish(ctx context.Context, partitionKey string, evt *eventpb.CounterJournalEvent) error
}

// TxnPublisher is the transactional interface used by PlaceOrder / CancelOrder
// (atomic counter-journal + order-event, ADR-0005).
type TxnPublisher interface {
	PublishOrderPlacement(
		ctx context.Context,
		journalEvt *eventpb.CounterJournalEvent,
		orderEvt *eventpb.OrderEvent,
		journalKey string,
		orderKey string,
	) error
}

// IDGen generates monotonically increasing order ids (snowflake).
type IDGen interface {
	Next() uint64
}

// Config configures the Service.
type Config struct {
	ShardID    int    // numeric shard id (0..N-1)
	ProducerID string // stamped on every CounterJournalEvent (ADR-0017 for future use)
	// TotalShards enables user→shard ownership checks. Zero disables the
	// guard (every user is considered owned — legacy single-shard mode
	// retained for tests and pre-MVP-8 deployments).
	TotalShards int
}

// Service executes Counter business operations.
type Service struct {
	cfg       Config
	state     *engine.ShardState
	seq       *sequencer.UserSequencer
	dedup     *dedup.Table
	publisher Publisher
	txn       TxnPublisher
	idgen     IDGen
	logger    *zap.Logger
}

// New wires a Service. All dependencies must be non-nil. txn / idgen may be
// nil if PlaceOrder / CancelOrder are not used (legacy MVP-2 path).
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

// SetOrderDeps configures the PlaceOrder / CancelOrder dependencies. Must be
// called before serving requests in MVP-3.
func (s *Service) SetOrderDeps(txn TxnPublisher, idgen IDGen) {
	s.txn = txn
	s.idgen = idgen
}

// OwnsUser reports whether userID belongs to this shard. Returns true when
// TotalShards==0 (guard disabled).
func (s *Service) OwnsUser(userID string) bool {
	if s.cfg.TotalShards <= 0 {
		return true
	}
	return shard.OwnsUser(s.cfg.ShardID, s.cfg.TotalShards, userID)
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
	if !s.OwnsUser(req.UserID) {
		return nil, ErrWrongShard
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
func (s *Service) QueryBalance(userID, asset string) (engine.Balance, error) {
	if !s.OwnsUser(userID) {
		return engine.Balance{}, ErrWrongShard
	}
	return s.state.Balance(userID, asset), nil
}

// QueryAccount returns the full map of (asset -> balance) for a user.
func (s *Service) QueryAccount(userID string) (map[string]engine.Balance, error) {
	if !s.OwnsUser(userID) {
		return nil, ErrWrongShard
	}
	return s.state.Account(userID).Copy(), nil
}

// QueryOrder returns a clone of the order if it exists and belongs to userID.
func (s *Service) QueryOrder(userID string, orderID uint64) (*engine.Order, error) {
	if !s.OwnsUser(userID) {
		return nil, ErrWrongShard
	}
	o := s.state.Orders().Get(orderID)
	if o == nil {
		return nil, engine.ErrOrderNotFound
	}
	if o.UserID != userID {
		return nil, engine.ErrNotOrderOwner
	}
	return o.Clone(), nil
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
