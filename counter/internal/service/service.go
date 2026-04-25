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
	"github.com/xargin/opentrade/counter/engine"
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
	// DefaultMaxOpenLimitOrders is the ADR-0054 cap applied when a symbol's
	// SymbolConfig.MaxOpenLimitOrders is zero (or symbolLookup is nil).
	// Zero here = compatibility mode (cap disabled). main supplies 100 by
	// default; tests leave zero for skip-the-check behaviour.
	DefaultMaxOpenLimitOrders uint32
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

	// symbolLookup, when non-nil, is consulted during PlaceOrder to
	// enforce ADR-0053 precision filters (M3). Nil = compatibility mode
	// (no filter applied). Wired by main via SetSymbolLookup after the
	// etcd symbol-config watcher starts.
	symbolLookup SymbolLookup
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

// SetSymbolLookup wires the ADR-0053 M3 precision source. Passing nil (or
// never calling this method) keeps PlaceOrder in compatibility mode — no
// precision filter is enforced. Ops enables enforcement by pointing this
// at an etcd-backed registry (see main.go).
func (s *Service) SetSymbolLookup(fn SymbolLookup) {
	s.symbolLookup = fn
}

// HandleTradeRecord is the Kafka-aware sibling of HandleTradeEvent.
// Implements journal.TradeHandler for the synchronous consumer path
// used by tests and the pre-ADR-0060 loop; production runs the async
// path through HandleTradeRecordAsync.
//
// partition / offset are accepted to satisfy the interface; offset
// tracking now lives entirely in trade-dump's shadow engine
// (ADR-0061), which is the sole snapshot producer.
func (s *Service) HandleTradeRecord(ctx context.Context, evt *eventpb.TradeEvent, _ int32, _ int64) error {
	return s.HandleTradeEvent(ctx, evt)
}

// HandleTradeRecordAsync is the ADR-0060 fire-and-forget entry point
// for the VShardWorker consumer loop. Delegates to HandleTradeEventAsync
// — the callback contract is defined there.
//
// The Kafka offset is NOT stamped by this method. The advancer
// publishes TECheckpointEvent so trade-dump's shadow pipeline (the
// sole snapshot producer post ADR-0061) records the watermark.
func (s *Service) HandleTradeRecordAsync(ctx context.Context, evt *eventpb.TradeEvent, onCount func(int32), cb func(err error)) {
	s.HandleTradeEventAsync(ctx, evt, onCount, cb)
}

// ShardID returns the numeric shard id stamped on this Service.
func (s *Service) ShardID() int { return s.cfg.ShardID }

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

	// Fast-path: skip the sequencer if this transfer_id was recently applied.
	// ADR-0048 backlog item 4 方案 A — the response lacks balance_after /
	// counter_seq_id (see dedupResult). Callers that need the post-transfer
	// balance must follow up with QueryBalance.
	if s.state.Account(req.UserID).TransferSeen(req.TransferID) {
		return dedupResult(req.TransferID), nil
	}

	v, err := s.seq.Execute(req.UserID, func(counterSeq uint64) (any, error) {
		acc := s.state.Account(req.UserID)
		// Re-check inside the sequencer so concurrent arrivals of the same
		// transfer_id collapse to one CONFIRMED + (N-1) DUPLICATED.
		if acc.TransferSeen(req.TransferID) {
			return dedupResult(req.TransferID), nil
		}

		// Compute.
		newBalance, cerr := s.state.ComputeTransfer(req)
		if cerr != nil {
			// Business rejection — no Kafka write, no ring remember. The
			// caller may retry with fixed params.
			return &engine.TransferResult{
				TransferID:   req.TransferID,
				Status:       engine.TransferStatusRejected,
				RejectReason: cerr.Error(),
				CounterSeqID: counterSeq,
			}, nil
		}

		// Project post-commit versions so the journal event matches what
		// state will look like after CommitBalance below (ADR-0048 backlog:
		// 双层 version). setBalance always bumps both, so "+1" is exact.
		currentBal := acc.Balance(req.Asset)
		newBalance.Version = currentBal.Version + 1
		expectedAccVer := acc.Version() + 1

		// Build + publish counter-journal event.
		evt, err := journal.BuildTransferEvent(journal.TransferEventInput{
			CounterSeqID:   counterSeq,
			ProducerID:     s.cfg.ProducerID,
			AccountVersion: expectedAccVer,
			Req:            req,
			BalanceAfter:   newBalance,
		})
		if err != nil {
			return nil, fmt.Errorf("build event: %w", err)
		}
		if err := s.publisher.Publish(ctx, req.UserID, evt); err != nil {
			// Kafka failed — state untouched, caller may retry with same
			// transfer_id. We deliberately do NOT remember the id here:
			// a partial (published-but-not-committed) state would keep the
			// retry from running, and we want the next retry to finish the
			// idempotent re-publish via Kafka's own dedup on transfer_id.
			return nil, fmt.Errorf("publish: %w", err)
		}

		// Commit + remember.
		s.state.CommitBalance(req.UserID, req.Asset, newBalance)
		acc.RememberTransfer(req.TransferID)
		return &engine.TransferResult{
			TransferID:   req.TransferID,
			Status:       engine.TransferStatusConfirmed,
			BalanceAfter: acc.Balance(req.Asset), // read back to pick up bumped version
			CounterSeqID: counterSeq,
		}, nil
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

// dedupResult is the ADR-0048-era canonical duplicate response. The ring
// only remembers the id, not the cached response payload; callers that
// need the balance_after must follow up with QueryBalance. This is a
// deliberate breaking simplification (see ADR-0048 backlog item 4, 方案 A
// + documented drawbacks).
func dedupResult(transferID string) *engine.TransferResult {
	return &engine.TransferResult{
		TransferID: transferID,
		Status:     engine.TransferStatusDuplicated,
	}
}
