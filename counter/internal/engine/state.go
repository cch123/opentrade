package engine

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xargin/opentrade/pkg/dec"
)

// Common errors returned by state mutations. These indicate the request
// should be surfaced as a REJECTED transfer, not as an RPC error.
var (
	ErrInsufficientAvailable = errors.New("insufficient available balance")
	ErrInsufficientFrozen    = errors.New("insufficient frozen balance")
	ErrInvalidAmount         = errors.New("amount must be strictly positive")
	ErrUnknownTransferType   = errors.New("unknown transfer type")
)

// Account holds a single user's balances. Access to the internal map must be
// serialized by the UserSequencer (ADR-0018); callers must not touch it from
// outside that context, except via Copy which takes an internal lock for
// snapshotting.
type Account struct {
	UserID string

	// mu guards balances, matchSeq and version. In the common path the
	// UserSequencer already serializes per-user access and mu is
	// uncontended; mu exists so that snapshot readers can obtain a
	// consistent per-user view without going through the sequencer.
	mu       sync.RWMutex
	balances map[string]*Balance
	// matchSeq tracks the highest trade-event Meta.SeqId this user has seen
	// per symbol. Trade-event handlers gate on this value to filter replayed
	// records after a snapshot-restore (ADR-0048 backlog: trade-event
	// idempotency, user × symbol match_seq guard).
	matchSeq map[string]uint64
	// version is the user-level monotonic counter bumped on every balance
	// mutation (any asset). Paired with per-asset Balance.Version, this
	// forms the double-layer versioning scheme (ADR-0048 backlog: "双层
	// version 方案 B"). Clients can use version as an optimistic-lock /
	// cache-invalidation handle; trade-dump mirrors it to the accounts
	// projection.
	version uint64
}

func newAccount(userID string) *Account {
	return &Account{UserID: userID, balances: make(map[string]*Balance)}
}

// Balance returns a snapshot of the given asset's balance. Missing assets
// yield a zero balance.
func (a *Account) Balance(asset string) Balance {
	a.mu.RLock()
	defer a.mu.RUnlock()
	b, ok := a.balances[asset]
	if !ok {
		return Balance{}
	}
	return *b
}

// Copy returns a deep copy of the account's balances, safe to use outside
// the sequencer (e.g. for snapshotting).
func (a *Account) Copy() map[string]Balance {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make(map[string]Balance, len(a.balances))
	for k, v := range a.balances {
		out[k] = *v
	}
	return out
}

// setBalance writes the given asset's balance. Caller must hold per-user
// serialization (UserSequencer) or be doing a restore before traffic starts.
//
// This is the single entry point for production balance mutations and so
// owns the double-layer version bump: the asset's own Balance.Version +1
// and the user's Account.version +1. Callers MUST NOT pre-set b.Version
// themselves — any value they put there is ignored; the function always
// derives the new version from the stored current balance. For restore
// paths that need to preserve on-disk versions, use putBalanceForRestore.
func (a *Account) setBalance(asset string, b Balance) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.bumpAccountVersionLocked()
	if b.IsEmpty() {
		delete(a.balances, asset)
		return
	}
	cur, ok := a.balances[asset]
	if !ok {
		cur = &Balance{}
		a.balances[asset] = cur
	}
	prevVersion := cur.Version
	*cur = b
	cur.Version = prevVersion + 1
}

// bumpAccountVersionLocked bumps the user-level version. Caller MUST hold
// a.mu.
func (a *Account) bumpAccountVersionLocked() { a.version++ }

// putBalanceForRestore writes b verbatim — including b.Version — WITHOUT
// bumping any counters. Only used by snapshot.Restore before the shard
// starts serving traffic.
func (a *Account) putBalanceForRestore(asset string, b Balance) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if b.IsEmpty() && b.Version == 0 {
		delete(a.balances, asset)
		return
	}
	cur, ok := a.balances[asset]
	if !ok {
		cur = &Balance{}
		a.balances[asset] = cur
	}
	*cur = b
}

// PutForRestore is a restore-only hook used by the snapshot package to install
// a balance bypassing the normal transfer validation. Callers MUST ensure
// this runs before the shard starts serving traffic.
func (a *Account) PutForRestore(asset string, b Balance) {
	a.putBalanceForRestore(asset, b)
}

// Version returns the user-level version counter. Bumped on every balance
// mutation; stable across snapshot roundtrip.
func (a *Account) Version() uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.version
}

// RestoreVersion replaces the user-level version counter. Restore-only,
// same contract as PutForRestore.
func (a *Account) RestoreVersion(v uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.version = v
}

// LastMatchSeq returns the highest trade-event seq already applied for this
// (user, symbol). Zero means no event seen yet — callers MUST treat incoming
// seq 0 as "unset" (pre-ADR-0048 test fixtures / legacy) and skip the guard.
func (a *Account) LastMatchSeq(symbol string) uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.matchSeq[symbol]
}

// AdvanceMatchSeq bumps (symbol → seq). Monotonic: shorter-or-equal values
// are ignored so out-of-order arrivals never rewind the guard.
func (a *Account) AdvanceMatchSeq(symbol string, seq uint64) {
	if seq == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.matchSeq == nil {
		a.matchSeq = make(map[string]uint64)
	}
	if seq > a.matchSeq[symbol] {
		a.matchSeq[symbol] = seq
	}
}

// MatchSeqSnapshot returns a copy of the full per-symbol map, for snapshot
// serialization. Safe to call concurrently with AdvanceMatchSeq.
func (a *Account) MatchSeqSnapshot() map[string]uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.matchSeq) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(a.matchSeq))
	for k, v := range a.matchSeq {
		out[k] = v
	}
	return out
}

// RestoreMatchSeq replaces the per-symbol map (restore-only, same contract
// as PutForRestore).
func (a *Account) RestoreMatchSeq(m map[string]uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(m) == 0 {
		a.matchSeq = nil
		return
	}
	a.matchSeq = make(map[string]uint64, len(m))
	for k, v := range m {
		a.matchSeq[k] = v
	}
}

// ShardState is the in-memory state for one Counter shard.
type ShardState struct {
	ShardID int

	accounts     sync.Map // user_id → *Account
	orders       *OrderStore
	reservations *reservationStore
}

// NewShardState constructs an empty state.
func NewShardState(shardID int) *ShardState {
	return &ShardState{
		ShardID:      shardID,
		orders:       newOrderStore(),
		reservations: newReservationStore(),
	}
}

// Orders returns the in-memory order store.
func (s *ShardState) Orders() *OrderStore { return s.orders }

// Account returns the account for userID (creating on demand if it does not
// exist). Always non-nil.
func (s *ShardState) Account(userID string) *Account {
	if v, ok := s.accounts.Load(userID); ok {
		return v.(*Account)
	}
	actual, _ := s.accounts.LoadOrStore(userID, newAccount(userID))
	return actual.(*Account)
}

// Balance returns the balance of (userID, asset). Missing accounts / assets
// yield a zero balance.
func (s *ShardState) Balance(userID, asset string) Balance {
	if v, ok := s.accounts.Load(userID); ok {
		return v.(*Account).Balance(asset)
	}
	return Balance{}
}

// Users returns all user ids currently tracked. Order is non-deterministic.
// Useful for snapshot.
func (s *ShardState) Users() []string {
	var out []string
	s.accounts.Range(func(k, _ any) bool {
		out = append(out, k.(string))
		return true
	})
	return out
}

// ApplyTransfer validates req, mutates the relevant account, and returns the
// post-transfer balance. Used by tests / direct-state callers where the
// write-ahead split is not needed.
//
// The production path in Service.Transfer uses ComputeTransfer + CommitBalance
// so it can publish to Kafka between the two steps (ADR-0001 Kafka as source
// of truth).
func (s *ShardState) ApplyTransfer(req TransferRequest) (Balance, error) {
	after, err := s.ComputeTransfer(req)
	if err != nil {
		return Balance{}, err
	}
	s.CommitBalance(req.UserID, req.Asset, after)
	return after, nil
}

// ComputeTransfer validates req and returns what the post-transfer balance
// would be, WITHOUT mutating state.
func (s *ShardState) ComputeTransfer(req TransferRequest) (Balance, error) {
	if !dec.IsPositive(req.Amount) {
		return Balance{}, ErrInvalidAmount
	}
	before := s.Balance(req.UserID, req.Asset)
	return computeTransfer(before, req)
}

// CommitBalance writes balance for (userID, asset). Caller is responsible for
// running this inside the per-user sequencer.
func (s *ShardState) CommitBalance(userID, asset string, b Balance) {
	s.Account(userID).setBalance(asset, b)
}

// computeTransfer returns the post-transfer balance without mutating state.
func computeTransfer(before Balance, req TransferRequest) (Balance, error) {
	after := before
	switch req.Type {
	case TransferDeposit:
		after.Available = after.Available.Add(req.Amount)
	case TransferWithdraw:
		if after.Available.Cmp(req.Amount) < 0 {
			return Balance{}, ErrInsufficientAvailable
		}
		after.Available = after.Available.Sub(req.Amount)
	case TransferFreeze:
		if after.Available.Cmp(req.Amount) < 0 {
			return Balance{}, ErrInsufficientAvailable
		}
		after.Available = after.Available.Sub(req.Amount)
		after.Frozen = after.Frozen.Add(req.Amount)
	case TransferUnfreeze:
		if after.Frozen.Cmp(req.Amount) < 0 {
			return Balance{}, ErrInsufficientFrozen
		}
		after.Frozen = after.Frozen.Sub(req.Amount)
		after.Available = after.Available.Add(req.Amount)
	default:
		return Balance{}, fmt.Errorf("%w: %d", ErrUnknownTransferType, req.Type)
	}
	return after, nil
}
