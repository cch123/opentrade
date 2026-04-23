// Package engine holds asset-service's in-memory funding-wallet state
// (ADR-0057). This is the biz_line=funding account book; asset-service
// itself plays the AssetHolder role for funding and orchestrates cross-
// biz_line sagas elsewhere.
//
// The MVP state model is deliberately lean:
//
//   - One Account per user, holding (available, frozen) per asset.
//   - A bounded ring buffer of recently-applied transfer_ids for
//     idempotency (same device as counter's Account ring; see
//     counter/internal/engine/state.go).
//   - Two monotonically-increasing counters per user: a user-level
//     funding_version bumped on every mutation, and a per-asset
//     Balance.Version. These mirror counter's ADR-0048 双层 version scheme
//     so downstream consumers can cache-invalidate on either handle.
//
// The engine is concurrency-safe via a per-account mutex. Different users
// proceed in parallel; operations on the same user serialize. There is
// no cross-user cross-asset atomic path (funding wallet has no
// order/freeze notion — that lives in counter).
package engine

import (
	"errors"
	"sync"

	"github.com/xargin/opentrade/pkg/dec"
)

// Errors returned by the engine. These surface through service to
// AssetHolder.RejectReason mapping at the RPC boundary.
var (
	ErrInsufficientAvailable = errors.New("insufficient available balance")
	ErrInvalidAmount         = errors.New("amount must be strictly positive")
	ErrMissingUserID         = errors.New("user_id is required")
	ErrMissingAsset          = errors.New("asset is required")
	ErrMissingTransferID     = errors.New("transfer_id is required")
)

// recentRing is the per-account idempotency window. Matches counter's
// 256-slot sizing — enough to absorb a short burst of retries without
// unbounded growth.
const recentRingSize = 256

// Balance is the (available, frozen, version) triple for one asset.
type Balance struct {
	Available dec.Decimal
	Frozen    dec.Decimal
	Version   uint64
}

// Account is one user's funding wallet: many assets, one ring buffer,
// one user-level version counter. All fields are guarded by `mu`.
type Account struct {
	mu sync.Mutex

	balances map[string]Balance
	version  uint64 // user-level monotonic (funding_version on the wire)

	// recent* implement a FIFO ring of the last N successful transfer_ids.
	// seen keeps O(1) lookup; ring preserves insertion order for eviction.
	recentSeen map[string]struct{}
	recentRing [recentRingSize]string
	recentHead int
	recentSize int
}

// newAccount constructs an empty account.
func newAccount() *Account {
	return &Account{
		balances:   make(map[string]Balance),
		recentSeen: make(map[string]struct{}),
	}
}

// Balance returns a copy of the asset's balance. Unknown assets return
// a zero balance.
func (a *Account) Balance(asset string) Balance {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.balances[asset]
}

// Version returns the user-level funding_version.
func (a *Account) Version() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.version
}

// Copy returns a snapshot of all (asset -> Balance) pairs. Used by
// QueryFundingBalance.
func (a *Account) Copy() map[string]Balance {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]Balance, len(a.balances))
	for k, v := range a.balances {
		out[k] = v
	}
	return out
}

// seen reports whether transferID has been applied recently. Caller
// already holds a.mu.
func (a *Account) seen(transferID string) bool {
	_, ok := a.recentSeen[transferID]
	return ok
}

// remember records transferID in the ring, evicting the oldest entry if
// the ring is full. Caller already holds a.mu.
func (a *Account) remember(transferID string) {
	if _, ok := a.recentSeen[transferID]; ok {
		return
	}
	if a.recentSize == recentRingSize {
		old := a.recentRing[a.recentHead]
		delete(a.recentSeen, old)
	} else {
		a.recentSize++
	}
	a.recentRing[a.recentHead] = transferID
	a.recentHead = (a.recentHead + 1) % recentRingSize
	a.recentSeen[transferID] = struct{}{}
}

// State is the process-wide accounts map. A top-level RWMutex guards the
// map; per-account locks guard balances. This two-level scheme lets
// different users proceed concurrently while keeping same-user ops
// serialized.
type State struct {
	mu       sync.RWMutex
	accounts map[string]*Account
}

// NewState constructs an empty State.
func NewState() *State {
	return &State{accounts: make(map[string]*Account)}
}

// Account returns (or lazily creates) the user's account.
func (s *State) Account(userID string) *Account {
	s.mu.RLock()
	acc, ok := s.accounts[userID]
	s.mu.RUnlock()
	if ok {
		return acc
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if acc, ok := s.accounts[userID]; ok {
		return acc
	}
	acc = newAccount()
	s.accounts[userID] = acc
	return acc
}

// TransferRequest is the internal shape passed to Apply*. It carries
// everything the engine needs to update state; RPC-layer fields (peer_biz,
// memo, compensate_cause) are handled by the service/store boundary and are
// not needed here.
type TransferRequest struct {
	UserID     string
	TransferID string
	Asset      string
	Amount     dec.Decimal
}

// Result is what Apply* returns. Duplicated is true when the transfer_id
// hit the idempotency ring; BalanceAfter / FundingVersion still reflect
// the CURRENT state but do not represent the first-execution snapshot
// (this is the ADR-0048 方案 A simplification — callers that need the
// historical balance must follow up with QueryFundingBalance).
type Result struct {
	BalanceAfter   Balance
	FundingVersion uint64
	Duplicated     bool
}

// ApplyTransferIn credits amount to (user, asset).available. Errors on
// invalid amount. Idempotent on req.TransferID.
func (s *State) ApplyTransferIn(req TransferRequest) (Result, error) {
	return s.applyTransferIn(req, nil)
}

// ApplyTransferInCommitted is ApplyTransferIn plus an atomic commit hook.
// The hook runs while the user's account lock is held, after the next
// balance is visible to the hook but before transfer_id is remembered. If
// commit returns an error, the balance/version mutation is rolled back and
// the transfer remains retryable.
func (s *State) ApplyTransferInCommitted(req TransferRequest, commit func(Result) error) (Result, error) {
	return s.applyTransferIn(req, commit)
}

func (s *State) applyTransferIn(req TransferRequest, commit func(Result) error) (Result, error) {
	if err := validate(req); err != nil {
		return Result{}, err
	}
	acc := s.Account(req.UserID)
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.seen(req.TransferID) {
		bal := acc.balances[req.Asset]
		return Result{BalanceAfter: bal, FundingVersion: acc.version, Duplicated: true}, nil
	}
	cur, hadBalance := acc.balances[req.Asset]
	prevVersion := acc.version
	next := Balance{
		Available: cur.Available.Add(req.Amount),
		Frozen:    cur.Frozen,
		Version:   cur.Version + 1,
	}
	acc.balances[req.Asset] = next
	acc.version++
	res := Result{BalanceAfter: next, FundingVersion: acc.version}
	if commit != nil {
		if err := commit(res); err != nil {
			if hadBalance {
				acc.balances[req.Asset] = cur
			} else {
				delete(acc.balances, req.Asset)
			}
			acc.version = prevVersion
			return Result{}, err
		}
	}
	acc.remember(req.TransferID)
	return res, nil
}

// ApplyTransferOut debits amount from (user, asset).available. Returns
// ErrInsufficientAvailable when the balance would go negative; the ring
// is NOT updated on rejection so the caller may retry with corrected
// params. Idempotent on req.TransferID.
func (s *State) ApplyTransferOut(req TransferRequest) (Result, error) {
	return s.applyTransferOut(req, nil)
}

// ApplyTransferOutCommitted is ApplyTransferOut plus an atomic commit hook.
// See ApplyTransferInCommitted for the rollback/retry contract.
func (s *State) ApplyTransferOutCommitted(req TransferRequest, commit func(Result) error) (Result, error) {
	return s.applyTransferOut(req, commit)
}

func (s *State) applyTransferOut(req TransferRequest, commit func(Result) error) (Result, error) {
	if err := validate(req); err != nil {
		return Result{}, err
	}
	acc := s.Account(req.UserID)
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if acc.seen(req.TransferID) {
		bal := acc.balances[req.Asset]
		return Result{BalanceAfter: bal, FundingVersion: acc.version, Duplicated: true}, nil
	}
	cur, hadBalance := acc.balances[req.Asset]
	if cur.Available.Cmp(req.Amount) < 0 {
		return Result{}, ErrInsufficientAvailable
	}
	prevVersion := acc.version
	next := Balance{
		Available: cur.Available.Sub(req.Amount),
		Frozen:    cur.Frozen,
		Version:   cur.Version + 1,
	}
	acc.balances[req.Asset] = next
	acc.version++
	res := Result{BalanceAfter: next, FundingVersion: acc.version}
	if commit != nil {
		if err := commit(res); err != nil {
			if hadBalance {
				acc.balances[req.Asset] = cur
			} else {
				delete(acc.balances, req.Asset)
			}
			acc.version = prevVersion
			return Result{}, err
		}
	}
	acc.remember(req.TransferID)
	return res, nil
}

// ApplyCompensate is semantically a credit (mirrors TransferIn). The engine
// itself is agnostic — same math.
func (s *State) ApplyCompensate(req TransferRequest) (Result, error) {
	return s.ApplyTransferIn(req)
}

// ApplyCompensateCommitted is ApplyCompensate plus an atomic commit hook.
func (s *State) ApplyCompensateCommitted(req TransferRequest, commit func(Result) error) (Result, error) {
	return s.ApplyTransferInCommitted(req, commit)
}

func validate(req TransferRequest) error {
	if req.UserID == "" {
		return ErrMissingUserID
	}
	if req.TransferID == "" {
		return ErrMissingTransferID
	}
	if req.Asset == "" {
		return ErrMissingAsset
	}
	if req.Amount.Sign() <= 0 {
		return ErrInvalidAmount
	}
	return nil
}
