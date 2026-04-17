// Package engine holds the Counter account state machine.
//
// State is organized as ShardState:
//
//	ShardState -> user_id -> Account -> asset -> Balance(available, frozen)
//
// ShardState's map-level access is thread-safe (sync.Map). Individual Account
// balances are mutated only through the UserSequencer (ADR-0018), which
// guarantees at most one in-flight write per user.
package engine

import "github.com/xargin/opentrade/pkg/dec"

// Balance represents a user's holdings of a single asset.
type Balance struct {
	Available dec.Decimal
	Frozen    dec.Decimal
}

// Total returns available + frozen.
func (b Balance) Total() dec.Decimal { return b.Available.Add(b.Frozen) }

// IsEmpty reports whether the balance is zero (both available and frozen).
func (b Balance) IsEmpty() bool { return b.Available.Sign() == 0 && b.Frozen.Sign() == 0 }

// TransferType classifies a Transfer request.
type TransferType uint8

const (
	TransferDeposit  TransferType = 1
	TransferWithdraw TransferType = 2
	TransferFreeze   TransferType = 3
	TransferUnfreeze TransferType = 4
)

func (t TransferType) String() string {
	switch t {
	case TransferDeposit:
		return "deposit"
	case TransferWithdraw:
		return "withdraw"
	case TransferFreeze:
		return "freeze"
	case TransferUnfreeze:
		return "unfreeze"
	default:
		return "unknown"
	}
}

// TransferRequest is the input to ShardState.ApplyTransfer.
type TransferRequest struct {
	TransferID string
	UserID     string
	Asset      string
	Amount     dec.Decimal
	Type       TransferType
	BizRefID   string
	Memo       string
}

// TransferStatus is the outcome of a Transfer.
type TransferStatus uint8

const (
	TransferStatusConfirmed  TransferStatus = 1
	TransferStatusRejected   TransferStatus = 2
	TransferStatusDuplicated TransferStatus = 3 // dedup hit; caller returns cached response
)

// TransferResult is what Service.Transfer returns to callers.
type TransferResult struct {
	TransferID   string
	Status       TransferStatus
	RejectReason string
	BalanceAfter Balance
	SeqID        uint64 // shard-level seq assigned by UserSequencer (for audit)
}
