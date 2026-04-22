package engine

import (
	"errors"
	"sync"

	"github.com/xargin/opentrade/pkg/dec"
)

// Order-related errors.
var (
	ErrOrderNotFound       = errors.New("order not found")
	ErrDuplicateOrder      = errors.New("order id already exists")
	ErrClientOrderIDActive = errors.New("clientOrderId belongs to an active order")
	ErrNotOrderOwner       = errors.New("caller is not the order owner")
	ErrOrderNotCancellable = errors.New("order is not in a cancellable state")
)

// OrderStore is the in-memory map of live orders and the clientOrderId
// dedup index (ADR-0015: only non-terminal orders are indexed).
type OrderStore struct {
	mu           sync.RWMutex
	byID         map[uint64]*Order
	activeByCOID map[string]uint64 // (user_id + "|" + clientOrderId) → order_id
	// activeLimits counts active LIMIT orders per (user_id, symbol) for the
	// ADR-0054 slot cap. Active = Status non-terminal && Type == LIMIT.
	// Derived index — rebuilt from byID on restore; not persisted.
	activeLimits map[string]map[string]int // user_id → symbol → count
}

func newOrderStore() *OrderStore {
	return &OrderStore{
		byID:         make(map[uint64]*Order),
		activeByCOID: make(map[string]uint64),
		activeLimits: make(map[string]map[string]int),
	}
}

// Get returns the order with the given id, or nil.
func (s *OrderStore) Get(id uint64) *Order {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byID[id]
}

// LookupActiveByCOID returns the active order matching (userID, clientOrderID).
// Returns nil if no active order exists.
func (s *OrderStore) LookupActiveByCOID(userID, clientOrderID string) *Order {
	if clientOrderID == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.activeByCOID[coidKey(userID, clientOrderID)]
	if !ok {
		return nil
	}
	return s.byID[id]
}

// Insert registers a new order.
//   - ErrDuplicateOrder:      order_id collision
//   - ErrClientOrderIDActive: another active order has the same (user, coid)
func (s *OrderStore) Insert(o *Order) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.byID[o.ID]; dup {
		return ErrDuplicateOrder
	}
	if o.ClientOrderID != "" {
		key := coidKey(o.UserID, o.ClientOrderID)
		if _, active := s.activeByCOID[key]; active {
			return ErrClientOrderIDActive
		}
		s.activeByCOID[key] = o.ID
	}
	s.byID[o.ID] = o
	if countsAsActiveLimit(o) {
		s.incActiveLimit(o.UserID, o.Symbol)
	}
	return nil
}

// UpdateStatus transitions an order to newStatus and maintains the active
// clientOrderId index (drops the entry when reaching a terminal state).
func (s *OrderStore) UpdateStatus(id uint64, newStatus OrderStatus, updatedAtMS int64) (*Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.byID[id]
	if !ok {
		return nil, ErrOrderNotFound
	}
	prev := o.Status
	if !prev.IsTerminal() && newStatus.IsTerminal() && o.ClientOrderID != "" {
		delete(s.activeByCOID, coidKey(o.UserID, o.ClientOrderID))
	}
	// ADR-0054 activeLimits: the counter tracks "active LIMIT" which is the
	// conjunction (Type == LIMIT) && (!Status.IsTerminal()). A transition
	// only moves the counter if it crosses the terminal boundary.
	if o.Type == OrderTypeLimit && !prev.IsTerminal() && newStatus.IsTerminal() {
		s.decActiveLimit(o.UserID, o.Symbol)
	}
	if newStatus == OrderStatusPendingCancel {
		o.PreCancelStatus = prev
	}
	o.Status = newStatus
	o.UpdatedAt = updatedAtMS
	return o, nil
}

// SetFilledQty updates FilledQty and returns the order. Does not change Status
// (caller should UpdateStatus separately).
func (s *OrderStore) SetFilledQty(id uint64, filledQty dec.Decimal, updatedAtMS int64) (*Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.byID[id]
	if !ok {
		return nil, ErrOrderNotFound
	}
	o.FilledQty = filledQty
	o.UpdatedAt = updatedAtMS
	return o, nil
}

// AddFrozenSpent increments o.FrozenSpent by delta. Settlement uses it so
// unfreezeResidual can compute residual = FrozenAmount − FrozenSpent
// uniformly across limit / market-sell / market-buy-by-quote (ADR-0035).
func (s *OrderStore) AddFrozenSpent(id uint64, delta dec.Decimal) (*Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.byID[id]
	if !ok {
		return nil, ErrOrderNotFound
	}
	o.FrozenSpent = o.FrozenSpent.Add(delta)
	return o, nil
}

// All returns a snapshot slice of all stored orders (deep copies, safe to
// read outside the sequencer — used for snapshotting).
func (s *OrderStore) All() []*Order {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Order, 0, len(s.byID))
	for _, o := range s.byID {
		out = append(out, o.Clone())
	}
	return out
}

// Len returns the number of orders currently tracked (active + terminal that
// haven't been GC'd).
func (s *OrderStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byID)
}

// RestoreInsert adds an order without CIF validation. Used during snapshot
// restore; not to be called on the live path.
func (s *OrderStore) RestoreInsert(o *Order) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[o.ID] = o
	if o.ClientOrderID != "" && !o.Status.IsTerminal() {
		s.activeByCOID[coidKey(o.UserID, o.ClientOrderID)] = o.ID
	}
	if countsAsActiveLimit(o) {
		s.incActiveLimit(o.UserID, o.Symbol)
	}
}

// CountActiveLimits returns the number of active LIMIT orders the user
// currently holds on symbol (ADR-0054). Used by PlaceOrder to enforce
// per-(user, symbol) slot caps. O(1); takes RLock.
func (s *OrderStore) CountActiveLimits(userID, symbol string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bySymbol, ok := s.activeLimits[userID]
	if !ok {
		return 0
	}
	return bySymbol[symbol]
}

// Delete removes the order from byID and all derived indices.
// ADR-0063: called by applyOrderStatusEvent (and the equivalent
// emitStatus path on the producer side) when an order transitions
// into a terminal state. Returns ErrOrderNotFound if id does not
// exist; callers should treat that as a no-op (crash-and-retry
// idempotency).
//
// Note: only terminal orders should be Delete'd in production. This
// method does not enforce IsTerminal() so tests can exercise the
// path; the terminal check sits in the call sites.
func (s *OrderStore) Delete(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.byID[id]
	if !ok {
		return ErrOrderNotFound
	}
	// activeByCOID was already cleared on the terminal transition by
	// UpdateStatus, but defensively re-check so a buggy caller cannot
	// leave a dangling index entry.
	if o.ClientOrderID != "" {
		key := coidKey(o.UserID, o.ClientOrderID)
		if s.activeByCOID[key] == id {
			delete(s.activeByCOID, key)
		}
	}
	// Same defensive re-check for activeLimits. In practice
	// countsAsActiveLimit returns false for terminal orders so this
	// is a no-op; we guard against partially-initialised state.
	if countsAsActiveLimit(o) {
		s.decActiveLimit(o.UserID, o.Symbol)
	}
	delete(s.byID, id)
	return nil
}

// countsAsActiveLimit reports whether the order occupies a slot in the
// ADR-0054 MaxOpenLimitOrders counter.
func countsAsActiveLimit(o *Order) bool {
	return o.Type == OrderTypeLimit && !o.Status.IsTerminal()
}

// incActiveLimit / decActiveLimit maintain the per-(user, symbol) counter.
// Caller must hold s.mu exclusively. decActiveLimit drops empty maps to
// avoid leaking memory for users that churn through many symbols.
func (s *OrderStore) incActiveLimit(userID, symbol string) {
	bySymbol, ok := s.activeLimits[userID]
	if !ok {
		bySymbol = make(map[string]int)
		s.activeLimits[userID] = bySymbol
	}
	bySymbol[symbol]++
}

func (s *OrderStore) decActiveLimit(userID, symbol string) {
	bySymbol, ok := s.activeLimits[userID]
	if !ok {
		return
	}
	bySymbol[symbol]--
	if bySymbol[symbol] <= 0 {
		delete(bySymbol, symbol)
	}
	if len(bySymbol) == 0 {
		delete(s.activeLimits, userID)
	}
}

func coidKey(userID, coid string) string { return userID + "|" + coid }
