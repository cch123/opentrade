package counterstate

import (
	"errors"
	"sync"

	"github.com/xargin/opentrade/pkg/dec"
)

// Reservation is a pre-frozen balance held against a privileged caller's
// ref_id (ADR-0041). Used by the trigger-order service so STOP /
// TAKE_PROFIT can reserve funds at Place time and consume them atomically
// at trigger time.
type Reservation struct {
	UserID      string
	RefID       string // caller-supplied id (e.g. "trig-<id>")
	Asset       string
	Amount      dec.Decimal
	CreatedAtMs int64
}

// Clone returns a deep copy suitable for returning across API boundaries.
func (r *Reservation) Clone() *Reservation {
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}

// reservationStore is the engine's reservation registry. All access is
// through the methods below; callers hold no external locks.
type reservationStore struct {
	mu sync.RWMutex
	// byRef is the primary index (idempotency hinges on it).
	byRef map[string]*Reservation
	// byUser gives us O(users) iteration for snapshot / audit.
	byUser map[string]map[string]*Reservation
}

func newReservationStore() *reservationStore {
	return &reservationStore{
		byRef:  make(map[string]*Reservation),
		byUser: make(map[string]map[string]*Reservation),
	}
}

// insert adds r to the store. Callers must have already validated that
// r.RefID is unique (Get == nil) and holding no higher-level lock.
func (s *reservationStore) insert(r *Reservation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byRef[r.RefID] = r
	set, ok := s.byUser[r.UserID]
	if !ok {
		set = make(map[string]*Reservation)
		s.byUser[r.UserID] = set
	}
	set[r.RefID] = r
}

// Get returns a clone of the reservation or nil when unknown.
func (s *reservationStore) Get(refID string) *Reservation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r, ok := s.byRef[refID]; ok {
		return r.Clone()
	}
	return nil
}

// remove deletes the reservation for refID. Returns (*, true) on hit.
func (s *reservationStore) remove(refID string) (*Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.byRef[refID]
	if !ok {
		return nil, false
	}
	delete(s.byRef, refID)
	if set, ok := s.byUser[r.UserID]; ok {
		delete(set, refID)
		if len(set) == 0 {
			delete(s.byUser, r.UserID)
		}
	}
	return r, true
}

// all returns every reservation in an arbitrary order (for snapshot).
func (s *reservationStore) all() []*Reservation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Reservation, 0, len(s.byRef))
	for _, r := range s.byRef {
		out = append(out, r.Clone())
	}
	return out
}

// count returns the current reservation count (for tests / metrics).
func (s *reservationStore) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byRef)
}

// -----------------------------------------------------------------------------
// ShardState integration
// -----------------------------------------------------------------------------

// ErrReservationNotFound is returned when a caller references a ref_id
// the store doesn't know about. Idempotency: Release surfaces this as
// accepted=false, not as an error.
var ErrReservationNotFound = errors.New("engine: reservation not found")

// ErrReservationMismatch is returned when a PlaceOrder(reservation_id) is
// made but the prior reservation's (asset, amount) does not match what
// ComputeFreeze would produce for the new order shape — most often a
// caller bug mixing up reservations across triggers.
var ErrReservationMismatch = errors.New("engine: reservation shape mismatch")

// ErrReservationUserMismatch: the reservation exists but belongs to a
// different user than the one making the call. Surface as PermissionDenied.
var ErrReservationUserMismatch = errors.New("engine: reservation belongs to another user")

// Reservations exposes the reservation store for service-level callers.
// Snapshot / restore uses this; regular read paths should go through
// LookupReservation instead.
func (s *ShardState) Reservations() *reservationStore { return s.reservations }

// LookupReservation is the standard read path — returns a clone or nil.
func (s *ShardState) LookupReservation(refID string) *Reservation {
	return s.reservations.Get(refID)
}

// AllReservations returns cloned snapshots (for Capture). Order is
// arbitrary.
func (s *ShardState) AllReservations() []*Reservation {
	return s.reservations.all()
}

// CreateReservation atomically moves `amount` of `asset` from Available to
// Frozen for userID and inserts a reservation record keyed by refID. This
// is the Reserve RPC's write path. Callers MUST run it inside the user's
// sequencer; the dedup check + balance check + mutation are atomic from
// the user's point of view.
//
// Idempotency: when refID already exists for the same user/asset/amount the
// prior record is returned with accepted=false. Different (user, asset,
// amount) under the same refID is a caller bug and returns
// ErrReservationMismatch.
//
// This does not emit a counter-journal event. Reservations are an
// internal balance bookkeeping concern; trade-dump's accounts projection
// converges when the reservation is later consumed by PlaceOrder (via the
// usual FreezeEvent's BalanceAfter) or released (which restores the
// pre-Reserve state, matching trade-dump's unchanged view).
func (s *ShardState) CreateReservation(userID, asset, refID string, amount dec.Decimal) (res *Reservation, accepted bool, err error) {
	if userID == "" {
		return nil, false, errors.New("engine: user_id required")
	}
	if refID == "" {
		return nil, false, errors.New("engine: reservation ref_id required")
	}
	if !dec.IsPositive(amount) {
		return nil, false, ErrInvalidAmount
	}
	if asset == "" {
		return nil, false, errors.New("engine: asset required")
	}
	if existing := s.reservations.Get(refID); existing != nil {
		if existing.UserID != userID || existing.Asset != asset || existing.Amount.Cmp(amount) != 0 {
			return nil, false, ErrReservationMismatch
		}
		return existing, false, nil
	}
	acc := s.Account(userID)
	bal := acc.Balance(asset)
	if bal.Available.Cmp(amount) < 0 {
		return nil, false, ErrInsufficientAvailable
	}
	bal.Available = bal.Available.Sub(amount)
	bal.Frozen = bal.Frozen.Add(amount)
	acc.setBalance(asset, bal) // bump version; this is business, not restore
	r := &Reservation{
		UserID: userID,
		RefID:  refID,
		Asset:  asset,
		Amount: amount,
	}
	s.reservations.insert(r)
	return r.Clone(), true, nil
}

// ReleaseReservationByRef is the user-cancel path: Frozen → Available,
// reservation record removed. Returns the released record (for journaling
// / response) or (nil, false, nil) when refID is unknown (idempotent).
func (s *ShardState) ReleaseReservationByRef(userID, refID string) (*Reservation, bool, error) {
	if refID == "" {
		return nil, false, errors.New("engine: reservation ref_id required")
	}
	existing := s.reservations.Get(refID)
	if existing == nil {
		return nil, false, nil
	}
	if userID != "" && existing.UserID != userID {
		return nil, false, ErrReservationUserMismatch
	}
	acc := s.Account(existing.UserID)
	bal := acc.Balance(existing.Asset)
	if bal.Frozen.Cmp(existing.Amount) < 0 {
		// Shouldn't happen — reservations are the sole consumers of this
		// portion of frozen. Surface as an error to signal the invariant
		// break so operators investigate.
		return nil, false, ErrInsufficientFrozen
	}
	bal.Frozen = bal.Frozen.Sub(existing.Amount)
	bal.Available = bal.Available.Add(existing.Amount)
	acc.setBalance(existing.Asset, bal) // bump version
	_, _ = s.reservations.remove(refID)
	return existing, true, nil
}

// ConsumeReservationForOrder is the PlaceOrder(reservation_id=…) path:
// validate the reservation matches the order's computed freeze, delete
// the reservation record, and leave the balance unchanged (the funds
// stay in Frozen and become the order's FrozenAmount).
func (s *ShardState) ConsumeReservationForOrder(userID, refID, asset string, amount dec.Decimal) error {
	if refID == "" {
		return errors.New("engine: reservation ref_id required")
	}
	existing := s.reservations.Get(refID)
	if existing == nil {
		return ErrReservationNotFound
	}
	if existing.UserID != userID {
		return ErrReservationUserMismatch
	}
	if existing.Asset != asset || existing.Amount.Cmp(amount) != 0 {
		return ErrReservationMismatch
	}
	_, _ = s.reservations.remove(refID)
	return nil
}

// RestoreReservation is the snapshot-restore entry point. It inserts the
// record without touching balances (snapshot's balance state already
// reflects the reservation).
func (s *ShardState) RestoreReservation(r *Reservation) {
	if r == nil || r.RefID == "" {
		return
	}
	cp := *r
	s.reservations.insert(&cp)
}
