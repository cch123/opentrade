package service

import (
	"context"
	"errors"

	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// Reservation-specific errors surfaced at the gRPC boundary.
var (
	ErrReservationIDRequired = errors.New("service: reservation_id is required")
)

// ReserveRequest is the internal input for Service.Reserve. Shape matches
// PlaceOrderRequest's freeze-relevant fields; Counter runs its existing
// ComputeFreeze on these to derive (asset, amount).
type ReserveRequest struct {
	UserID        string
	ReservationID string // idempotency key
	Symbol        string
	Side          engine.Side
	OrderType     engine.OrderType
	Price         dec.Decimal
	Qty           dec.Decimal
	QuoteQty      dec.Decimal
}

// ReserveResult is the response payload.
type ReserveResult struct {
	ReservationID string
	Asset         string
	Amount        dec.Decimal
	Accepted      bool // false = dedup hit (prior record returned)
}

// Reserve moves Available → Frozen for an upcoming order. See ADR-0041.
func (s *Service) Reserve(_ context.Context, req ReserveRequest) (*ReserveResult, error) {
	if req.UserID == "" {
		return nil, ErrMissingUserID
	}
	if req.ReservationID == "" {
		return nil, ErrReservationIDRequired
	}
	if !s.OwnsUser(req.UserID) {
		return nil, ErrWrongShard
	}
	asset, amount, err := engine.ComputeFreeze(req.Symbol, req.Side, req.OrderType, req.Price, req.Qty, req.QuoteQty)
	if err != nil {
		return nil, err
	}
	v, err := s.seq.Execute(req.UserID, func(_ uint64) (any, error) {
		r, accepted, cerr := s.state.CreateReservation(req.UserID, asset, req.ReservationID, amount)
		if cerr != nil {
			return nil, cerr
		}
		return &ReserveResult{
			ReservationID: r.RefID,
			Asset:         r.Asset,
			Amount:        r.Amount,
			Accepted:      accepted,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*ReserveResult), nil
}

// ReleaseReservationRequest is the input for Service.ReleaseReservation.
type ReleaseReservationRequest struct {
	UserID        string
	ReservationID string
}

// ReleaseReservationResult is the response payload.
type ReleaseReservationResult struct {
	ReservationID string
	Accepted      bool // true = we actually released a record
	Asset         string
	Amount        dec.Decimal
}

// ReleaseReservation moves Frozen → Available and removes the reservation
// record. Missing ref_id yields Accepted=false (idempotent).
func (s *Service) ReleaseReservation(_ context.Context, req ReleaseReservationRequest) (*ReleaseReservationResult, error) {
	if req.ReservationID == "" {
		return nil, ErrReservationIDRequired
	}
	// Best-effort ownership check; if the caller did not supply a user id
	// (future internal tooling case) we still allow the release but only
	// after the engine verifies the record's user. OwnsUser only fires
	// when we have a user id to check.
	if req.UserID != "" && !s.OwnsUser(req.UserID) {
		return nil, ErrWrongShard
	}
	userForSeq := req.UserID
	if userForSeq == "" {
		if existing := s.state.LookupReservation(req.ReservationID); existing != nil {
			userForSeq = existing.UserID
		}
	}
	if userForSeq == "" {
		// Unknown ref_id AND no user given: nothing to do.
		return &ReleaseReservationResult{ReservationID: req.ReservationID, Accepted: false}, nil
	}
	v, err := s.seq.Execute(userForSeq, func(_ uint64) (any, error) {
		released, accepted, rerr := s.state.ReleaseReservationByRef(req.UserID, req.ReservationID)
		if rerr != nil {
			return nil, rerr
		}
		out := &ReleaseReservationResult{ReservationID: req.ReservationID, Accepted: accepted}
		if released != nil {
			out.Asset = released.Asset
			out.Amount = released.Amount
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*ReleaseReservationResult), nil
}
