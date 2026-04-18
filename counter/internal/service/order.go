package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/pkg/dec"
)

// Order-service errors.
var (
	ErrOrderDepsNotConfigured = errors.New("service: PlaceOrder requires SetOrderDeps")
	ErrInvalidSymbol          = errors.New("service: invalid symbol")
	ErrInsufficientBalance    = errors.New("service: insufficient balance")
)

// PlaceOrderRequest is the internal input for Service.PlaceOrder.
type PlaceOrderRequest struct {
	UserID        string
	ClientOrderID string // optional
	Symbol        string
	Side          engine.Side
	OrderType     engine.OrderType
	TIF           engine.TIF
	Price         dec.Decimal
	Qty           dec.Decimal
	// QuoteQty is only set for BN-style market buy with quoteOrderQty
	// (ADR-0035). Zero for every other shape.
	QuoteQty dec.Decimal
	// ReservationID, when non-empty, consumes an existing reservation
	// instead of freezing Available at place time. The reservation's
	// (asset, amount) must match ComputeFreeze for this order shape or
	// the call is rejected (ADR-0041).
	ReservationID string
}

// PlaceOrderResult is the response payload.
type PlaceOrderResult struct {
	OrderID       uint64
	ClientOrderID string
	Accepted      bool // false = dedup hit (returned existing active order)
	ReceivedAtMS  int64
	RejectReason  string
}

// CancelOrderRequest is the input for Service.CancelOrder.
type CancelOrderRequest struct {
	UserID  string
	OrderID uint64
}

// CancelOrderResult is the response payload.
type CancelOrderResult struct {
	OrderID      uint64
	Accepted     bool
	RejectReason string
}

// PlaceOrder accepts a new order. The call is idempotent against the
// clientOrderId: if an active order exists for (user_id, clientOrderId) it
// returns that order with Accepted=false (ADR-0015).
func (s *Service) PlaceOrder(ctx context.Context, req PlaceOrderRequest) (*PlaceOrderResult, error) {
	if s.txn == nil || s.idgen == nil {
		return nil, ErrOrderDepsNotConfigured
	}
	if req.UserID == "" {
		return nil, ErrMissingUserID
	}
	if !s.OwnsUser(req.UserID) {
		return nil, ErrWrongShard
	}
	if req.Symbol == "" {
		return nil, ErrInvalidSymbol
	}

	// Validate shape + compute freeze outside the sequencer.
	freezeAsset, freezeAmount, err := engine.ComputeFreeze(req.Symbol, req.Side, req.OrderType, req.Price, req.Qty, req.QuoteQty)
	if err != nil {
		return &PlaceOrderResult{
			ClientOrderID: req.ClientOrderID,
			Accepted:      false,
			RejectReason:  err.Error(),
		}, nil
	}

	// Fast-path: active-COID dedup without grabbing the sequencer.
	if req.ClientOrderID != "" {
		if existing := s.state.Orders().LookupActiveByCOID(req.UserID, req.ClientOrderID); existing != nil {
			return &PlaceOrderResult{
				OrderID:       existing.ID,
				ClientOrderID: req.ClientOrderID,
				Accepted:      false,
				ReceivedAtMS:  existing.CreatedAt,
			}, nil
		}
	}

	v, err := s.seq.Execute(req.UserID, func(seqID uint64) (any, error) {
		// Re-check dedup inside the sequencer window.
		if req.ClientOrderID != "" {
			if existing := s.state.Orders().LookupActiveByCOID(req.UserID, req.ClientOrderID); existing != nil {
				return &PlaceOrderResult{
					OrderID:       existing.ID,
					ClientOrderID: req.ClientOrderID,
					Accepted:      false,
					ReceivedAtMS:  existing.CreatedAt,
				}, nil
			}
		}

		// Reservation path (ADR-0041): the funds are already in Frozen
		// under `req.ReservationID`; we just validate the match and
		// consume the record. Plain-path freezes Available here.
		bal := s.state.Balance(req.UserID, freezeAsset)
		var balAfter engine.Balance
		if req.ReservationID != "" {
			if err := s.state.ConsumeReservationForOrder(req.UserID, req.ReservationID, freezeAsset, freezeAmount); err != nil {
				return &PlaceOrderResult{
					ClientOrderID: req.ClientOrderID,
					Accepted:      false,
					RejectReason:  err.Error(),
				}, nil
			}
			// Balance is unchanged in this step — Reserve already did the
			// Available → Frozen move. Projected == current.
			balAfter = bal
		} else {
			if bal.Available.Cmp(freezeAmount) < 0 {
				return &PlaceOrderResult{
					ClientOrderID: req.ClientOrderID,
					Accepted:      false,
					RejectReason:  ErrInsufficientBalance.Error(),
				}, nil
			}
			balAfter = engine.Balance{
				Available: bal.Available.Sub(freezeAmount),
				Frozen:    bal.Frozen.Add(freezeAmount),
			}
		}

		now := time.Now().UnixMilli()
		orderID := s.idgen.Next()
		order := &engine.Order{
			ID:            orderID,
			ClientOrderID: req.ClientOrderID,
			UserID:        req.UserID,
			Symbol:        req.Symbol,
			Side:          req.Side,
			Type:          req.OrderType,
			TIF:           req.TIF,
			Price:         req.Price,
			Qty:           req.Qty,
			QuoteQty:      req.QuoteQty,
			FilledQty:     dec.Zero,
			FrozenAsset:   freezeAsset,
			FrozenAmount:  freezeAmount,
			Status:        engine.OrderStatusPendingNew,
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		// Project post-commit versions (setBalance bumps both by 1).
		acc := s.state.Account(req.UserID)
		balAfter.Version = acc.Balance(freezeAsset).Version + 1
		expectedAccVer := acc.Version() + 1

		journalEvt, orderEvt, err := journal.BuildPlaceOrderEvents(journal.PlaceOrderEventInput{
			SeqID:          seqID,
			TsUnixMS:       now,
			ProducerID:     s.cfg.ProducerID,
			AccountVersion: expectedAccVer,
			Order:          order,
			BalanceAfter:   balAfter,
		})
		if err != nil {
			return nil, fmt.Errorf("build events: %w", err)
		}
		if err := s.txn.PublishOrderPlacement(ctx, journalEvt, orderEvt, req.UserID, req.Symbol); err != nil {
			return nil, fmt.Errorf("publish: %w", err)
		}

		// Commit state now that Kafka accepted the events. CommitBalance
		// routes through setBalance and bumps both Account.Version and
		// Balance.Version (ADR-0048 backlog: 双层 version).
		s.state.CommitBalance(req.UserID, freezeAsset, balAfter)
		if err := s.state.Orders().Insert(order); err != nil {
			// Extremely unlikely (idgen collision); we've already committed
			// to Kafka, so best we can do is log and return the error.
			s.logger.Error("order insert failed after Kafka commit",
				zap.Uint64("order_id", orderID), zap.Error(err))
			return nil, err
		}
		return &PlaceOrderResult{
			OrderID:       orderID,
			ClientOrderID: req.ClientOrderID,
			Accepted:      true,
			ReceivedAtMS:  now,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*PlaceOrderResult), nil
}

// CancelOrder requests cancellation of an existing order. Returns Accepted=
// false if the order is absent / not owned / already terminal / already
// pending cancel (idempotent).
func (s *Service) CancelOrder(ctx context.Context, req CancelOrderRequest) (*CancelOrderResult, error) {
	if s.txn == nil {
		return nil, ErrOrderDepsNotConfigured
	}
	if req.UserID == "" {
		return nil, ErrMissingUserID
	}
	if !s.OwnsUser(req.UserID) {
		return nil, ErrWrongShard
	}

	v, err := s.seq.Execute(req.UserID, func(seqID uint64) (any, error) {
		o := s.state.Orders().Get(req.OrderID)
		if o == nil {
			return &CancelOrderResult{
				OrderID:      req.OrderID,
				Accepted:     false,
				RejectReason: engine.ErrOrderNotFound.Error(),
			}, nil
		}
		if o.UserID != req.UserID {
			return &CancelOrderResult{
				OrderID:      req.OrderID,
				Accepted:     false,
				RejectReason: engine.ErrNotOrderOwner.Error(),
			}, nil
		}
		if o.Status.IsTerminal() {
			return &CancelOrderResult{
				OrderID:      req.OrderID,
				Accepted:     false,
				RejectReason: "order already terminal",
			}, nil
		}
		if o.Status == engine.OrderStatusPendingCancel {
			// Idempotent: cancel already in flight.
			return &CancelOrderResult{
				OrderID:  req.OrderID,
				Accepted: true,
			}, nil
		}

		// Cancel doesn't touch balances, so AccountVersion is the current
		// value — a stable witness, not a post-mutation projection.
		journalEvt, orderEvt := journal.BuildCancelEvents(journal.CancelOrderEventInput{
			SeqID:          seqID,
			TsUnixMS:       time.Now().UnixMilli(),
			ProducerID:     s.cfg.ProducerID,
			AccountVersion: s.state.Account(req.UserID).Version(),
			Order:          o,
		})
		if err := s.txn.PublishOrderPlacement(ctx, journalEvt, orderEvt, req.UserID, o.Symbol); err != nil {
			return nil, fmt.Errorf("publish: %w", err)
		}

		// Transition local status. The Match-side OrderCancelled event will
		// eventually move us to CANCELED (or FILLED if we raced with fills).
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusPendingCancel, time.Now().UnixMilli()); err != nil {
			s.logger.Error("cancel: update status after commit", zap.Error(err))
			return nil, err
		}
		return &CancelOrderResult{OrderID: req.OrderID, Accepted: true}, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*CancelOrderResult), nil
}
