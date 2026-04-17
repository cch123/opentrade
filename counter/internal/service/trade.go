package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/pkg/dec"
)

// ErrUnknownPayload is surfaced for trade-event records whose oneof payload
// we do not recognize (forward-compat).
var ErrUnknownPayload = errors.New("trade consumer: unknown payload")

// HandleTradeEvent applies a trade-event produced by Match to the Counter
// state. For MVP-3 the consumer is at-least-once; idempotency lives in the
// handler (e.g. a Trade whose FilledQtyAfter is already reflected in our
// stored order is skipped).
func (s *Service) HandleTradeEvent(ctx context.Context, evt *eventpb.TradeEvent) error {
	if evt == nil {
		return errors.New("nil event")
	}
	switch p := evt.Payload.(type) {
	case *eventpb.TradeEvent_Accepted:
		return s.handleAccepted(ctx, p.Accepted)
	case *eventpb.TradeEvent_Rejected:
		return s.handleRejected(ctx, p.Rejected)
	case *eventpb.TradeEvent_Trade:
		return s.handleTrade(ctx, p.Trade)
	case *eventpb.TradeEvent_Cancelled:
		return s.handleCancelled(ctx, p.Cancelled)
	case *eventpb.TradeEvent_Expired:
		return s.handleExpired(ctx, p.Expired)
	default:
		return ErrUnknownPayload
	}
}

// -----------------------------------------------------------------------------
// Individual handlers
// -----------------------------------------------------------------------------

func (s *Service) handleAccepted(ctx context.Context, a *eventpb.OrderAccepted) error {
	_, err := s.seq.Execute(a.UserId, func(seqID uint64) (any, error) {
		o := s.state.Orders().Get(a.OrderId)
		if o == nil || o.Status != engine.OrderStatusPendingNew {
			return nil, nil // already past PENDING_NEW or not ours
		}
		oldStatus := o.Status
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusNew, time.Now().UnixMilli()); err != nil {
			return nil, err
		}
		return nil, s.emitStatus(ctx, seqID, o, oldStatus, engine.OrderStatusNew, 0)
	})
	return err
}

func (s *Service) handleRejected(ctx context.Context, r *eventpb.OrderRejected) error {
	_, err := s.seq.Execute(r.UserId, func(seqID uint64) (any, error) {
		o := s.state.Orders().Get(r.OrderId)
		if o == nil || o.Status.IsTerminal() {
			return nil, nil
		}
		oldStatus := o.Status
		if err := s.unfreezeResidual(o); err != nil {
			return nil, err
		}
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusRejected, time.Now().UnixMilli()); err != nil {
			return nil, err
		}
		return nil, s.emitStatus(ctx, seqID, o, oldStatus, engine.OrderStatusRejected, r.Reason)
	})
	return err
}

func (s *Service) handleTrade(ctx context.Context, t *eventpb.Trade) error {
	makerFilledAfter, err := dec.Parse(t.MakerFilledQtyAfter)
	if err != nil {
		return fmt.Errorf("parse maker_filled_qty_after: %w", err)
	}
	takerFilledAfter, err := dec.Parse(t.TakerFilledQtyAfter)
	if err != nil {
		return fmt.Errorf("parse taker_filled_qty_after: %w", err)
	}
	price, err := dec.Parse(t.Price)
	if err != nil {
		return fmt.Errorf("parse trade price: %w", err)
	}
	qty, err := dec.Parse(t.Qty)
	if err != nil {
		return fmt.Errorf("parse trade qty: %w", err)
	}
	ti := engine.TradeInput{
		TradeID:             t.TradeId,
		Symbol:              t.Symbol,
		Price:               price,
		Qty:                 qty,
		MakerUserID:         t.MakerUserId,
		MakerOrderID:        t.MakerOrderId,
		TakerUserID:         t.TakerUserId,
		TakerOrderID:        t.TakerOrderId,
		TakerSide:           sideFromProto(t.TakerSide),
		MakerFilledQtyAfter: makerFilledAfter,
		TakerFilledQtyAfter: takerFilledAfter,
	}

	// Apply maker side, then taker side. Each goes through its own user
	// sequencer so concurrent order flow for that user is serialized.
	if err := s.applyPartyViaSequencer(ctx, ti, true); err != nil {
		return fmt.Errorf("maker settlement: %w", err)
	}
	if err := s.applyPartyViaSequencer(ctx, ti, false); err != nil {
		return fmt.Errorf("taker settlement: %w", err)
	}
	return nil
}

func (s *Service) handleCancelled(ctx context.Context, c *eventpb.OrderCancelled) error {
	_, err := s.seq.Execute(c.UserId, func(seqID uint64) (any, error) {
		o := s.state.Orders().Get(c.OrderId)
		if o == nil || o.Status.IsTerminal() {
			return nil, nil
		}
		oldStatus := o.Status
		if err := s.unfreezeResidual(o); err != nil {
			return nil, err
		}
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusCanceled, time.Now().UnixMilli()); err != nil {
			return nil, err
		}
		return nil, s.emitStatus(ctx, seqID, o, oldStatus, engine.OrderStatusCanceled, 0)
	})
	return err
}

func (s *Service) handleExpired(ctx context.Context, e *eventpb.OrderExpired) error {
	_, err := s.seq.Execute(e.UserId, func(seqID uint64) (any, error) {
		o := s.state.Orders().Get(e.OrderId)
		if o == nil || o.Status.IsTerminal() {
			return nil, nil
		}
		oldStatus := o.Status
		if err := s.unfreezeResidual(o); err != nil {
			return nil, err
		}
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusExpired, time.Now().UnixMilli()); err != nil {
			return nil, err
		}
		return nil, s.emitStatus(ctx, seqID, o, oldStatus, engine.OrderStatusExpired, e.Reason)
	})
	return err
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// applyPartyViaSequencer runs the per-party settlement under the user's
// sequencer. Idempotency: if our stored FilledQty already matches (or exceeds)
// the event's FilledQtyAfter, assume the update was applied earlier and skip.
func (s *Service) applyPartyViaSequencer(ctx context.Context, ti engine.TradeInput, maker bool) error {
	userID := ti.TakerUserID
	if maker {
		userID = ti.MakerUserID
	}
	_, err := s.seq.Execute(userID, func(seqID uint64) (any, error) {
		orderID := ti.TakerOrderID
		if maker {
			orderID = ti.MakerOrderID
		}
		o := s.state.Orders().Get(orderID)
		if o == nil {
			// Shouldn't happen in normal flow — Match only produces Trade for
			// orders we placed. Skip silently (can log at higher level).
			return nil, nil
		}
		// Idempotency: compare stored FilledQty with the event's after-value.
		after := ti.MakerFilledQtyAfter
		if !maker {
			after = ti.TakerFilledQtyAfter
		}
		if o.FilledQty.Cmp(after) >= 0 {
			return nil, nil // already applied
		}

		mSet, tSet, err := engine.ComputeSettlement(s.state, ti)
		if err != nil {
			return nil, err
		}
		party := tSet
		if maker {
			party = mSet
		}
		if err := s.state.ApplyPartySettlement(ti.Symbol, party); err != nil {
			return nil, err
		}

		// Emit settlement event to counter-journal (best-effort, non-txn).
		base, quote, _ := engine.SymbolAssets(ti.Symbol)
		baseAfter := s.state.Balance(party.UserID, base)
		quoteAfter := s.state.Balance(party.UserID, quote)
		evt, err := journal.BuildSettlementEvent(journal.SettlementEventInput{
			SeqID:      seqID,
			ProducerID: s.cfg.ProducerID,
			Symbol:     ti.Symbol,
			TradeID:    ti.TradeID,
			Party:      party,
			BaseAfter:  baseAfter,
			QuoteAfter: quoteAfter,
		})
		if err != nil {
			return nil, err
		}
		if err := s.publisher.Publish(ctx, party.UserID, evt); err != nil {
			s.logger.Error("publish settlement",
				zap.Uint64("order_id", party.OrderID),
				zap.Error(err))
			// For MVP-3 we don't roll back state on publish failure; the
			// event will be re-sent on consumer restart via idempotency.
		}
		return nil, nil
	})
	return err
}

// unfreezeResidual releases the still-frozen funds of a terminal order
// (cancel / reject / expire). For buy limit: residual = price * (qty -
// filled). For sell limit: residual = qty - filled.
func (s *Service) unfreezeResidual(o *engine.Order) error {
	if o.FrozenAsset == "" {
		return nil
	}
	remaining := o.RemainingQty()
	if !dec.IsPositive(remaining) {
		return nil
	}
	var residual dec.Decimal
	switch o.Side {
	case engine.SideBid:
		residual = o.Price.Mul(remaining)
	case engine.SideAsk:
		residual = remaining
	default:
		return fmt.Errorf("unfreeze: invalid side")
	}
	acc := s.state.Account(o.UserID)
	b := acc.Balance(o.FrozenAsset)
	b.Frozen = b.Frozen.Sub(residual)
	b.Available = b.Available.Add(residual)
	if b.Frozen.Sign() < 0 {
		return fmt.Errorf("unfreeze: frozen would be negative for %s %s", o.UserID, o.FrozenAsset)
	}
	acc.PutForRestore(o.FrozenAsset, b)
	return nil
}

// emitStatus publishes an OrderStatusEvent for downstream observers.
func (s *Service) emitStatus(ctx context.Context, seqID uint64, o *engine.Order, oldStatus, newStatus engine.OrderStatus, reason eventpb.RejectReason) error {
	evt := journal.BuildOrderStatusEvent(journal.OrderStatusEventInput{
		SeqID:      seqID,
		ProducerID: s.cfg.ProducerID,
		UserID:     o.UserID,
		OrderID:    o.ID,
		OldStatus:  oldStatus,
		NewStatus:  newStatus,
		FilledQty:  o.FilledQty.String(),
		Reject:     reason,
	})
	return s.publisher.Publish(ctx, o.UserID, evt)
}

// sideFromProto converts the wire Side enum to the internal one. Returns
// SideBid for unknown values for safety; callers may ignore the mismatch
// since Side is used only for settlement arithmetic, which also takes the
// order's stored side.
func sideFromProto(s eventpb.Side) engine.Side {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return engine.SideBid
	case eventpb.Side_SIDE_SELL:
		return engine.SideAsk
	}
	return engine.SideBid
}
