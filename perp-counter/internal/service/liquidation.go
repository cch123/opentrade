package service

// liquidation.go is the liquidation execution flow (ADR-0068 §8). Each mark tick
// is the trigger: a position whose isolated collateral pool breaches the
// maintenance margin rate is taken over — its resting orders are cancelled and a
// reduce_only order at the bankruptcy price is sent to Match to close it. The
// bankruptcy order's fills settle to the symbol's insurance fund (not the user's
// wallet — isolated margin is forfeit), incrementally so partial fills are
// correct. Detection ran lock-free (the scan); every state change here runs
// inside the user's sequencer (invariant #1) with a TOCTOU re-check.
//
// MVP bounds (ADR-0068 §8/§9): one bankruptcy order per position (no re-dispatch
// if it only partially fills for lack of liquidity); ADL is alert-only
// (adl_queued is set when the insurance fund goes negative, not auto-executed).

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// liquidation tracks one in-flight position takeover.
type liquidation struct {
	userID     string
	symbol     string
	orderID    uint64      // the bankruptcy reduce_only order
	bankruptcy dec.Decimal // price the order was placed at
}

func liqKey(user, symbol string) string { return user + "|" + symbol }

// scanLiquidations runs on every mark tick: it finds positions breaching the
// maintenance margin rate and starts a takeover for each not already in flight.
func (s *Service) scanLiquidations(symbol string) {
	if s.cfg.MMR.Sign() <= 0 {
		return // no MMR configured → liquidation disabled
	}
	for _, cand := range s.eng.LiquidatablePositions(symbol, s.cfg.MMR) {
		s.beginLiquidation(cand)
	}
}

// beginLiquidation cancels the position's resting orders and dispatches a
// reduce_only bankruptcy-price order to close it. Runs under the user's
// sequencer with a TOCTOU re-check (the scan was lock-free).
func (s *Service) beginLiquidation(cand engine.LiquidationCandidate) {
	s.seq.do(cand.UserID, func() {
		key := liqKey(cand.UserID, cand.Symbol)
		if s.hasLiquidation(key) {
			return // already being liquidated
		}
		c, ok := s.eng.LiquidationCheck(cand.UserID, cand.Symbol, s.cfg.MMR)
		if !ok {
			return // moved back above maintenance since the scan
		}
		// Cancel resting orders so their IM frees and they don't race the
		// bankruptcy order (IM is released when each OrderCancelled returns).
		s.cancelOrdersFor(c.UserID, c.Symbol)

		o := &Order{
			OrderID: s.nextID(), UserID: c.UserID, Symbol: c.Symbol,
			Side: c.Side.Opposite(), Type: eventpb.OrderType_ORDER_TYPE_LIMIT,
			TIF:   eventpb.TimeInForce_TIME_IN_FORCE_GTC,
			Price: c.BankruptcyPrice, Qty: c.Size, Leverage: zero, ReduceOnly: true,
			ReservedIM: zero, FilledQty: zero,
			Status:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
			CreatedMs: s.now(), UpdatedMs: s.now(),
		}
		s.putOrder(o)
		liq := &liquidation{userID: c.UserID, symbol: c.Symbol, orderID: o.OrderID, bankruptcy: c.BankruptcyPrice}
		s.registerLiquidation(key, o.OrderID, liq)

		if err := s.dispatch.DispatchOrder(o.Symbol, s.placedOrderEvent(o)); err != nil {
			s.delOrder(o.OrderID)
			s.unregisterLiquidation(liq)
			return
		}
		s.emitOrderStatus(o, eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED,
			eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW)
	})
}

// cancelOrdersFor dispatches cancels for all of (user, symbol)'s live orders.
// Caller holds the user's seq lock. The bankruptcy order does not exist yet, so
// nothing here cancels it.
func (s *Service) cancelOrdersFor(user, symbol string) {
	for _, o := range s.ordersFor(user, symbol) {
		if isTerminal(o.Status) || o.Status == eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL {
			continue
		}
		if err := s.dispatch.DispatchCancel(o.Symbol, s.cancelOrderEvent(o)); err != nil {
			continue
		}
		old := o.Status
		o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
		o.UpdatedMs = s.now()
		s.emitOrderStatus(o, old, o.Status)
	}
}

// settleLiquidationFill applies one fill of the bankruptcy order: it reduces the
// position and routes the freed equity to insurance (ADR-0068 §8), emits a
// PerpLiquidationEvent, and finishes the takeover when the position reaches
// flat. Caller holds the user's seq lock.
func (s *Service) settleLiquidationFill(o *Order, liq *liquidation, side perpstate.Side, matchSeq uint64,
	t *eventpb.Trade, statusAfter eventpb.InternalOrderStatus, filledAfter string) {
	fill := perpstate.Fill{Side: side, Price: dec.New(t.GetPrice()), Qty: dec.New(t.GetQty()), Fee: zero}
	res, insDelta, applied := s.eng.ApplyLiquidationFill(o.UserID, o.Symbol, matchSeq, fill)
	if !applied {
		return // replay
	}
	old := o.Status
	if filledAfter != "" {
		o.FilledQty = dec.New(filledAfter)
	}
	if statusAfter != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED {
		o.Status = statusAfter
	}
	o.UpdatedMs = s.now()
	s.emitLiquidation(o, liq, t, res, insDelta)
	if o.Status != old {
		s.emitOrderStatus(o, old, o.Status)
	}
	if _, stillOpen := s.eng.PositionOf(o.UserID, o.Symbol); !stillOpen {
		s.finishLiquidation(liq) // position closed
	}
	if isTerminal(o.Status) {
		s.delOrder(o.OrderID)
	}
}

// emitLiquidation writes a PerpLiquidationEvent for one bankruptcy-order fill.
func (s *Service) emitLiquidation(o *Order, liq *liquidation, t *eventpb.Trade, res perpstate.FillResult, insDelta dec.Decimal) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Liquidation{Liquidation: &eventpb.PerpLiquidationEvent{
			UserId: o.UserID, Symbol: o.Symbol, LiqOrderId: o.OrderID,
			BankruptcyPrice: liq.bankruptcy.String(), MarkPrice: s.eng.MarkOf(o.Symbol).String(),
			ClosedQty: t.GetQty(), RealizedPnl: res.Realized.String(),
			InsuranceDelta: insDelta.String(),
			// MVP: fund-negative → ADL queue alert only, not auto-executed (§9).
			AdlQueued:     s.eng.InsuranceFund(o.Symbol).Sign() < 0,
			PositionAfter: s.positionSnap(o.UserID, o.Symbol),
		}},
	})
}

// --- liquidation registry (guarded by s.mu) --------------------------------

func (s *Service) hasLiquidation(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.liqByKey[key]
	return ok
}

func (s *Service) liquidationFor(orderID uint64) *liquidation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.liqByOrder[orderID]
}

func (s *Service) registerLiquidation(key string, orderID uint64, liq *liquidation) {
	s.mu.Lock()
	s.liqByKey[key] = liq
	s.liqByOrder[orderID] = liq
	s.mu.Unlock()
}

func (s *Service) unregisterLiquidation(liq *liquidation) {
	s.mu.Lock()
	delete(s.liqByKey, liqKey(liq.userID, liq.symbol))
	delete(s.liqByOrder, liq.orderID)
	s.mu.Unlock()
}

// finishLiquidation clears the in-flight guard so the position can be liquidated
// again if it re-breaches later.
func (s *Service) finishLiquidation(liq *liquidation) { s.unregisterLiquidation(liq) }

// clearLiquidationIfAny re-arms liquidation when a bankruptcy order terminates
// abnormally (Match rejected it). Without this the position's guard would wedge
// it out of future liquidation. Returns true if orderID was a bankruptcy order.
func (s *Service) clearLiquidationIfAny(orderID uint64) bool {
	if liq := s.liquidationFor(orderID); liq != nil {
		s.finishLiquidation(liq)
		return true
	}
	return false
}

// ordersFor returns the tracked orders for (user, symbol).
func (s *Service) ordersFor(user, symbol string) []*Order {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Order
	for _, o := range s.orders {
		if o.UserID == user && o.Symbol == symbol {
			out = append(out, o)
		}
	}
	return out
}
