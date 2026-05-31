package service

// liquidation.go is the ADR-0070 liquidation execution flow. Mark ticks still
// drive a lock-free scan, and every mutation still re-enters the owning user's
// sequencer, but the action is no longer a single "whole position at bankruptcy"
// path: tiered MMR selects the trigger, partial liquidation tries to reduce back
// to safety, the backstop guarantees closure when Match liquidity disappears,
// and ADL tasks are handed to the profitable users' own sequencers.

import (
	"strconv"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perprisk"
	"github.com/xargin/opentrade/pkg/perpstate"
)

type liquidationMode uint8

const (
	liquidationPartial liquidationMode = 1
	liquidationFull    liquidationMode = 2
)

// liquidation tracks one in-flight forced reduce. The guard belongs to
// (user,symbol), not only to an order id, because mark ticks can arrive while a
// Match order is partially filled; the tick counter drives the ADR-0070
// liquidity-escalation path into the internal backstop.
type liquidation struct {
	userID     uint64
	symbol     string
	orderID    uint64
	mode       liquidationMode
	side       perpstate.Side
	orderPrice dec.Decimal
	bankruptcy dec.Decimal
	liqFeeRate dec.Decimal
	tier       int32
	ticks      int
}

func liqKey(user uint64, symbol string) string { return userIDString(user) + "|" + symbol }

// scanLiquidations runs on every mark tick: ADR-0072 narrows the read side to a
// liq-price threshold query, while the later sequencer step still performs the
// authoritative LiquidationCheck before any forced order is sent.
func (s *Service) scanLiquidations(symbol string) {
	if !s.risk.HasMMR() {
		return // no MMR configured → liquidation disabled
	}
	for _, cand := range s.eng.LiquidatablePositions(symbol, s.risk.MMRFunc()) {
		if cand.UserID == s.cfg.BackstopAccount {
			continue // system inventory is managed off-system and must not recurse
		}
		if liq := s.liquidationByKey(liqKey(cand.UserID, cand.Symbol)); liq != nil {
			s.advanceLiquidation(liq)
			continue
		}
		s.beginLiquidation(cand)
	}
}

// beginLiquidation cancels the position's resting orders and dispatches a
// reduce_only bankruptcy-price order to close it. Runs under the user's
// sequencer with a TOCTOU re-check because the indexed scan is only a read-side
// candidate pass.
func (s *Service) beginLiquidation(cand engine.LiquidationCandidate) {
	s.seq.do(cand.UserID, func() {
		key := liqKey(cand.UserID, cand.Symbol)
		if s.hasLiquidation(key) {
			return // already being liquidated
		}
		c, ok := s.eng.LiquidationCheck(cand.UserID, cand.Symbol, s.risk.MMRFunc())
		if !ok {
			return // moved back above maintenance since the scan
		}
		qty := c.Size
		price := c.BankruptcyPrice
		mode := liquidationFull
		if reduceQty := s.eng.ReduceToTarget(c.UserID, c.Symbol, s.risk.MMRFunc(), s.cfg.TargetMarginBuffer); reduceQty.Sign() > 0 && reduceQty.Cmp(c.Size) < 0 {
			// Partial liquidation prefers the smaller close at liq price when it
			// restores health. If the solver cannot find such a slice, the flow
			// falls back to full bankruptcy close so liquidation always makes
			// finite progress.
			qty = reduceQty
			price = c.LiqPrice
			mode = liquidationPartial
		}
		// Cancel resting orders so their IM frees and they don't race the
		// forced reduce order (IM is released when each OrderCancelled returns).
		s.cancelOrdersFor(c.UserID, c.Symbol)

		o := &Order{
			OrderID: s.nextID(), UserID: c.UserID, Symbol: c.Symbol,
			Side: c.Side.Opposite(), Type: eventpb.OrderType_ORDER_TYPE_LIMIT,
			TIF:   eventpb.TimeInForce_TIME_IN_FORCE_GTC,
			Price: price, Qty: qty, Leverage: zero, ReduceOnly: true,
			ReservedIM: zero, FilledQty: zero,
			Status:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
			CreatedMs: s.now(), UpdatedMs: s.now(),
		}
		s.putOrder(o)
		notional := c.Mark.Mul(c.Size)
		liq := &liquidation{
			userID: c.UserID, symbol: c.Symbol, orderID: o.OrderID, mode: mode, side: c.Side,
			orderPrice: price, bankruptcy: c.BankruptcyPrice,
			liqFeeRate: s.risk.LiqFeeRate(notional), tier: s.risk.TierIndex(notional),
		}
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
func (s *Service) cancelOrdersFor(user uint64, symbol string) {
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

// advanceLiquidation is called by later mark ticks while a forced reduce is
// still in flight. Match remains the first liquidity source, but after N ticks
// the service cancels the live order and performs an internal backstop transfer
// for the remaining quantity so ADR-0070's "finite-step closure" invariant does
// not depend on an external book.
func (s *Service) advanceLiquidation(liq *liquidation) {
	s.seq.do(liq.userID, func() {
		cur := s.liquidationFor(liq.orderID)
		if cur == nil {
			return
		}
		cur.ticks++
		if cur.ticks < s.cfg.BackstopAfterTicks {
			return
		}
		o := s.getOrder(cur.orderID)
		if o == nil || isTerminal(o.Status) {
			return
		}
		remaining := o.Qty.Sub(o.FilledQty)
		if remaining.Sign() <= 0 {
			s.finishLiquidation(cur)
			return
		}
		_ = s.dispatch.DispatchCancel(o.Symbol, s.cancelOrderEvent(o))
		res, insDelta, ok := s.eng.BackstopTakeover(o.UserID, o.Symbol, remaining, cur.bankruptcy,
			s.cfg.BackstopAccount, cur.mode == liquidationPartial, cur.liqFeeRate)
		if !ok {
			s.finishLiquidation(cur)
			s.delOrder(o.OrderID)
			return
		}
		o.FilledQty = o.Qty
		old := o.Status
		o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED
		o.UpdatedMs = s.now()
		s.emitTakeover(o, cur, remaining, res, insDelta)
		if s.shouldRunLocalADL(o.Symbol) {
			s.runADL(o.UserID, o.Symbol, cur.side, cur.bankruptcy)
		}
		s.emitOrderStatus(o, old, o.Status)
		s.finishLiquidation(cur)
		s.delOrder(o.OrderID)
	})
}

// settleLiquidationFill applies one fill of the bankruptcy order: it reduces the
// position and routes the freed equity to insurance (ADR-0068 §8), emits a
// PerpLiquidationEvent, and finishes the takeover when the position reaches
// flat. Caller holds the user's seq lock.
func (s *Service) settleLiquidationFill(o *Order, liq *liquidation, side perpstate.Side, matchSeq uint64,
	t *eventpb.Trade, statusAfter eventpb.InternalOrderStatus, filledAfter string) {
	fill := perpstate.Fill{Side: side, Price: dec.New(t.GetPrice()), Qty: dec.New(t.GetQty()), Fee: zero}
	var (
		res      perpstate.FillResult
		insDelta dec.Decimal
		applied  bool
	)
	if liq.mode == liquidationPartial {
		res, insDelta, applied = s.eng.ApplyPartialLiquidationFill(o.UserID, o.Symbol, matchSeq, fill, liq.liqFeeRate)
	} else {
		res, insDelta, applied = s.eng.ApplyLiquidationFill(o.UserID, o.Symbol, matchSeq, fill)
	}
	if !applied {
		return // replay
	}
	old := o.Status
	// Match is the source of truth for cumulative filled qty/status. The
	// settlement math is guarded by match_seq, but order lifecycle must still
	// mirror Match's post-fill view so later cancels release only the real
	// unfilled remainder.
	if filledAfter != "" {
		o.FilledQty = dec.New(filledAfter)
	}
	if statusAfter != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED {
		o.Status = statusAfter
	}
	o.UpdatedMs = s.now()
	s.emitLiquidation(o, liq, t, res, insDelta)
	if s.shouldRunLocalADL(o.Symbol) {
		s.runADL(o.UserID, o.Symbol, liq.side, liq.bankruptcy)
	}
	if o.Status != old {
		s.emitOrderStatus(o, old, o.Status)
	}
	if _, stillOpen := s.eng.PositionOf(o.UserID, o.Symbol); !stillOpen {
		s.finishLiquidation(liq) // position closed
	} else if liq.mode == liquidationPartial && isTerminal(o.Status) {
		s.finishLiquidation(liq) // re-arm next tick for a fresh health check
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
			AdlQueued:      s.adlQueued(o.Symbol),
			Partial:        liq.mode == liquidationPartial,
			Backstop:       false,
			RiskTier:       liq.tier,
			PositionAfter:  s.positionSnap(o.UserID, o.Symbol),
		}},
	})
}

func (s *Service) emitTakeover(o *Order, liq *liquidation, qty dec.Decimal, res perpstate.FillResult, insDelta dec.Decimal) {
	takeoverNotional := qty.Mul(liq.bankruptcy)
	lotID := s.takeoverLotID(o.Symbol, o.OrderID)
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Takeover{Takeover: &eventpb.PerpTakeoverEvent{
			UserId: o.UserID, Symbol: o.Symbol, LiqOrderId: o.OrderID,
			BankruptcyPrice: liq.bankruptcy.String(), MarkPrice: s.eng.MarkOf(o.Symbol).String(),
			ClosedQty: qty.String(), RealizedPnl: res.Realized.String(),
			LotId:            lotID,
			TakenOverQty:     qty.String(),
			TakeoverPrice:    liq.bankruptcy.String(),
			TakenOverBalance: insDelta.String(),
			PositionVersion:  s.positionSnap(o.UserID, o.Symbol).GetVersion(),
			InsuranceDelta:   insDelta.String(),
			TakeoverNotional: takeoverNotional.String(),
			BackstopUserId:   s.cfg.BackstopAccount,
			InventorySide:    toEventSide(liq.side),
			Partial:          liq.mode == liquidationPartial,
			RiskTier:         liq.tier,
			AdlQueued:        s.adlQueued(o.Symbol),
			PositionAfter:    s.positionSnap(o.UserID, o.Symbol),
		}},
	})
}

func (s *Service) runADL(liquidatedUser uint64, symbol string, liquidatedSide perpstate.Side, bankruptcy dec.Decimal) {
	// ADR-0073 makes perp-risk the lot owner and ADL planner. A shard may only
	// execute a version-stamped task for a specific lot via ExecuteAdlTask; it
	// must not locally turn an insurance deficit into ADL fund credit.
}

func (s *Service) shouldRunLocalADL(symbol string) bool {
	return false
}

func (s *Service) adlQueued(symbol string) bool {
	return false
}

// ExecuteAdlTask is the shard-side ADR-0071 entrypoint for the external
// perp-risk coordinator. The task enters the owning user's sequencer and is
// applied only if the coordinator's observed side/PosSeq/PositionVersion still
// match the current position; this is the cross-shard TOCTOU guard that
// replaces the old same-process candidate read.
func (s *Service) ExecuteAdlTask(task perprisk.ADLTask) perprisk.ADLTaskResult {
	result := perprisk.ADLTaskResult{}
	s.seq.do(task.UserID, func() {
		res, factQty, ok := s.eng.ApplyAdlCloseGuarded(task.UserID, task.Symbol, task.Qty, task.Price,
			task.Side, task.PosSeq, task.PositionVersion, task.AdlRound, true)
		if !ok {
			return
		}
		s.emitADL(task.UserID, task.Symbol, task.LotID, task.Price, task.Qty, factQty, res, task.AdlRound)
		result = perprisk.ADLTaskResult{Applied: true, FactQty: factQty, RealizedPnL: res.Realized}
	})
	return result
}

// ADLCandidates is the shard-local candidate source for ADR-0071. The external
// coordinator asks every shard for candidates scoped to one bankruptcy/ADL
// price, then dispatches version-stamped tasks back to the owning shard.
func (s *Service) ADLCandidates(symbol string, adlPrice dec.Decimal, excludeUser uint64) []perprisk.ADLCandidate {
	src := s.eng.SelectAnyAdlCandidates(symbol, adlPrice, excludeUser)
	out := make([]perprisk.ADLCandidate, 0, len(src))
	for _, c := range src {
		out = append(out, perprisk.ADLCandidate{
			UserID: c.UserID, Symbol: c.Symbol, Side: c.Side,
			Size: c.Size, Score: c.Score, SacrificePerQty: c.SacrificePerQty,
			PosSeq: c.LastMatchSeq, PositionVersion: c.PositionVersion,
		})
	}
	return out
}

func (s *Service) emitADL(user uint64, symbol, lotID string, price, requestedQty, factQty dec.Decimal, res perpstate.FillResult, round uint64) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Adl{Adl: &eventpb.PerpAdlEvent{
			UserId: user, Symbol: symbol, LotId: lotID, AdlRound: round,
			Price: price.String(), RequestedQty: requestedQty.String(), FactQty: factQty.String(),
			ClosedQty: factQty.String(), RealizedPnl: res.Realized.String(),
			InsuranceDelta: zero.String(),
			PositionAfter:  s.positionSnap(user, symbol),
		}},
	})
}

func (s *Service) takeoverLotID(symbol string, orderID uint64) string {
	producer := s.cfg.ProducerID
	if producer == "" {
		producer = "perp-counter"
	}
	return producer + ":" + symbol + ":" + strconv.FormatUint(orderID, 10)
}

// --- liquidation registry (guarded by s.mu) --------------------------------

func (s *Service) hasLiquidation(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.liqByKey[key]
	return ok
}

func (s *Service) liquidationByKey(key string) *liquidation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.liqByKey[key]
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
func (s *Service) ordersFor(user uint64, symbol string) []*Order {
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
