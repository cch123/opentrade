package service

// cross_liquidation.go is the ADR-0074 §4 pool-triggered liquidation flow.
// Unlike the isolated path (liquidation.go), there is no per-position liq
// price and no Match round-trip: when the account pool's equity no longer
// covers its maintenance requirement, positions are force-closed at mark
// directly into the backstop account (ADR-0073 takeover semantics),
// largest-unrealized-loss first, re-evaluating pool health after each step.
// The whole plan executes synchronously inside the owning user's sequencer,
// so no in-flight registry is needed — a re-trigger on the next tick simply
// finds a healthy (or empty) pool.

import (
	"strconv"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// scanCrossLiquidations runs on every mark tick for users holding a cross
// position in the ticked symbol. The read-side health check is lock-free
// w.r.t. the sequencer; the executor re-checks inside it (TOCTOU guard).
func (s *Service) scanCrossLiquidations(symbol string) {
	if !s.liquidationEnabled(symbol) {
		return
	}
	for _, user := range s.eng.CrossUsersWith(symbol) {
		if user == s.cfg.BackstopAccount {
			continue // system inventory is managed off-system and must not recurse
		}
		if h, ok := s.eng.CrossPoolHealth(user); !ok || !h.Liquidatable() {
			continue
		}
		s.executeCrossLiquidation(user, symbol)
	}
}

// executeCrossLiquidation closes the user's cross positions until the pool
// is healthy again (or empty), then settles any remaining deficit against
// the insurance fund. triggerSymbol attributes the deficit (a multi-symbol
// pool deficit has no canonical owner; the triggering symbol is the
// documented v1 rule).
func (s *Service) executeCrossLiquidation(user uint64, triggerSymbol string) {
	s.seq.do(user, func() {
		h, ok := s.eng.CrossPoolHealth(user)
		if !ok || !h.Liquidatable() {
			return // saved between scan and sequencer entry
		}
		// De-risk: cancel every cross-mode order. Their reservations are
		// already counted in pool equity (recoverable cash), so the async
		// cancel confirmations do not change the health view — this only
		// stops further fills from racing the forced closes.
		s.cancelCrossOrdersFor(user)

		for _, entry := range s.eng.CrossClosePlanOf(user) {
			h, ok = s.eng.CrossPoolHealth(user)
			if !ok || !h.Liquidatable() {
				break // healthy again — stop closing
			}
			mark := s.eng.MarkOf(entry.Symbol)
			if mark.Sign() <= 0 {
				continue // no mark to value the close; try the next symbol
			}
			notional := mark.Mul(entry.Size)
			model, cfgVersion := s.riskModelForPosition(user, entry.Symbol, entry.PositionIdx)
			feeRate := model.EffectiveLiqFeeRate(notional, entry.RiskID)
			res, fee, okC := s.eng.CrossForceClose(user, entry.Symbol, entry.PositionIdx, mark, s.cfg.BackstopAccount, feeRate)
			if !okC {
				continue
			}
			s.emitCrossTakeover(user, entry, mark, res, fee, model, cfgVersion)
		}
		if covered, settled := s.eng.CrossSettleDeficit(user, triggerSymbol); settled {
			s.emitCrossDeficit(user, triggerSymbol, covered)
		}
	})
}

// cancelCrossOrdersFor dispatches cancels for all of the user's live
// cross-mode orders (any symbol). Caller holds the user's seq lock.
func (s *Service) cancelCrossOrdersFor(user uint64) {
	s.mu.Lock()
	var targets []*Order
	for _, o := range s.orders {
		if o.UserID == user && o.Mode == perpstate.MarginCross && !isTerminal(o.Status) &&
			o.Status != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL {
			targets = append(targets, o)
		}
	}
	s.mu.Unlock()
	for _, o := range targets {
		if err := s.dispatch.DispatchCancel(o.Symbol, s.cancelOrderEvent(o)); err != nil {
			continue
		}
		old := o.Status
		o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
		o.UpdatedMs = s.now()
		s.emitOrderStatus(o, old, o.Status)
	}
}

// emitCrossTakeover journals one forced cross close as an ADR-0073 takeover:
// the inventory lands on the backstop account and the lot id lets the
// perp-risk coordinator drive unwind/ADL exactly like an isolated takeover.
// The user keeps the realized PnL in their free balance (cross equity is
// account-level — only the liquidation fee moves to insurance here; a final
// negative balance is settled separately as a deficit event).
func (s *Service) emitCrossTakeover(user uint64, entry engine.CrossCloseEntry, mark dec.Decimal, res perpstate.FillResult, fee dec.Decimal, model perpstate.RiskModel, cfgVersion uint64) {
	snap := s.positionSnap(user, entry.Symbol, entry.PositionIdx)
	lotID := s.takeoverLotID(entry.Symbol, 0) + ":cross:" + userIDString(user) + ":" +
		strconv.Itoa(int(entry.PositionIdx)) + ":" + strconv.FormatUint(snap.GetVersion(), 10)
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Takeover{Takeover: &eventpb.PerpTakeoverEvent{
			UserId: user, Symbol: entry.Symbol,
			BankruptcyPrice: mark.String(), MarkPrice: mark.String(),
			ClosedQty: entry.Size.String(), RealizedPnl: res.Realized.String(),
			LotId:            lotID,
			TakenOverQty:     entry.Size.String(),
			TakeoverPrice:    mark.String(),
			TakenOverBalance: fee.String(),
			PositionVersion:  snap.GetVersion(),
			InsuranceDelta:   fee.String(),
			TakeoverNotional: mark.Mul(entry.Size).String(),
			BackstopUserId:   s.cfg.BackstopAccount,
			InventorySide:    toEventSide(entry.Side),
			Partial:          false,
			RiskTier:         s.risk.EffectiveTierIndex(mark.Mul(entry.Size), entry.RiskID),
			PositionAfter:    snap,
		}},
	})
}

// emitCrossDeficit journals the insurance fund absorbing a cross account's
// negative balance after all positions closed (pool bankruptcy). Shape: a
// PerpLiquidationEvent with closed_qty 0 — the cash leg without a position
// leg. The snapshot reads idx 0: the deficit is account-cash-scoped, not
// leg-scoped (every leg is already flat when this fires).
func (s *Service) emitCrossDeficit(user uint64, symbol string, covered dec.Decimal) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Liquidation{Liquidation: &eventpb.PerpLiquidationEvent{
			UserId: user, Symbol: symbol,
			MarkPrice: s.eng.MarkOf(symbol).String(),
			ClosedQty: "0", RealizedPnl: "0",
			InsuranceDelta: covered.Neg().String(),
			Backstop:       true,
			PositionAfter:  s.positionSnap(user, symbol, perpstate.IdxNet),
		}},
	})
}
