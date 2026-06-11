package service

import (
	"errors"
	"strconv"
	"sync"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// userSeq serializes work per user (ADR-0068 invariant #1): at most one
// in-flight operation per user, full cross-user parallelism. A keyed mutex
// is sufficient for the MVP; it can be upgraded to a channel-FIFO worker
// (like counter's UserSequencer) if strict ordering under contention is
// needed.
type userSeq struct {
	mu    sync.Mutex
	locks map[uint64]*sync.Mutex
}

func newUserSeq() *userSeq { return &userSeq{locks: map[uint64]*sync.Mutex{}} }

func (s *userSeq) do(user uint64, fn func()) {
	s.mu.Lock()
	l := s.lockFor(user)
	s.mu.Unlock()
	l.Lock()
	defer l.Unlock()
	fn()
}

// do2 serializes one operation across TWO users' sequencers (ADR-0078 §8
// block trade). Locks are acquired in ascending user-id order so concurrent
// reversed pairs cannot deadlock; both checks and mutations run inside the
// double critical section, so per-leg validation has no TOCTOU window. The
// same-user case degrades to do — callers reject it upstream, this is the
// defensive path.
func (s *userSeq) do2(a, b uint64, fn func()) {
	if a == b {
		s.do(a, fn)
		return
	}
	lo, hi := a, b
	if lo > hi {
		lo, hi = hi, lo
	}
	s.mu.Lock()
	l1, l2 := s.lockFor(lo), s.lockFor(hi)
	s.mu.Unlock()
	l1.Lock()
	defer l1.Unlock()
	l2.Lock()
	defer l2.Unlock()
	fn()
}

// lockFor returns (creating on demand) a user's sequencer mutex. Caller
// holds s.mu.
func (s *userSeq) lockFor(user uint64) *sync.Mutex {
	l := s.locks[user]
	if l == nil {
		l = &sync.Mutex{}
		s.locks[user] = l
	}
	return l
}

func userIDString(user uint64) string {
	return strconv.FormatUint(user, 10)
}

// errInvalid wraps an invalid-argument message (mapped to CodeInvalidArgument
// at the Connect server).
func errInvalid(msg string) error { return errors.New(msg) }

// --- order store (guarded by s.mu; ops run inside seq.do) -------------------

func (s *Service) putOrder(o *Order) {
	s.mu.Lock()
	s.orders[o.OrderID] = o
	// ADR-0078 修订 #3: live orders are dedup-indexed by (user, coid). An
	// amend replacement inherits the old order's coid — the active index
	// then points at the replacement, shadowing the ring entry.
	s.indexCOIDLocked(o)
	s.mu.Unlock()
}

func (s *Service) getOrder(id uint64) *Order {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.orders[id]
}

func (s *Service) delOrder(id uint64) {
	s.mu.Lock()
	delete(s.orders, id)
	s.mu.Unlock()
}

// OrderCount reports the number of tracked (non-terminal) orders — test/obs.
func (s *Service) OrderCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.orders)
}

// --- journal emit -----------------------------------------------------------

func (s *Service) nextPerpSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.perpSeq++
	return s.perpSeq
}

// nextOrderSeq advances the order-event stream sequence (counter_seq_id). It is
// kept separate from perpSeq so the perp-journal stream stays gapless — the two
// topics carry independent monotonic sequences (ADR-0051).
func (s *Service) nextOrderSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orderSeq++
	return s.orderSeq
}

func (s *Service) nextAdlRound() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adlRound++
	return s.adlRound
}

func (s *Service) meta() *eventpb.EventMeta {
	return &eventpb.EventMeta{TsUnixMs: s.now(), ProducerId: s.cfg.ProducerID}
}

// maxLeverageForOrder estimates the post-order notional used for ADR-0070 risk
// tier selection. It stays intentionally conservative for flips: if an incoming
// order could both close and reopen, we size the tier from the submitted order
// notional because Match, not perp-counter, determines the exact execution mix.
// The existing-position add-in reads the order's target leg (ADR-0077). The
// cap itself resolves through the engine's ADR-0074 §10 min-chain (effective
// tier incl. the symbol's riskID ∩ admin customer limit).
func (s *Service) maxLeverageForOrder(user uint64, symbol string, idx uint8, side perpstate.Side, price, qty dec.Decimal) dec.Decimal {
	notional := price.Mul(qty)
	if pos, ok := s.eng.PositionOf(user, symbol, idx); ok && pos.Side == side {
		mark := s.eng.MarkOf(symbol)
		if mark.Sign() <= 0 {
			mark = price
		}
		notional = notional.Add(pos.Notional(mark))
	}
	return s.eng.EffectiveMaxLeverage(user, symbol, notional)
}

// placedOrderEvent builds the order-event Match consumes for a new order
// (ADR-0050/0068 §1). leverage / reduce_only are intentionally NOT on the wire:
// Match is margin-agnostic and treats the order as a plain limit/market order;
// perp-counter keeps those in its own order store and applies them when the
// fill returns. The symbol's `-PERP` suffix is the only thing distinguishing it
// from a spot order to Match — exactly the "Match 原样复用" property (ADR-0068 §1).
func (s *Service) placedOrderEvent(o *Order) *eventpb.OrderEvent {
	return &eventpb.OrderEvent{
		Meta:         s.meta(),
		CounterSeqId: s.nextOrderSeq(),
		Payload: &eventpb.OrderEvent_Placed{Placed: &eventpb.OrderPlaced{
			UserId: o.UserID, OrderId: o.OrderID, ClientOrderId: o.ClientID,
			Symbol: o.Symbol, Side: toEventSide(o.Side), OrderType: o.Type, Tif: o.TIF,
			Price: o.Price.String(), Qty: o.Qty.String(),
			// ADR-0075 handshake stamp: Match must hold the SAME catalog
			// version or reject. 0 = catalog disabled, Match skips the check.
			SymbolConfigVersion: o.ConfigVersion,
			// ADR-0083: Match derives the protection collar from its book for
			// slippage-stamped market orders. Perp has no quote_cap (IM is the
			// funds bound), so freeze_cap stays empty.
			SlippageBps: o.SlippageBps,
		}},
	}
}

// cancelOrderEvent builds the cancel order-event for Match.
func (s *Service) cancelOrderEvent(o *Order) *eventpb.OrderEvent {
	return &eventpb.OrderEvent{
		Meta:         s.meta(),
		CounterSeqId: s.nextOrderSeq(),
		Payload: &eventpb.OrderEvent_Cancel{Cancel: &eventpb.OrderCancel{
			UserId: o.UserID, OrderId: o.OrderID, Symbol: o.Symbol,
		}},
	}
}

func (s *Service) emitOrderStatus(o *Order, oldSt, newSt eventpb.InternalOrderStatus) {
	s.emitOrderStatusReason(o, oldSt, newSt, eventpb.RejectReason_REJECT_REASON_UNSPECIFIED)
}

// emitOrderStatusReason emits a perp-journal order-status transition, carrying
// the Match reject reason when the new status is REJECTED / EXPIRED. The
// position_idx stamp is the order→leg routing record replay/audit reads
// (ADR-0077 §6).
func (s *Service) emitOrderStatusReason(o *Order, oldSt, newSt eventpb.InternalOrderStatus, reason eventpb.RejectReason) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_OrderStatus{OrderStatus: &eventpb.PerpOrderStatusEvent{
			UserId: o.UserID, OrderId: o.OrderID, Symbol: o.Symbol,
			OldStatus: oldSt, NewStatus: newSt, FilledQty: o.FilledQty.String(),
			ReduceOnly: o.ReduceOnly, RejectReason: reason,
			PositionIdx: uint32(o.PositionIdx),
		}},
	})
}

func (s *Service) emitSettlement(o *Order, t *eventpb.Trade, side perpstate.Side, res perpstate.FillResult, fee settleFee) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Settlement{Settlement: &eventpb.PerpSettlementEvent{
			UserId: o.UserID, OrderId: o.OrderID, TradeId: t.GetTradeId(), Symbol: o.Symbol,
			FillSide: toEventSide(side), Price: t.GetPrice(), Qty: t.GetQty(),
			RealizedPnl: res.Realized.String(),
			MarginAdded: res.MarginAdded.String(), MarginReleased: res.MarginReleased.String(),
			PositionAfter: s.positionSnap(o.UserID, o.Symbol, o.PositionIdx),
			// ADR-0075: settle-time version, the row audit reads params from.
			SymbolConfigVersion: s.activeConfigVersion(o.Symbol),
			// ADR-0079 fee attribution: the signed requested amount plus how
			// it routed. The rates/rule are the order's admission pins.
			Fee:              fee.Amount.String(),
			LiquidityRole:    fee.Role,
			FeeRuleId:        o.FeeRuleID,
			FeeRate:          fee.Rate.String(),
			FeeAsset:         o.FeeAsset,
			FeeDeficit:       fee.Outcome.Deficit.String(),
			RebateSuppressed: fee.Suppressed,
			WalletAfter:      fee.Outcome.WalletAfter.String(),
		}},
	})
}

// emitBreachIfAny surfaces a clamped-off close excess as a
// REDUCE_ONLY_INVARIANT_BREACH (ADR-0077 §2 / ADR-0081 §2): the counterparty's
// execution stands while this leg under-settled — journal + log, alert /
// manual-repair input, never silently dropped.
func (s *Service) emitBreachIfAny(o *Order, t *eventpb.Trade, excess dec.Decimal) {
	if excess.Sign() <= 0 {
		return
	}
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_InvariantBreach{InvariantBreach: &eventpb.PerpInvariantBreachEvent{
			UserId: o.UserID, Symbol: o.Symbol, PositionIdx: uint32(o.PositionIdx),
			OrderId: o.OrderID, TradeId: t.GetTradeId(),
			Kind: "reduce_only_excess", ExcessQty: excess.String(), FillPrice: t.GetPrice(),
		}},
	})
}

// positionSnap builds the post-change snapshot of one leg, including a flat
// position (zeros) so consumers always see the resulting state.
func (s *Service) positionSnap(user uint64, symbol string, idx uint8) *eventpb.PerpPositionSnapshot {
	p, ok := s.eng.PositionRaw(user, symbol, idx)
	if !ok {
		return &eventpb.PerpPositionSnapshot{UserId: user, Symbol: symbol, PositionIdx: uint32(idx),
			Size: "0", EntryPrice: "0", Margin: "0", Leverage: "0", RealizedPnl: "0"}
	}
	return &eventpb.PerpPositionSnapshot{
		UserId: p.UserID, Symbol: p.Symbol, PositionIdx: uint32(p.PositionIdx), Side: toEventSide(p.Side),
		Size: p.Size.String(), EntryPrice: p.Entry.String(), Margin: p.Margin.String(),
		Leverage: p.Leverage.String(), RealizedPnl: p.Realized.String(), Version: p.Version,
		MarginMode: toWireMarginMode(p.Mode), RiskId: p.RiskID,
		RiskConfigVersion: p.RiskConfigVersion,
	}
}

// resolveOrderLeverage applies ADR-0074 §8's config-first leverage rule for
// PlaceOrder: an omitted leverage uses the symbol config (uniform across
// legs, ADR-0077 §7); a provided value that differs from the config is a
// write-through convenience set, allowed only while nothing live depends on
// the old value (every leg flat, no orders). symCfg is the locked config
// snapshot PlaceOrder already read. dryRun (PreCheckOrder, ADR-0078 §4)
// reports the same outcome without performing the write-through. Caller
// holds the user's seq lock.
func (s *Service) resolveOrderLeverage(user uint64, symbol string, symCfg engine.SymbolOrderConfig, reqLev dec.Decimal, dryRun bool) (lev dec.Decimal, mode perpstate.MarginMode, riskID uint32, reason string) {
	mode = symCfg.MarginMode
	riskID = symCfg.RiskID
	cfgLev := symCfg.Leverage
	switch {
	case reqLev.Sign() == 0:
		if cfgLev.Sign() <= 0 {
			return zero, mode, riskID, "leverage_required"
		}
		return cfgLev, mode, riskID, ""
	case cfgLev.Sign() > 0 && cfgLev.Cmp(reqLev) == 0:
		return reqLev, mode, riskID, ""
	default:
		// Config write-through. A live position or resting orders pin the old
		// leverage — the user must go through SetPositionLeverage (which
		// resizes margin / re-checks requirements) instead of a side effect.
		if (s.eng.SymbolNotionalForCap(user, symbol).Sign() > 0 && cfgLev.Sign() > 0) || s.hasActiveOrders(user, symbol) {
			return zero, mode, riskID, "leverage_conflict_use_set_leverage"
		}
		if dryRun {
			return reqLev, mode, riskID, ""
		}
		out := s.eng.SetLeverage(user, symbol, "", reqLev, s.cfg.TargetMarginBuffer)
		if !out.Accepted {
			return zero, mode, riskID, out.Reason
		}
		s.emitPositionConfig(user, symbol, "place_order", "")
		return reqLev, mode, riskID, ""
	}
}

func (s *Service) reject(req *perprpc.PlaceOrderRequest, reason string) *perprpc.PlaceOrderResponse {
	return &perprpc.PlaceOrderResponse{
		ClientOrderId: req.GetClientOrderId(), Accepted: false,
		RejectReason: reason, ReceivedTsUnixMs: s.now(),
	}
}

// --- enum helpers -----------------------------------------------------------

func fromEventSide(s eventpb.Side) perpstate.Side {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return perpstate.SideBuy
	case eventpb.Side_SIDE_SELL:
		return perpstate.SideSell
	default:
		return 0
	}
}

func toEventSide(s perpstate.Side) eventpb.Side {
	switch s {
	case perpstate.SideBuy:
		return eventpb.Side_SIDE_BUY
	case perpstate.SideSell:
		return eventpb.Side_SIDE_SELL
	default:
		return eventpb.Side_SIDE_UNSPECIFIED
	}
}

func isTerminal(st eventpb.InternalOrderStatus) bool {
	switch st {
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED:
		return true
	default:
		return false
	}
}

// --- no-op sinks (used when a dispatcher/journal isn't wired) ---------------

type noopDispatcher struct{}

func (noopDispatcher) DispatchOrder(string, *eventpb.OrderEvent) error  { return nil }
func (noopDispatcher) DispatchCancel(string, *eventpb.OrderEvent) error { return nil }

type noopJournal struct{}

func (noopJournal) Emit(*eventpb.PerpJournalEvent) {}
