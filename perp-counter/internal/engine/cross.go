package engine

// cross.go is the ADR-0074 P1 cross-margin engine surface: the account-level
// pool evaluation (one pool per user per settle asset, §4 rule #1), the
// order-admission candidate check, the isolated↔cross mode switches, and the
// pool-triggered force-close primitives.
//
// Equity views differ on purpose:
//   - admission uses Available - thisOrderIM (reservations excluded) so
//     stacked orders consume free cash and rule #4 holds;
//   - liquidation uses Available + CrossReserved (reserved cash is
//     recoverable by cancelling, which the liquidation flow does first), so
//     an in-flight order cannot make a healthy pool look liquidatable.

import (
	"sort"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// crossPositionsLocked returns the user's live cross positions. Caller holds
// e.mu (any).
func (e *Engine) crossPositionsLocked(user uint64) []*perpstate.Position {
	var out []*perpstate.Position
	for _, p := range e.positions[user] {
		if p.Mode == perpstate.MarginCross && !p.IsFlat() {
			out = append(out, p)
		}
	}
	return out
}

// crossHealthLocked evaluates the user's cross pool in the liquidation view
// (drawable = free balance + cross-order reservations). Caller holds e.mu.
func (e *Engine) crossHealthLocked(w *Wallet, positions []*perpstate.Position) perpstate.PoolHealth {
	drawable := w.Available.Add(w.CrossReserved)
	return perpstate.StandardRisk{Model: e.risk}.Eval(perpstate.Cross(drawable, positions), e.marks)
}

// crossCandidateHealthLocked evaluates the user's cross pool with cand
// replacing (or adding) the user's position in cand.Symbol, at the given
// drawable. Caller holds e.mu.
func (e *Engine) crossCandidateHealthLocked(user uint64, cand *perpstate.Position, drawable dec.Decimal) perpstate.PoolHealth {
	positions := []*perpstate.Position{cand}
	for _, p := range e.crossPositionsLocked(user) {
		if p.Symbol == cand.Symbol {
			continue
		}
		positions = append(positions, p)
	}
	return perpstate.StandardRisk{Model: e.risk}.Eval(perpstate.Cross(drawable, positions), e.marks)
}

// CrossUsersWith returns the users holding a live cross position in symbol,
// sorted. A mark tick fans pool checks out across these users.
func (e *Engine) CrossUsersWith(symbol string) []uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	byUser := e.crossUsers[symbol]
	out := make([]uint64, 0, len(byUser))
	for u := range byUser {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// CrossPoolHealth evaluates the user's cross pool (liquidation view).
// ok=false when the user holds no cross positions or no risk model is set.
func (e *Engine) CrossPoolHealth(user uint64) (perpstate.PoolHealth, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.riskSet {
		return perpstate.PoolHealth{}, false
	}
	positions := e.crossPositionsLocked(user)
	if len(positions) == 0 {
		return perpstate.PoolHealth{}, false
	}
	w := e.wallets[user]
	if w == nil {
		w = &Wallet{Available: zero, Reserved: zero, CrossReserved: zero}
	}
	return e.crossHealthLocked(w, positions), true
}

// CrossOrderCheck is the ADR-0074 §4 admission gate: simulate the order's
// maximum position-increase impact on the user's cross pool and require the
// candidate to clear the initial requirement plus buffer. im is the order's
// initial-margin reservation about to be taken (the candidate drawable is
// Available - im: stacking orders consume free cash, rule #4).
func (e *Engine) CrossOrderCheck(user uint64, symbol string, side perpstate.Side, price, qty, leverage, im, buffer dec.Decimal) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.riskSet {
		return "cross_unavailable", false
	}
	w := e.wallets[user]
	if w == nil {
		return "insufficient_margin", false
	}
	drawable := w.Available.Sub(im)
	if drawable.Sign() < 0 {
		return "insufficient_margin", false
	}
	cand := perpstate.Position{UserID: user, Symbol: symbol, Mode: perpstate.MarginCross, Leverage: leverage}
	if bySym := e.positions[user]; bySym != nil {
		if p := bySym[symbol]; p != nil {
			cand = *p
			if cand.Leverage.Sign() <= 0 {
				cand.Leverage = leverage
			}
		}
	}
	cand.ApplyFill(perpstate.Fill{Side: side, Price: price, Qty: qty})
	h := e.crossCandidateHealthLocked(user, &cand, drawable)
	if h.Liquidatable() {
		return "unsafe_below_maintenance", false
	}
	if !h.MeetsInitial(buffer) {
		return "insufficient_margin", false
	}
	return "", true
}

// SwitchToCross flips (user, symbol) to cross margin (ADR-0074 §5
// isolated→cross): the isolated margin is released to the free balance in
// full (open-question decision: no partial retention) and the position joins
// the account pool, which must clear both the maintenance and the initial
// requirement (+imBuffer) with the position included.
func (e *Engine) SwitchToCross(user uint64, symbol, opID string, imBuffer dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	p := e.positionLocked(user, symbol)
	if p.Mode == perpstate.MarginCross {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", zero)) // idempotent no-op
	}
	if p.IsFlat() {
		// Pure config flip; any residual margin on a flat record drains back.
		moved := p.Margin
		if moved.Sign() > 0 {
			w.Available = w.Available.Add(moved)
			p.Margin = zero
		}
		p.Mode = perpstate.MarginCross
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", moved))
	}
	if !e.riskSet {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "cross_unavailable", zero))
	}
	cand := *p
	cand.Mode = perpstate.MarginCross
	cand.Margin = zero
	// Candidate drawable: free balance + the released isolated margin (plus
	// cross reservations — liquidation view — none of which change here).
	drawable := w.Available.Add(w.CrossReserved).Add(p.Margin)
	h := e.crossCandidateHealthLocked(user, &cand, drawable)
	if h.Liquidatable() {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "unsafe_below_maintenance", zero))
	}
	if !h.MeetsInitial(imBuffer) {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "below_initial_requirement", zero))
	}
	moved := p.Margin
	w.Available = w.Available.Add(moved)
	p.Margin = zero
	p.Mode = perpstate.MarginCross
	p.Version++
	e.syncIndexesLocked(user, symbol, p)
	return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", moved))
}

// SwitchToIsolated flips (user, symbol) to isolated margin (ADR-0074 §5
// cross→isolated). The locked margin is max(requestedMargin, position IM
// requirement + imBuffer), drawn from the free balance. Both resulting
// states must be safe: the isolated position clear of maintenance by
// removeBuffer, and the remaining cross pool (without this position, with
// the cash moved out) not liquidatable.
func (e *Engine) SwitchToIsolated(user uint64, symbol, opID string, requestedMargin, imBuffer, removeBuffer dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	p := e.positionLocked(user, symbol)
	if p.Mode != perpstate.MarginCross {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", zero)) // idempotent no-op
	}
	if p.IsFlat() {
		p.Mode = perpstate.MarginIsolated
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", zero))
	}
	mark := e.marks[symbol]
	if mark.Sign() <= 0 {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "no_mark", zero))
	}
	std := perpstate.StandardRisk{Model: e.risk}
	imReq := std.PositionInitialRequirement(p, e.marks)
	target := dec.Max(requestedMargin, imReq.Add(imBuffer))
	if w.Available.Cmp(target) < 0 {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "insufficient_free_balance", zero))
	}
	// Isolated health at the target margin. The IM floor is already baked
	// into target (max(requested, IM requirement + buffer)); a deep-loss
	// position needs a larger requestedMargin than the floor implies.
	cand := *p
	cand.Mode = perpstate.MarginIsolated
	cand.Margin = target
	if _, ok := e.marginHealthSafeLocked(&cand, target, removeBuffer); !ok {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "isolated_unsafe_target_margin", zero))
	}
	// Remaining cross pool without this position, conservative view
	// (admission-style: reservations excluded, cash moved out).
	remaining := make([]*perpstate.Position, 0)
	for _, cp := range e.crossPositionsLocked(user) {
		if cp.Symbol == symbol {
			continue
		}
		remaining = append(remaining, cp)
	}
	if len(remaining) > 0 {
		h := perpstate.StandardRisk{Model: e.risk}.Eval(
			perpstate.Cross(w.Available.Sub(target), remaining), e.marks)
		if h.Liquidatable() {
			return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "cross_pool_unsafe_after_exit", zero))
		}
	}
	w.Available = w.Available.Sub(target)
	p.Margin = target
	p.Mode = perpstate.MarginIsolated
	p.Version++
	e.syncIndexesLocked(user, symbol, p)
	return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", target))
}

// CrossForceClose closes the user's full cross position in symbol at price
// (the cross liquidation execution step). Realized PnL settles into the free
// balance, the liquidation fee moves to the symbol's insurance fund, and the
// closed inventory lands on the backstop account (ADR-0073 semantics). The
// caller drives the position-by-position plan and finishes with
// CrossSettleDeficit.
func (e *Engine) CrossForceClose(user uint64, symbol string, price dec.Decimal, backstopUser uint64, liqFeeRate dec.Decimal) (res perpstate.FillResult, fee dec.Decimal, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bySym := e.positions[user]
	if bySym == nil {
		return perpstate.FillResult{}, zero, false
	}
	p := bySym[symbol]
	if p == nil || p.IsFlat() || p.Mode != perpstate.MarginCross {
		return perpstate.FillResult{}, zero, false
	}
	originalSide := p.Side
	closeQty := p.Size
	fee = price.Mul(closeQty).Mul(liqFeeRate)
	res = p.ApplyFill(perpstate.Fill{Side: p.Side.Opposite(), Price: price, Qty: closeQty, Fee: fee})
	p.Version++
	e.routeCashLocked(user, res) // cross: realized - fee into Available
	e.insurance[symbol] = e.insurance[symbol].Add(fee)
	e.syncIndexesLocked(user, symbol, p)
	if closeQty.Sign() > 0 {
		e.applyBackstopInventoryLocked(backstopUser, symbol, originalSide, price, closeQty)
	}
	return res, fee, true
}

// CrossSettleDeficit zeroes a negative free balance left by a cross
// liquidation against the symbol's insurance fund (the pool lost more than
// its cash). covered is the amount the fund absorbed. The symbol attribution
// is the liquidation's triggering symbol — a multi-symbol pool deficit has
// no canonical owner, so the trigger symbol is the documented v1 rule.
func (e *Engine) CrossSettleDeficit(user uint64, symbol string) (covered dec.Decimal, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	if w.Available.Sign() >= 0 {
		return zero, false
	}
	covered = w.Available.Neg()
	e.insurance[symbol] = e.insurance[symbol].Add(w.Available)
	w.Available = zero
	return covered, true
}
