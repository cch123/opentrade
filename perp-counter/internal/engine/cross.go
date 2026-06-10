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

// crossPositionsLocked returns the user's live cross position legs. Caller
// holds e.mu (any).
func (e *Engine) crossPositionsLocked(user uint64) []*perpstate.Position {
	var out []*perpstate.Position
	for _, sp := range e.positions[user] {
		for _, p := range sp.liveLegs(nil) {
			if p.Mode == perpstate.MarginCross && !p.IsFlat() {
				out = append(out, p)
			}
		}
	}
	return out
}

// crossHealthLocked evaluates the user's cross pool in the liquidation view
// (drawable = free balance + cross-order reservations). Caller holds e.mu.
func (e *Engine) crossHealthLocked(w *Wallet, positions []*perpstate.Position) perpstate.PoolHealth {
	drawable := w.Available.Add(w.CrossReserved)
	return e.poolRiskLocked().Eval(perpstate.Cross(drawable, positions), e.marks)
}

// crossCandidateHealthLocked evaluates the user's cross pool with cand
// replacing (or adding) the user's (cand.Symbol, cand.PositionIdx) leg, at
// the given drawable. Exclusion is by (symbol, position_idx), NOT by symbol —
// in hedge mode the same symbol's other leg is a distinct pool member whose
// gross requirement must stay in the evaluation (ADR-0077 §4). Caller holds
// e.mu.
func (e *Engine) crossCandidateHealthLocked(user uint64, cand *perpstate.Position, drawable dec.Decimal) perpstate.PoolHealth {
	return e.crossCandidatesHealthLocked(user, []*perpstate.Position{cand}, drawable)
}

// crossCandidatesHealthLocked is the multi-leg variant: every cand replaces
// its (symbol, position_idx) leg in the pool. Used by the margin-mode switch,
// which moves both hedge legs atomically (ADR-0077 §7). Caller holds e.mu.
func (e *Engine) crossCandidatesHealthLocked(user uint64, cands []*perpstate.Position, drawable dec.Decimal) perpstate.PoolHealth {
	positions := append([]*perpstate.Position{}, cands...)
	for _, p := range e.crossPositionsLocked(user) {
		replaced := false
		for _, cand := range cands {
			if p.Symbol == cand.Symbol && p.PositionIdx == cand.PositionIdx {
				replaced = true
				break
			}
		}
		if !replaced {
			positions = append(positions, p)
		}
	}
	return e.poolRiskLocked().Eval(perpstate.Cross(drawable, positions), e.marks)
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
	if !e.riskConfiguredLocked() {
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
// candidate to clear the initial requirement plus buffer.
//
// Drawable is Available as-is. Resting orders are accounted exactly: each
// holds a CrossReserved reservation equal to its filled initial requirement,
// and Available already excludes those holds — so excluding their future
// requirements from the candidate is offset one-for-one by the excluded
// cash. This order's own reservation cycle is net-zero on Available (held
// now, released on fill while its requirement appears), so it must NOT be
// subtracted here — only counted once, as the candidate's requirement.
// Stacking still consumes free cash via ReserveCross (rule #4).
func (e *Engine) CrossOrderCheck(user uint64, symbol string, idx uint8, side perpstate.Side, price, qty, leverage, im, buffer dec.Decimal) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.hasRiskLocked(symbol) {
		return "cross_unavailable", false
	}
	w := e.wallets[user]
	if w == nil {
		return "insufficient_margin", false
	}
	if w.Available.Cmp(im) < 0 {
		return "insufficient_margin", false
	}
	drawable := w.Available
	cand := perpstate.Position{UserID: user, Symbol: symbol, PositionIdx: idx,
		Mode: perpstate.MarginCross, Leverage: leverage}
	if p := e.legPeekLocked(user, symbol, idx); p != nil {
		cand = *p
		if cand.Leverage.Sign() <= 0 {
			cand.Leverage = leverage
		}
	}
	// The candidate simulates this order's maximum position impact through
	// the same settlement core fills use: net keeps flip semantics, a hedge
	// leg clamps (a reduce_only close order can only shrink it).
	_, _ = applyFillByIdx(&cand, perpstate.Fill{Side: side, Price: price, Qty: qty})
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
// requirement (+imBuffer) with the position included. In hedge mode BOTH legs
// switch atomically — all validations pass or nothing moves (ADR-0077 §7);
// flat legs of the inactive mode flip too so margin mode stays uniform per
// (user, symbol).
func (e *Engine) SwitchToCross(user uint64, symbol, opID string, imBuffer dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	legs := e.modeLegsLocked(user, symbol)
	allLegs := e.legsLocked(user, symbol) // includes inactive-mode flat legs
	if marginModeUniform(allLegs, perpstate.MarginCross) {
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, true, "", zero)) // idempotent no-op
	}

	// Live isolated legs need the pool admission check; flat legs are pure
	// config flips with a defensive residual-margin drain.
	var cands []*perpstate.Position
	released := zero
	for _, p := range legs {
		if p.IsFlat() || p.Mode == perpstate.MarginCross {
			continue
		}
		cand := *p
		cand.Mode = perpstate.MarginCross
		cand.Margin = zero
		cands = append(cands, &cand)
		released = released.Add(p.Margin)
	}
	if len(cands) > 0 {
		if !e.hasRiskLocked(symbol) {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "cross_unavailable", zero))
		}
		// Candidate drawable: free balance + every released isolated margin
		// (plus cross reservations — liquidation view — unchanged here).
		drawable := w.Available.Add(w.CrossReserved).Add(released)
		h := e.crossCandidatesHealthLocked(user, cands, drawable)
		if h.Liquidatable() {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "unsafe_below_maintenance", zero))
		}
		if !h.MeetsInitial(imBuffer) {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "below_initial_requirement", zero))
		}
	}

	// Apply: drain margins to the free balance, flip every leg.
	moved := zero
	var legMoves []LegMove
	for _, p := range allLegs {
		before := p.Margin
		if p.Margin.Sign() > 0 {
			w.Available = w.Available.Add(p.Margin)
			moved = moved.Add(p.Margin)
			p.Margin = zero
		}
		changed := p.Mode != perpstate.MarginCross || before.Sign() > 0
		p.Mode = perpstate.MarginCross
		if changed {
			p.Version++
			legMoves = append(legMoves, LegMove{
				PositionIdx: p.PositionIdx, Moved: before,
				MarginBefore: before, MarginAfter: zero, Version: p.Version,
			})
		}
		e.syncIndexesLocked(user, symbol, p)
	}
	out := e.opStateSymbolLocked(user, symbol, w, true, "", moved)
	out.LegMoves = legMoves
	return e.cacheOpLocked(opID, out)
}

// SwitchToIsolated flips (user, symbol) to isolated margin (ADR-0074 §5
// cross→isolated). Per leg, the locked margin is max(requestedMargin, leg IM
// requirement + imBuffer), drawn from the free balance. Every resulting state
// must be safe: each isolated leg clear of maintenance by removeBuffer, and
// the remaining cross pool (without this symbol's legs, with the cash moved
// out) not liquidatable. In hedge mode both legs switch atomically (ADR-0077
// §7); requestedMargin targets a single position and is rejected there — use
// AdjustIsolatedMargin per leg afterwards.
func (e *Engine) SwitchToIsolated(user uint64, symbol, opID string, requestedMargin, imBuffer, removeBuffer dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	legs := e.modeLegsLocked(user, symbol)
	allLegs := e.legsLocked(user, symbol)
	if marginModeUniform(allLegs, perpstate.MarginIsolated) {
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, true, "", zero)) // idempotent no-op
	}
	if len(legs) > 1 && requestedMargin.Sign() > 0 {
		// target_margin addresses ONE position; in hedge mode the switch moves
		// two legs and silently splitting the request would be a guess.
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "target_margin_unsupported_in_hedge", zero))
	}

	// Per-leg targets for live cross legs; flat legs flip config only.
	type isoTarget struct {
		p      *perpstate.Position
		target dec.Decimal
	}
	var targets []isoTarget
	totalTarget := zero
	std := e.poolRiskLocked()
	for _, p := range legs {
		if p.IsFlat() || p.Mode != perpstate.MarginCross {
			continue
		}
		if e.marks[symbol].Sign() <= 0 {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "no_mark", zero))
		}
		imReq := std.PositionInitialRequirement(p, e.marks)
		target := dec.Max(requestedMargin, imReq.Add(imBuffer))
		targets = append(targets, isoTarget{p: p, target: target})
		totalTarget = totalTarget.Add(target)
	}
	if w.Available.Cmp(totalTarget) < 0 {
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "insufficient_free_balance", zero))
	}
	// Isolated health per leg at its target margin. The IM floor is already
	// baked into target; a deep-loss leg needs a larger requestedMargin than
	// the floor implies (one-way only — hedge rejects requestedMargin above).
	for _, t := range targets {
		cand := *t.p
		cand.Mode = perpstate.MarginIsolated
		cand.Margin = t.target
		if _, ok := e.marginHealthSafeLocked(&cand, t.target, removeBuffer); !ok {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "isolated_unsafe_target_margin", zero))
		}
	}
	// Remaining cross pool without this symbol's legs, conservative view
	// (admission-style: reservations excluded, cash moved out).
	remaining := make([]*perpstate.Position, 0)
	for _, cp := range e.crossPositionsLocked(user) {
		if cp.Symbol == symbol {
			continue
		}
		remaining = append(remaining, cp)
	}
	if len(remaining) > 0 {
		h := e.poolRiskLocked().Eval(
			perpstate.Cross(w.Available.Sub(totalTarget), remaining), e.marks)
		if h.Liquidatable() {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "cross_pool_unsafe_after_exit", zero))
		}
	}

	// Apply: lock per-leg targets, flip every leg.
	var legMoves []LegMove
	for _, t := range targets {
		w.Available = w.Available.Sub(t.target)
		t.p.Margin = t.target
		t.p.Mode = perpstate.MarginIsolated
		t.p.Version++
		legMoves = append(legMoves, LegMove{
			PositionIdx: t.p.PositionIdx, Moved: t.target,
			MarginBefore: zero, MarginAfter: t.target, Version: t.p.Version,
		})
		e.syncIndexesLocked(user, symbol, t.p)
	}
	for _, p := range allLegs {
		if p.Mode == perpstate.MarginIsolated {
			continue
		}
		p.Mode = perpstate.MarginIsolated
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
	}
	out := e.opStateSymbolLocked(user, symbol, w, true, "", totalTarget)
	out.LegMoves = legMoves
	return e.cacheOpLocked(opID, out)
}

// marginModeUniform reports whether every existing leg already has mode m
// (vacuously true only when at least one leg exists — callers create the
// active-mode legs first via modeLegsLocked).
func marginModeUniform(legs []*perpstate.Position, m perpstate.MarginMode) bool {
	for _, p := range legs {
		if p.Mode != m {
			return false
		}
	}
	return len(legs) > 0
}

// opStateSymbolLocked aggregates a (user, symbol) outcome across legs: mode /
// leverage / risk_id are uniform per ADR-0077 §7 (read from the first leg),
// MarginAfter is the summed isolated margin, Version is the highest leg
// version. Caller holds e.mu.
func (e *Engine) opStateSymbolLocked(user uint64, symbol string, w *Wallet, accepted bool, reason string, moved dec.Decimal) OpOutcome {
	out := OpOutcome{
		Accepted: accepted, Reason: reason,
		Mode: perpstate.MarginIsolated, PosMode: perpstate.PositionOneWay,
		Leverage: zero, MarginAfter: zero, FreeAfter: w.Available, Moved: moved,
	}
	sp := e.symPeekLocked(user, symbol)
	if sp == nil {
		return out
	}
	out.PosMode = sp.mode
	first := true
	for _, p := range sp.liveLegs(nil) {
		if first {
			out.Mode, out.Leverage, out.RiskID = p.Mode, p.Leverage, p.RiskID
			first = false
		}
		out.MarginAfter = out.MarginAfter.Add(p.Margin)
		if p.Version > out.Version {
			out.Version = p.Version
		}
	}
	return out
}

// CrossCloseEntry is one step of a cross liquidation plan: the position leg
// to force-close, snapshotted under the read lock. Within the owning user's
// sequencer the snapshot stays valid (all mutations of this user's positions
// are serialized there).
type CrossCloseEntry struct {
	Symbol      string
	PositionIdx uint8
	Side        perpstate.Side
	Size        dec.Decimal
	RiskID      uint32
	Version     uint64
}

// CrossClosePlanOf returns the user's cross position legs in forced-close
// order (largest unrealized loss first — ADR-0074 open-question decision v1).
// In hedge mode the two legs of one symbol are independent plan entries
// (ADR-0077 §4: the plan may pick one or both).
func (e *Engine) CrossClosePlanOf(user uint64) []CrossCloseEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()
	plan := perpstate.CrossClosePlan(perpstate.Cross(zero, e.crossPositionsLocked(user)), e.marks)
	out := make([]CrossCloseEntry, 0, len(plan))
	for _, p := range plan {
		out = append(out, CrossCloseEntry{
			Symbol: p.Symbol, PositionIdx: p.PositionIdx, Side: p.Side, Size: p.Size,
			RiskID: p.RiskID, Version: p.Version,
		})
	}
	return out
}

// CrossForceClose closes the user's full cross position leg in
// (symbol, idx) at price (the cross liquidation execution step). Realized PnL
// settles into the free balance, the liquidation fee moves to the symbol's
// insurance fund, and the closed inventory lands on the backstop account
// (ADR-0073 semantics). The caller drives the leg-by-leg plan and finishes
// with CrossSettleDeficit.
func (e *Engine) CrossForceClose(user uint64, symbol string, idx uint8, price dec.Decimal, backstopUser uint64, liqFeeRate dec.Decimal) (res perpstate.FillResult, fee dec.Decimal, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.legPeekLocked(user, symbol, idx)
	if p == nil || p.IsFlat() || p.Mode != perpstate.MarginCross {
		return perpstate.FillResult{}, zero, false
	}
	originalSide := p.Side
	closeQty := p.Size
	fee = price.Mul(closeQty).Mul(liqFeeRate)
	// Exact-size close (closeQty == p.Size): cannot flip a net position or a
	// leg, so the shared core is safe for both.
	res, _ = applyFillByIdx(p, perpstate.Fill{Side: p.Side.Opposite(), Price: price, Qty: closeQty, Fee: fee})
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
