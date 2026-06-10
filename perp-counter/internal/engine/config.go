package engine

// config.go holds the ADR-0074 P0 account/position config primitives. Every
// mutation here is one atomic critical section under e.mu: idempotency-cache
// check, validation against current state (health, caps), and the state
// write happen with no interleaving window. The service layer adds the
// checks that need service-owned state (active orders, in-flight
// liquidations) and runs everything inside the user's sequencer.

import (
	"sort"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// LegMove is one leg's cash movement inside a multi-leg config op (margin
// mode switch / leverage resize touch both hedge legs atomically, ADR-0077
// §7). The service journals one PerpMarginAdjustmentEvent per entry so money
// movement stays attributable to a single (user, symbol, position_idx).
type LegMove struct {
	PositionIdx  uint8
	Moved        dec.Decimal
	MarginBefore dec.Decimal
	MarginAfter  dec.Decimal
	Version      uint64
}

// OpOutcome is the cached result of a config / margin operation, keyed by
// client_op_id (ADR-0074: a repeated op returns the first outcome — the same
// contract as the AssetHolder transfer cache). Moved is the total cash this
// op transferred between the wallet and position margin (0 for pure config);
// LegMoves breaks it down per leg when the op touched more than one.
type OpOutcome struct {
	Accepted    bool
	Reason      string
	Mode        perpstate.MarginMode
	PosMode     perpstate.PositionMode
	Leverage    dec.Decimal
	RiskID      uint32
	MarginAfter dec.Decimal
	FreeAfter   dec.Decimal
	Moved       dec.Decimal
	Version     uint64
	LegMoves    []LegMove
}

// CachedOp returns the stored outcome for a client_op_id.
func (e *Engine) CachedOp(opID string) (OpOutcome, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out, ok := e.ops[opID]
	return out, ok
}

// cacheOpLocked stores the outcome (opID "" disables caching) and returns it.
func (e *Engine) cacheOpLocked(opID string, out OpOutcome) OpOutcome {
	if opID != "" {
		e.ops[opID] = out
	}
	return out
}

func (e *Engine) opStateLocked(p *perpstate.Position, w *Wallet, accepted bool, reason string, moved dec.Decimal) OpOutcome {
	posMode := perpstate.PositionOneWay
	if sp := e.symPeekLocked(p.UserID, p.Symbol); sp != nil {
		posMode = sp.mode
	}
	return OpOutcome{
		Accepted: accepted, Reason: reason,
		Mode: p.Mode, PosMode: posMode, Leverage: p.Leverage, RiskID: p.RiskID,
		MarginAfter: p.Margin, FreeAfter: w.Available, Moved: moved, Version: p.Version,
	}
}

// AdjustIsolatedMargin moves cash between the wallet free balance and one
// isolated leg's margin (ADR-0074 §6; per-leg in hedge mode — the only
// position config op keyed by position_idx, ADR-0077 §7). delta > 0 adds,
// delta < 0 removes. Removal must keep the leg above its initial requirement
// and outside the maintenance band by removeBuffer.
func (e *Engine) AdjustIsolatedMargin(user uint64, symbol string, idx uint8, opID string, delta, removeBuffer dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	p := e.legPeekLocked(user, symbol, idx)
	if p == nil || p.IsFlat() {
		return e.cacheOpLocked(opID, OpOutcome{Accepted: false, Reason: "no_position", FreeAfter: w.Available,
			MarginAfter: zero, Mode: perpstate.MarginIsolated})
	}
	if p.Mode == perpstate.MarginCross {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "not_isolated", zero))
	}
	switch delta.Sign() {
	case 0:
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "zero_delta", zero))
	case 1:
		if w.Available.Cmp(delta) < 0 {
			return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "insufficient_free_balance", zero))
		}
		w.Available = w.Available.Sub(delta)
		p.Margin = p.Margin.Add(delta)
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", delta))
	default:
		amount := delta.Neg()
		if amount.Cmp(p.Margin) > 0 {
			return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "exceeds_position_margin", zero))
		}
		if reason, ok := e.isolatedMarginSafeLocked(p, p.Margin.Sub(amount), removeBuffer); !ok {
			return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, reason, zero))
		}
		p.Margin = p.Margin.Sub(amount)
		w.Available = w.Available.Add(amount)
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", amount))
	}
}

// isolatedMarginSafeLocked checks ADR-0074 §6's removal safety line for a
// hypothetical margin level: margin' >= initial requirement (entry-based at
// the position's CURRENT leverage, matching the IM committed at fill) and
// the health line below. Caller holds e.mu.
func (e *Engine) isolatedMarginSafeLocked(p *perpstate.Position, newMargin, buffer dec.Decimal) (string, bool) {
	if p.Leverage.Sign() > 0 {
		imReq := p.Entry.Mul(p.Size).Div(p.Leverage)
		if newMargin.Cmp(imReq) < 0 {
			return "below_initial_requirement", false
		}
	}
	return e.marginHealthSafeLocked(p, newMargin, buffer)
}

// marginHealthSafeLocked checks only the health half of the safety line:
// equity at newMargin stays clear of the maintenance requirement by buffer
// at the current mark. Used directly by paths whose IM floor is defined by
// the operation itself (leverage resize, mode-switch target). Caller holds
// e.mu.
func (e *Engine) marginHealthSafeLocked(p *perpstate.Position, newMargin, buffer dec.Decimal) (string, bool) {
	mark := e.marks[p.Symbol]
	if mark.Sign() <= 0 {
		// No mark yet → no health view. Refuse to free collateral blind.
		return "no_mark", false
	}
	notional := p.Notional(mark)
	if notional.Sign() == 0 {
		return "", true
	}
	equity := newMargin.Add(p.UnrealizedPnL(mark))
	mmr := zero
	if m, ok := e.riskModelForPositionLocked(p); ok {
		mmr = m.EffectiveMMR(notional, p.RiskID)
	}
	if equity.Cmp(mmr.Add(buffer).Mul(notional)) <= 0 {
		return "unsafe_after_removal", false
	}
	return "", true
}

// SetLeverage updates the symbol's configured leverage (ADR-0074 §8;
// per-(user, symbol), uniform across hedge legs — ADR-0077 §7). For each live
// isolated leg the margin is resized to the new initial requirement: lowering
// leverage draws the difference from the free balance, raising leverage
// releases the surplus when the §6 safety line still holds. For live cross
// legs only the derived initial requirement changes — the new requirement
// must still be covered by pool equity. All legs validate first; nothing
// moves on any rejection.
func (e *Engine) SetLeverage(user uint64, symbol, opID string, lev, removeBuffer dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	legs := e.modeLegsLocked(user, symbol)
	if lev.Sign() <= 0 {
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "invalid_leverage", zero))
	}
	if maxLev := e.effectiveMaxLeverageLocked(user, symbol, e.symbolNotionalForCapLocked(user, symbol), e.riskIDOfLocked(user, symbol)); maxLev.Sign() > 0 && lev.Cmp(maxLev) > 0 {
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "leverage_exceeds_max", zero))
	}

	// Plan phase: collect cross candidates and isolated resize targets; all
	// checks pass before anything mutates (all-or-nothing across legs).
	type isoResize struct {
		p        *perpstate.Position
		targetIM dec.Decimal
	}
	var crossCands []*perpstate.Position
	var resizes []isoResize
	totalNeed := zero
	for _, p := range legs {
		if p.IsFlat() {
			continue
		}
		if p.Mode == perpstate.MarginCross {
			cand := *p
			cand.Leverage = lev
			crossCands = append(crossCands, &cand)
			continue
		}
		// Isolated resize: target margin is the entry-based initial
		// requirement at the new leverage (same formula the fills committed).
		targetIM := p.Entry.Mul(p.Size).Div(lev)
		switch targetIM.Cmp(p.Margin) {
		case 1:
			// Conservative funding check: releases from sibling legs are not
			// netted against needs — both legs resize in the same direction
			// under uniform leverage unless margins were manually adjusted.
			totalNeed = totalNeed.Add(targetIM.Sub(p.Margin))
		case -1:
			// The IM floor of this op IS targetIM (the new leverage's
			// entry-based requirement); only the health line needs checking.
			if reason, ok := e.marginHealthSafeLocked(p, targetIM, removeBuffer); !ok {
				return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, reason, zero))
			}
		}
		resizes = append(resizes, isoResize{p: p, targetIM: targetIM})
	}
	if len(crossCands) > 0 {
		h := e.crossCandidatesHealthLocked(user, crossCands, w.Available)
		if h.Liquidatable() || !h.MeetsInitial(zero) {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "insufficient_margin", zero))
		}
	}
	if w.Available.Cmp(totalNeed) < 0 {
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "insufficient_free_balance", zero))
	}

	// Apply phase.
	moved := zero
	var legMoves []LegMove
	for _, r := range resizes {
		before := r.p.Margin
		delta := r.targetIM.Sub(r.p.Margin)
		if delta.Sign() != 0 {
			w.Available = w.Available.Sub(delta) // negative delta credits back
			moved = moved.Add(delta.Abs())
		}
		r.p.Margin = r.targetIM
		r.p.Leverage = lev
		r.p.Version++
		legMoves = append(legMoves, LegMove{
			PositionIdx: r.p.PositionIdx, Moved: delta.Abs(),
			MarginBefore: before, MarginAfter: r.p.Margin, Version: r.p.Version,
		})
		e.syncIndexesLocked(user, symbol, r.p)
	}
	for _, p := range legs {
		if p.Leverage.Cmp(lev) == 0 {
			continue
		}
		p.Leverage = lev
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
	}
	out := e.opStateSymbolLocked(user, symbol, w, true, "", moved)
	out.LegMoves = legMoves
	return e.cacheOpLocked(opID, out)
}

// SetRiskID selects the symbol's risk-limit tier (ADR-0074 §9;
// per-(user, symbol), uniform across hedge legs — ADR-0077 §7: the tier cap
// is checked against the GROSS sum of leg notionals). extraNotional is the
// service-computed open-order notional that must also fit under the selected
// tier's cap.
func (e *Engine) SetRiskID(user uint64, symbol, opID string, riskID uint32, extraNotional dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	legs := e.modeLegsLocked(user, symbol)
	activeModel, hasModel := e.riskModelForLocked(symbol)
	if riskID != 0 {
		if !hasModel || int(riskID) > activeModel.TierCount() {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "invalid_risk_id", zero))
		}
	}
	if hasModel {
		if maxN := activeModel.MaxNotionalFor(riskID); maxN.Sign() > 0 {
			total := e.symbolNotionalForCapLocked(user, symbol).Add(extraNotional)
			if total.Cmp(maxN) > 0 {
				return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "notional_exceeds_tier_cap", zero))
			}
		}
	}
	// A higher tier means a more conservative MMR — no live leg may fall
	// straight into liquidation on the new requirement (all-or-nothing).
	var crossCands []*perpstate.Position
	for _, p := range legs {
		if p.IsFlat() {
			continue
		}
		cand := *p
		cand.RiskID = riskID
		if cand.Mode == perpstate.MarginCross {
			c := cand
			crossCands = append(crossCands, &c)
		} else if hasModel && activeModel.HasMMR() {
			marks := map[string]dec.Decimal{symbol: e.marks[symbol]}
			if perpstate.Isolated(&cand).Liquidatable(marks, activeModel.EffectiveMMRFunc(riskID)) {
				return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "unsafe_risk_id", zero))
			}
		}
	}
	if len(crossCands) > 0 {
		h := e.crossCandidatesHealthLocked(user, crossCands, w.Available)
		if h.Liquidatable() {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "unsafe_risk_id", zero))
		}
	}
	for _, p := range legs {
		p.RiskID = riskID
		p.Version++
		e.syncIndexesLocked(user, symbol, p) // liq price shifts with the new MMR
	}
	return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, true, "", zero))
}

// SetAutoAdd toggles ADR-0074 §7 auto-add-margin on the symbol's config
// (uniform across hedge legs — ADR-0077 §7; the top-up itself runs per leg).
func (e *Engine) SetAutoAdd(user uint64, symbol, opID string, enabled bool, maxAdd dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	legs := e.modeLegsLocked(user, symbol)
	if enabled && legs[0].Mode == perpstate.MarginCross {
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "auto_add_isolated_only", zero))
	}
	if maxAdd.Sign() < 0 {
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "invalid_max_add", zero))
	}
	for _, p := range legs {
		p.AutoAddMargin = enabled
		p.AutoAddMax = maxAdd
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
	}
	return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, true, "", zero))
}

// SetPositionMode is the ADR-0077 §3 engine primitive: the flat-only
// validation over ALL legs and the mode write happen in one critical section.
// Service-owned checks (active orders, position-bound triggers, in-flight
// liquidations) run before this inside the user's sequencer; this re-check
// makes the engine state transition self-defending regardless.
func (e *Engine) SetPositionMode(user uint64, symbol, opID string, target perpstate.PositionMode) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	sp := e.symLocked(user, symbol)
	if sp.mode == target {
		return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, true, "", zero)) // idempotent no-op
	}
	for _, p := range sp.legs {
		if p != nil && !p.IsFlat() {
			return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, false, "position_not_flat", zero))
		}
	}
	sp.mode = target
	// Materialize the new mode's legs (flat, config-inheriting): the journal
	// echo and QueryPositionConfig need a carrier row even before the first
	// trade, or a mode switch on a never-traded symbol would be invisible to
	// replay.
	e.modeLegsLocked(user, symbol)
	return e.cacheOpLocked(opID, e.opStateSymbolLocked(user, symbol, w, true, "", zero))
}

// AutoAddUsersWith returns users with an auto-add-enabled live isolated
// position in symbol, sorted (the mark-tick pre-liquidation pass).
func (e *Engine) AutoAddUsersWith(symbol string) []uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	byUser := e.autoAdd[symbol]
	out := make([]uint64, 0, len(byUser))
	for u := range byUser {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// AutoAddResult is one leg's ADR-0074 §7 top-up outcome (per leg, ADR-0077
// §7: the toggle is symbol-uniform but each leg's health line fires its own
// transfer).
type AutoAddResult struct {
	PositionIdx  uint8
	MarginBefore dec.Decimal
	Out          OpOutcome
}

// AutoAddMargin runs one ADR-0074 §7 top-up attempt per live leg: when an
// isolated leg's health is at or below MMR+triggerBuffer (and the leg is not
// already bankrupt), transfer free balance into the leg margin up to the
// MMR+targetBuffer line, bounded by the free balance, the leg's AutoAddMax,
// and maxPerEvent (product cap; 0 = uncapped). Legs are evaluated in idx
// order under one lock — a transfer to the first leg reduces the free balance
// the next leg sees. Returns one entry per fired transfer.
func (e *Engine) AutoAddMargin(user uint64, symbol string, triggerBuffer, targetBuffer, maxPerEvent dec.Decimal) []AutoAddResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	var out []AutoAddResult
	for _, p := range e.legsLocked(user, symbol) {
		if p.IsFlat() || p.Mode == perpstate.MarginCross || !p.AutoAddMargin {
			continue
		}
		mark := e.marks[symbol]
		if mark.Sign() <= 0 {
			continue
		}
		model, hasModel := e.riskModelForPositionLocked(p)
		if !hasModel {
			continue
		}
		notional := p.Notional(mark)
		if notional.Sign() == 0 {
			continue
		}
		equity := p.Margin.Add(p.UnrealizedPnL(mark))
		if equity.Sign() <= 0 {
			continue // bankrupt: liquidation, not top-up
		}
		mmr := model.EffectiveMMR(notional, p.RiskID)
		if equity.Div(notional).Cmp(mmr.Add(triggerBuffer)) > 0 {
			continue // healthy
		}
		need := mmr.Add(targetBuffer).Mul(notional).Sub(equity)
		if need.Sign() <= 0 {
			continue
		}
		amount := dec.Min(need, w.Available)
		if p.AutoAddMax.Sign() > 0 {
			amount = dec.Min(amount, p.AutoAddMax)
		}
		if maxPerEvent.Sign() > 0 {
			amount = dec.Min(amount, maxPerEvent)
		}
		if amount.Sign() <= 0 {
			continue // no free cash — proceed to liquidation check
		}
		before := p.Margin
		w.Available = w.Available.Sub(amount)
		p.Margin = p.Margin.Add(amount)
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
		out = append(out, AutoAddResult{
			PositionIdx: p.PositionIdx, MarginBefore: before,
			Out: e.opStateLocked(p, w, true, "", amount),
		})
	}
	return out
}

// --- customer leverage caps (ADR-0074 §10) ---------------------------------

// SetCustomerLeverageLimit upserts an admin leverage cap. maxLev <= 0 removes
// the row (cap cleared).
func (e *Engine) SetCustomerLeverageLimit(user uint64, symbol string, maxLev dec.Decimal, reason, updatedBy string, nowMs int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bySym := e.levLimits[user]
	if maxLev.Sign() <= 0 {
		if bySym != nil {
			delete(bySym, symbol)
			if len(bySym) == 0 {
				delete(e.levLimits, user)
			}
		}
		return
	}
	if bySym == nil {
		bySym = map[string]CustomerLimit{}
		e.levLimits[user] = bySym
	}
	bySym[symbol] = CustomerLimit{MaxLeverage: maxLev, Reason: reason, UpdatedBy: updatedBy, UpdatedMs: nowMs}
}

// CustomerLimitRow is one cap row with its scope, for list/audit APIs.
type CustomerLimitRow struct {
	UserID uint64
	Symbol string // "" = user-global
	CustomerLimit
}

// CustomerLeverageLimits lists cap rows. user 0 = all users. Sorted by
// (user, symbol) for stable output.
func (e *Engine) CustomerLeverageLimits(user uint64) []CustomerLimitRow {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if user != 0 {
		return sortLimitRows(e.limitRowsForLocked(user, nil))
	}
	return sortLimitRows(e.customerLeverageLimitsLocked())
}

// customerLeverageLimitsLocked collects every cap row. Caller holds e.mu
// (any) — also used by Snapshot, which already holds the read lock.
func (e *Engine) customerLeverageLimitsLocked() []CustomerLimitRow {
	var out []CustomerLimitRow
	for u := range e.levLimits {
		out = e.limitRowsForLocked(u, out)
	}
	return sortLimitRows(out)
}

func (e *Engine) limitRowsForLocked(user uint64, out []CustomerLimitRow) []CustomerLimitRow {
	for sym, lim := range e.levLimits[user] {
		out = append(out, CustomerLimitRow{UserID: user, Symbol: sym, CustomerLimit: lim})
	}
	return out
}

func sortLimitRows(out []CustomerLimitRow) []CustomerLimitRow {
	sort.Slice(out, func(i, j int) bool {
		if out[i].UserID != out[j].UserID {
			return out[i].UserID < out[j].UserID
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out
}

func (e *Engine) customerMaxLeverageLocked(user uint64, symbol string) dec.Decimal {
	bySym := e.levLimits[user]
	if bySym == nil {
		return zero
	}
	if lim, ok := bySym[symbol]; ok {
		return lim.MaxLeverage
	}
	if lim, ok := bySym[""]; ok {
		return lim.MaxLeverage
	}
	return zero
}

// effectiveMaxLeverageLocked is the ADR-0074 §10 min-chain: the effective
// tier's cap (which already falls back to the product default) intersected
// with the customer cap. Zero = uncapped. Caller holds e.mu (any).
func (e *Engine) effectiveMaxLeverageLocked(user uint64, symbol string, notional dec.Decimal, riskID uint32) dec.Decimal {
	tierCap := zero
	if m, ok := e.riskModelForLocked(symbol); ok {
		tierCap = m.EffectiveMaxLeverage(notional, riskID)
	}
	custCap := e.customerMaxLeverageLocked(user, symbol)
	switch {
	case tierCap.Sign() <= 0:
		return custCap
	case custCap.Sign() <= 0:
		return tierCap
	default:
		return dec.Min(tierCap, custCap)
	}
}

// EffectiveMaxLeverage resolves the order-admission leverage cap for
// (user, symbol) at the candidate notional, using the symbol's selected
// riskID when any leg record exists (ADR-0074 §8/§10; risk_id is uniform
// across legs per ADR-0077 §7).
func (e *Engine) EffectiveMaxLeverage(user uint64, symbol string, notional dec.Decimal) dec.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.effectiveMaxLeverageLocked(user, symbol, notional, e.riskIDOfLocked(user, symbol))
}

// riskIDOfLocked reads the symbol's uniform risk_id from the first existing
// leg (0 = auto when no record exists). Caller holds e.mu (any).
func (e *Engine) riskIDOfLocked(user uint64, symbol string) uint32 {
	for _, p := range e.legsLocked(user, symbol) {
		return p.RiskID
	}
	return 0
}

// SymbolOrderConfig is the (user, symbol)-uniform admission config snapshot
// PlaceOrder reads in one locked step: margin mode / leverage / risk_id are
// uniform across legs (ADR-0077 §7) and PosMode drives the §2 intent matrix.
type SymbolOrderConfig struct {
	MarginMode perpstate.MarginMode
	Leverage   dec.Decimal
	RiskID     uint32
	PosMode    perpstate.PositionMode
}

// SymbolOrderConfigOf reads the symbol-uniform config from the first existing
// leg (defaults: isolated, zero leverage, auto risk tier, ONE_WAY).
func (e *Engine) SymbolOrderConfigOf(user uint64, symbol string) SymbolOrderConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := SymbolOrderConfig{MarginMode: perpstate.MarginIsolated, Leverage: zero}
	if sp := e.symPeekLocked(user, symbol); sp != nil {
		out.PosMode = sp.mode
		for _, p := range sp.liveLegs(nil) {
			out.MarginMode, out.Leverage, out.RiskID = p.Mode, p.Leverage, p.RiskID
			break
		}
	}
	return out
}

// SymbolNotionalForCap is the public read of the symbol's GROSS leg notional
// sum (tier-cap admission input, ADR-0077 §7).
func (e *Engine) SymbolNotionalForCap(user uint64, symbol string) dec.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.symbolNotionalForCapLocked(user, symbol)
}

// notionalForCapLocked values one leg for tier-cap / max-leverage selection:
// mark notional when a mark exists, entry notional otherwise.
func (e *Engine) notionalForCapLocked(p *perpstate.Position) dec.Decimal {
	if p.IsFlat() {
		return zero
	}
	mark := e.marks[p.Symbol]
	if mark.Sign() <= 0 {
		mark = p.Entry
	}
	return p.Notional(mark)
}

// symbolNotionalForCapLocked sums the GROSS leg notionals of (user, symbol)
// for tier-cap checks (ADR-0077 §7: hedge legs do not net). Caller holds
// e.mu (any).
func (e *Engine) symbolNotionalForCapLocked(user uint64, symbol string) dec.Decimal {
	total := zero
	for _, p := range e.legsLocked(user, symbol) {
		total = total.Add(e.notionalForCapLocked(p))
	}
	return total
}

// --- config views -----------------------------------------------------------

// PositionConfigView is the query shape for per-(user, symbol, idx) config
// (ADR-0074 §13 + ADR-0077). Effective fields are derived at read time;
// PosMode is the symbol's position mode (shared by its legs).
type PositionConfigView struct {
	Symbol               string
	PositionIdx          uint8
	PosMode              perpstate.PositionMode
	Mode                 perpstate.MarginMode
	Leverage             dec.Decimal
	RiskID               uint32
	AutoAddMargin        bool
	AutoAddMax           dec.Decimal
	EffectiveMaxLeverage dec.Decimal
	MaxNotional          dec.Decimal
}

// PositionConfigsOf returns config views for the user's leg records
// (including flat records — config can pre-exist a position), one row per
// existing leg, sorted by (symbol, idx). symbol "" returns all.
func (e *Engine) PositionConfigsOf(user uint64, symbol string) []PositionConfigView {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bySym := e.positions[user]
	var out []PositionConfigView
	add := func(sp *symbolPositions, p *perpstate.Position) {
		v := PositionConfigView{
			Symbol: p.Symbol, PositionIdx: p.PositionIdx, PosMode: sp.mode,
			Mode: p.Mode, Leverage: p.Leverage, RiskID: p.RiskID,
			AutoAddMargin: p.AutoAddMargin, AutoAddMax: p.AutoAddMax,
			EffectiveMaxLeverage: e.effectiveMaxLeverageLocked(user, p.Symbol, e.notionalForCapLocked(p), p.RiskID),
			MaxNotional:          zero,
		}
		if m, ok := e.riskModelForLocked(p.Symbol); ok {
			v.MaxNotional = m.MaxNotionalFor(p.RiskID)
		}
		out = append(out, v)
	}
	addSym := func(sp *symbolPositions) {
		for _, p := range sp.liveLegs(nil) {
			add(sp, p)
		}
	}
	if symbol != "" {
		if sp := bySym[symbol]; sp != nil {
			addSym(sp)
		}
		return out
	}
	for _, sp := range bySym {
		addSym(sp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].PositionIdx < out[j].PositionIdx
	})
	return out
}
