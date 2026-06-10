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

// OpOutcome is the cached result of a config / margin operation, keyed by
// client_op_id (ADR-0074: a repeated op returns the first outcome — the same
// contract as the AssetHolder transfer cache). Moved is the cash this op
// transferred between the wallet and position margin (0 for pure config).
type OpOutcome struct {
	Accepted    bool
	Reason      string
	Mode        perpstate.MarginMode
	Leverage    dec.Decimal
	RiskID      uint32
	MarginAfter dec.Decimal
	FreeAfter   dec.Decimal
	Moved       dec.Decimal
	Version     uint64
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
	return OpOutcome{
		Accepted: accepted, Reason: reason,
		Mode: p.Mode, Leverage: p.Leverage, RiskID: p.RiskID,
		MarginAfter: p.Margin, FreeAfter: w.Available, Moved: moved, Version: p.Version,
	}
}

// AdjustIsolatedMargin moves cash between the wallet free balance and an
// isolated position's margin (ADR-0074 §6). delta > 0 adds, delta < 0
// removes. Removal must keep the position above its initial requirement and
// outside the maintenance band by removeBuffer.
func (e *Engine) AdjustIsolatedMargin(user uint64, symbol, opID string, delta, removeBuffer dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	bySym := e.positions[user]
	var p *perpstate.Position
	if bySym != nil {
		p = bySym[symbol]
	}
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

// SetLeverage updates the position's configured leverage (ADR-0074 §8). For
// a live isolated position the margin is resized to the new initial
// requirement: lowering leverage draws the difference from the free balance,
// raising leverage releases the surplus when the §6 safety line still holds.
// For a live cross position only the derived initial requirement changes —
// the new requirement must still be covered by pool equity.
func (e *Engine) SetLeverage(user uint64, symbol, opID string, lev, removeBuffer dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	p := e.positionLocked(user, symbol)
	if lev.Sign() <= 0 {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "invalid_leverage", zero))
	}
	if maxLev := e.effectiveMaxLeverageLocked(user, symbol, e.notionalForCapLocked(p), p.RiskID); maxLev.Sign() > 0 && lev.Cmp(maxLev) > 0 {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "leverage_exceeds_max", zero))
	}
	if p.IsFlat() {
		p.Leverage = lev
		p.Version++
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", zero))
	}
	if p.Mode == perpstate.MarginCross {
		cand := *p
		cand.Leverage = lev
		h := e.crossCandidateHealthLocked(user, &cand, w.Available)
		if h.Liquidatable() || !h.MeetsInitial(zero) {
			return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "insufficient_margin", zero))
		}
		p.Leverage = lev
		p.Version++
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", zero))
	}
	// Isolated resize: target margin is the entry-based initial requirement
	// at the new leverage (same formula the fills committed).
	targetIM := p.Entry.Mul(p.Size).Div(lev)
	moved := zero
	switch targetIM.Cmp(p.Margin) {
	case 1:
		need := targetIM.Sub(p.Margin)
		if w.Available.Cmp(need) < 0 {
			return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "insufficient_free_balance", zero))
		}
		w.Available = w.Available.Sub(need)
		moved = need
	case -1:
		// The IM floor of this op IS targetIM (the new leverage's entry-based
		// requirement); only the health line needs checking.
		if reason, ok := e.marginHealthSafeLocked(p, targetIM, removeBuffer); !ok {
			return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, reason, zero))
		}
		release := p.Margin.Sub(targetIM)
		w.Available = w.Available.Add(release)
		moved = release
	}
	p.Margin = targetIM
	p.Leverage = lev
	p.Version++
	e.syncIndexesLocked(user, symbol, p)
	return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", moved))
}

// SetRiskID selects the position's risk-limit tier (ADR-0074 §9).
// extraNotional is the service-computed open-order notional that must also
// fit under the selected tier's cap.
func (e *Engine) SetRiskID(user uint64, symbol, opID string, riskID uint32, extraNotional dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	p := e.positionLocked(user, symbol)
	activeModel, hasModel := e.riskModelForLocked(symbol)
	if riskID != 0 {
		if !hasModel || int(riskID) > activeModel.TierCount() {
			return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "invalid_risk_id", zero))
		}
	}
	if hasModel {
		if maxN := activeModel.MaxNotionalFor(riskID); maxN.Sign() > 0 {
			total := e.notionalForCapLocked(p).Add(extraNotional)
			if total.Cmp(maxN) > 0 {
				return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "notional_exceeds_tier_cap", zero))
			}
		}
	}
	if !p.IsFlat() {
		// A higher tier means a more conservative MMR — the position must not
		// fall straight into liquidation on the new requirement.
		cand := *p
		cand.RiskID = riskID
		if cand.Mode == perpstate.MarginCross {
			h := e.crossCandidateHealthLocked(user, &cand, w.Available)
			if h.Liquidatable() {
				return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "unsafe_risk_id", zero))
			}
		} else if hasModel && activeModel.HasMMR() {
			marks := map[string]dec.Decimal{symbol: e.marks[symbol]}
			if perpstate.Isolated(&cand).Liquidatable(marks, activeModel.EffectiveMMRFunc(riskID)) {
				return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "unsafe_risk_id", zero))
			}
		}
	}
	p.RiskID = riskID
	p.Version++
	e.syncIndexesLocked(user, symbol, p) // liq price shifts with the new MMR
	return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", zero))
}

// SetAutoAdd toggles ADR-0074 §7 auto-add-margin on the position config.
func (e *Engine) SetAutoAdd(user uint64, symbol, opID string, enabled bool, maxAdd dec.Decimal) OpOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.ops[opID]; ok && opID != "" {
		return prev
	}
	w := e.walletLocked(user)
	p := e.positionLocked(user, symbol)
	if enabled && p.Mode == perpstate.MarginCross {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "auto_add_isolated_only", zero))
	}
	if maxAdd.Sign() < 0 {
		return e.cacheOpLocked(opID, e.opStateLocked(p, w, false, "invalid_max_add", zero))
	}
	p.AutoAddMargin = enabled
	p.AutoAddMax = maxAdd
	p.Version++
	e.syncIndexesLocked(user, symbol, p)
	return e.cacheOpLocked(opID, e.opStateLocked(p, w, true, "", zero))
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

// AutoAddMargin runs one ADR-0074 §7 top-up attempt: when the isolated
// position's health is at or below MMR+triggerBuffer (and the position is
// not already bankrupt), transfer free balance into position margin up to
// the MMR+targetBuffer line, bounded by the free balance, the position's
// AutoAddMax, and maxPerEvent (product cap; 0 = uncapped). Returns the
// amount moved. fired=false means no transfer happened.
func (e *Engine) AutoAddMargin(user uint64, symbol string, triggerBuffer, targetBuffer, maxPerEvent dec.Decimal) (OpOutcome, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	bySym := e.positions[user]
	var p *perpstate.Position
	if bySym != nil {
		p = bySym[symbol]
	}
	if p == nil || p.IsFlat() || p.Mode == perpstate.MarginCross || !p.AutoAddMargin {
		return OpOutcome{}, false
	}
	mark := e.marks[symbol]
	if mark.Sign() <= 0 {
		return OpOutcome{}, false
	}
	model, hasModel := e.riskModelForPositionLocked(p)
	if !hasModel {
		return OpOutcome{}, false
	}
	notional := p.Notional(mark)
	if notional.Sign() == 0 {
		return OpOutcome{}, false
	}
	equity := p.Margin.Add(p.UnrealizedPnL(mark))
	if equity.Sign() <= 0 {
		return OpOutcome{}, false // bankrupt: liquidation, not top-up
	}
	mmr := model.EffectiveMMR(notional, p.RiskID)
	if equity.Div(notional).Cmp(mmr.Add(triggerBuffer)) > 0 {
		return OpOutcome{}, false // healthy
	}
	need := mmr.Add(targetBuffer).Mul(notional).Sub(equity)
	if need.Sign() <= 0 {
		return OpOutcome{}, false
	}
	amount := dec.Min(need, w.Available)
	if p.AutoAddMax.Sign() > 0 {
		amount = dec.Min(amount, p.AutoAddMax)
	}
	if maxPerEvent.Sign() > 0 {
		amount = dec.Min(amount, maxPerEvent)
	}
	if amount.Sign() <= 0 {
		return OpOutcome{}, false // no free cash — proceed to liquidation check
	}
	w.Available = w.Available.Sub(amount)
	p.Margin = p.Margin.Add(amount)
	p.Version++
	e.syncIndexesLocked(user, symbol, p)
	return e.opStateLocked(p, w, true, "", amount), true
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
// (user, symbol) at the candidate notional, using the position's selected
// riskID when a position record exists (ADR-0074 §8/§10).
func (e *Engine) EffectiveMaxLeverage(user uint64, symbol string, notional dec.Decimal) dec.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var riskID uint32
	if bySym := e.positions[user]; bySym != nil {
		if p := bySym[symbol]; p != nil {
			riskID = p.RiskID
		}
	}
	return e.effectiveMaxLeverageLocked(user, symbol, notional, riskID)
}

// notionalForCapLocked values the position for tier-cap / max-leverage
// selection: mark notional when a mark exists, entry notional otherwise.
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

// --- config views -----------------------------------------------------------

// PositionConfigView is the query shape for per-(user, symbol) config
// (ADR-0074 §13). Effective fields are derived at read time.
type PositionConfigView struct {
	Symbol               string
	Mode                 perpstate.MarginMode
	Leverage             dec.Decimal
	RiskID               uint32
	AutoAddMargin        bool
	AutoAddMax           dec.Decimal
	EffectiveMaxLeverage dec.Decimal
	MaxNotional          dec.Decimal
}

// PositionConfigsOf returns config views for the user's position records
// (including flat records — config can pre-exist a position), sorted by
// symbol. symbol "" returns all.
func (e *Engine) PositionConfigsOf(user uint64, symbol string) []PositionConfigView {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bySym := e.positions[user]
	var out []PositionConfigView
	add := func(p *perpstate.Position) {
		v := PositionConfigView{
			Symbol: p.Symbol, Mode: p.Mode, Leverage: p.Leverage, RiskID: p.RiskID,
			AutoAddMargin: p.AutoAddMargin, AutoAddMax: p.AutoAddMax,
			EffectiveMaxLeverage: e.effectiveMaxLeverageLocked(user, p.Symbol, e.notionalForCapLocked(p), p.RiskID),
			MaxNotional:          zero,
		}
		if m, ok := e.riskModelForLocked(p.Symbol); ok {
			v.MaxNotional = m.MaxNotionalFor(p.RiskID)
		}
		out = append(out, v)
	}
	if symbol != "" {
		if p := bySym[symbol]; p != nil {
			add(p)
		}
		return out
	}
	for _, p := range bySym {
		add(p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}
