package perpstate

import (
	"sort"

	"github.com/xargin/opentrade/pkg/dec"
)

// MMRFunc resolves the maintenance margin ratio from the pool notional that is
// being evaluated. Passing the resolver through CollateralPool keeps ADR-0068's
// margin-mode boundary intact: isolated and future cross pools can share the
// same liquidation trigger while sourcing their risk tier from different
// notionals.
type MMRFunc func(notional dec.Decimal) dec.Decimal

// ConstantMMR adapts the former single-rate configuration into the tier-aware
// API. Keeping this as the degenerate implementation lets old deployments opt
// into ADR-0070 incrementally, before SymbolConfig starts serving full tier
// tables.
func ConstantMMR(mmr dec.Decimal) MMRFunc {
	return func(dec.Decimal) dec.Decimal { return mmr }
}

// RiskTier is one notional bucket in the ADR-0070 risk limit table. Tiers are
// selected by the first TierMaxNotional >= current notional; a zero cap is
// treated as an open-ended final tier so tests and local configs can stay small.
type RiskTier struct {
	TierMaxNotional  dec.Decimal
	MaintMarginRatio dec.Decimal
	MaxLeverage      dec.Decimal
	LiqFeeRate       dec.Decimal
}

// RiskModel is the in-memory form of the per-symbol risk configuration. The
// service owns symbol lookup; this type owns the deterministic tier selection
// rules so all callers make the same boundary decision.
type RiskModel struct {
	tiers             []RiskTier
	defaultMMR        dec.Decimal
	defaultMaxLev     dec.Decimal
	defaultLiqFeeRate dec.Decimal
}

// NewRiskModel returns a sorted, copy-on-write risk model. Invalid empty tier
// fields are deliberately not rejected here: the service can still run with a
// partial config by falling back to the legacy scalar settings, which is safer
// for local development while the ADR-0056 admin plane is not wired yet.
func NewRiskModel(tiers []RiskTier, defaultMMR, defaultMaxLev, defaultLiqFeeRate dec.Decimal) RiskModel {
	cp := append([]RiskTier(nil), tiers...)
	sort.SliceStable(cp, func(i, j int) bool {
		// Open-ended tiers sort last; otherwise caps are ascending.
		if cp[i].TierMaxNotional.Sign() == 0 {
			return false
		}
		if cp[j].TierMaxNotional.Sign() == 0 {
			return true
		}
		return cp[i].TierMaxNotional.Cmp(cp[j].TierMaxNotional) < 0
	})
	return RiskModel{
		tiers:             cp,
		defaultMMR:        defaultMMR,
		defaultMaxLev:     defaultMaxLev,
		defaultLiqFeeRate: defaultLiqFeeRate,
	}
}

// HasMMR reports whether liquidation should be enabled for this model.
func (m RiskModel) HasMMR() bool { return m.MMR(zero).Sign() > 0 }

// MMR resolves the maintenance margin rate for notional.
func (m RiskModel) MMR(notional dec.Decimal) dec.Decimal {
	if t, ok := m.tier(notional); ok && t.MaintMarginRatio.Sign() > 0 {
		return t.MaintMarginRatio
	}
	return m.defaultMMR
}

// MMRFunc exposes the model as the CollateralPool-compatible resolver.
func (m RiskModel) MMRFunc() MMRFunc { return m.MMR }

// MaxLeverage resolves the maximum allowed leverage for notional. A zero return
// value means "uncapped", matching the legacy Config.MaxLeverage behavior.
func (m RiskModel) MaxLeverage(notional dec.Decimal) dec.Decimal {
	if t, ok := m.tier(notional); ok && t.MaxLeverage.Sign() > 0 {
		return t.MaxLeverage
	}
	return m.defaultMaxLev
}

// LiqFeeRate resolves the liquidation fee rate for notional. The fallback is
// kept separate from MMR because early ADR-0070 deployments may enable partial
// liquidation before charging liquidation fees.
func (m RiskModel) LiqFeeRate(notional dec.Decimal) dec.Decimal {
	if t, ok := m.tier(notional); ok && t.LiqFeeRate.Sign() > 0 {
		return t.LiqFeeRate
	}
	return m.defaultLiqFeeRate
}

// TierIndex returns the selected tier's 1-based index for journaling. Zero means
// the scalar fallback was used.
func (m RiskModel) TierIndex(notional dec.Decimal) int32 {
	for i, t := range m.tiers {
		if inTier(notional, t) {
			return int32(i + 1)
		}
	}
	return 0
}

// TierCount reports how many tiers are configured (0 = scalar fallback only).
func (m RiskModel) TierCount() int { return len(m.tiers) }

// TierAt returns the 1-based tier. ok=false when idx is out of range.
func (m RiskModel) TierAt(idx uint32) (RiskTier, bool) {
	if idx == 0 || int(idx) > len(m.tiers) {
		return RiskTier{}, false
	}
	return m.tiers[idx-1], true
}

// EffectiveTierIndex resolves ADR-0074 §9's conservative tier rule:
// effective_tier = max(auto tier by notional, user-selected riskID). A riskID
// beyond the table clamps to the last tier; riskID 0 means auto. Zero return
// means no tier matched (scalar fallback).
func (m RiskModel) EffectiveTierIndex(notional dec.Decimal, riskID uint32) int32 {
	idx := m.TierIndex(notional)
	if riskID == 0 || len(m.tiers) == 0 {
		return idx
	}
	sel := int32(riskID)
	if int(sel) > len(m.tiers) {
		sel = int32(len(m.tiers))
	}
	if sel > idx {
		return sel
	}
	return idx
}

// EffectiveMMR resolves the maintenance margin rate at the effective tier
// (ADR-0074 §9: selecting a higher riskID buys more allowed notional at the
// cost of a more conservative MMR).
func (m RiskModel) EffectiveMMR(notional dec.Decimal, riskID uint32) dec.Decimal {
	if t, ok := m.TierAt(uint32(m.EffectiveTierIndex(notional, riskID))); ok && t.MaintMarginRatio.Sign() > 0 {
		return t.MaintMarginRatio
	}
	return m.defaultMMR
}

// EffectiveMMRFunc adapts EffectiveMMR for one position's riskID into the
// CollateralPool / LiqPrice resolver shape.
func (m RiskModel) EffectiveMMRFunc(riskID uint32) MMRFunc {
	return func(notional dec.Decimal) dec.Decimal { return m.EffectiveMMR(notional, riskID) }
}

// EffectiveMaxLeverage resolves the leverage cap at the effective tier. Zero
// means uncapped (legacy behavior).
func (m RiskModel) EffectiveMaxLeverage(notional dec.Decimal, riskID uint32) dec.Decimal {
	if t, ok := m.TierAt(uint32(m.EffectiveTierIndex(notional, riskID))); ok && t.MaxLeverage.Sign() > 0 {
		return t.MaxLeverage
	}
	return m.defaultMaxLev
}

// EffectiveLiqFeeRate resolves the liquidation fee rate at the effective tier.
func (m RiskModel) EffectiveLiqFeeRate(notional dec.Decimal, riskID uint32) dec.Decimal {
	if t, ok := m.TierAt(uint32(m.EffectiveTierIndex(notional, riskID))); ok && t.LiqFeeRate.Sign() > 0 {
		return t.LiqFeeRate
	}
	return m.defaultLiqFeeRate
}

// MaxNotionalFor is the admission cap implied by a riskID selection
// (ADR-0074 §9: orders may not push notional past the selected tier's cap).
// riskID 0 (auto) caps at the last tier's bound. Zero means uncapped — no
// tiers configured, or the governing tier is open-ended.
func (m RiskModel) MaxNotionalFor(riskID uint32) dec.Decimal {
	if len(m.tiers) == 0 {
		return zero
	}
	if riskID == 0 {
		return m.tiers[len(m.tiers)-1].TierMaxNotional
	}
	if int(riskID) > len(m.tiers) {
		riskID = uint32(len(m.tiers))
	}
	return m.tiers[riskID-1].TierMaxNotional
}

func (m RiskModel) tier(notional dec.Decimal) (RiskTier, bool) {
	for _, t := range m.tiers {
		if inTier(notional, t) {
			return t, true
		}
	}
	return RiskTier{}, false
}

func inTier(notional dec.Decimal, t RiskTier) bool {
	return t.TierMaxNotional.Sign() == 0 || notional.Cmp(t.TierMaxNotional) <= 0
}

// ReduceToTarget returns the smallest isolated-position close quantity that
// makes the remaining pool meet MMR(remaining notional)+buffer at the current
// mark. The binary search is intentionally pure and conservative: it assumes the
// forced close happens at mark, so service-level execution at a better liq price
// can only improve the post-fill health.
func ReduceToTarget(cp CollateralPool, marks map[string]dec.Decimal, mmrOf MMRFunc, buffer dec.Decimal) dec.Decimal {
	p := singleActivePosition(cp)
	if p == nil || mmrOf == nil {
		return zero
	}
	mark := marks[p.Symbol]
	if mark.Sign() <= 0 {
		return p.Size
	}
	initial := cp.Eval(marks)
	if initial.Notional.Sign() == 0 {
		return zero
	}
	targetNow := mmrOf(initial.Notional).Add(buffer)
	if initial.MarginRatio.Cmp(targetNow) > 0 {
		return zero
	}
	if initial.Equity.Sign() <= 0 {
		return p.Size
	}

	lo := zero
	hi := p.Size
	for i := 0; i < 80; i++ {
		mid := lo.Add(hi).Div(dec.FromInt(2))
		if reducedHealthSafe(p, mark, mid, mmrOf, buffer) {
			hi = mid
		} else {
			lo = mid
		}
	}
	return hi
}

func singleActivePosition(cp CollateralPool) *Position {
	var out *Position
	for _, p := range cp.Positions {
		if p == nil || p.IsFlat() {
			continue
		}
		if out != nil {
			// Cross-margin reduce selection is a separate ADR. Returning nil is
			// safer than pretending a multi-position pool has a single q*.
			return nil
		}
		out = p
	}
	return out
}

func reducedHealthSafe(p *Position, mark, closeQty dec.Decimal, mmrOf MMRFunc, buffer dec.Decimal) bool {
	if closeQty.Cmp(p.Size) >= 0 {
		return true
	}
	remaining := p.Size.Sub(closeQty)
	notional := mark.Mul(remaining)
	if notional.Sign() <= 0 {
		return true
	}
	// The remaining position keeps the pre-close equity at mark. That mirrors
	// partial liquidation's purpose: realize part of the loss, shrink notional,
	// and leave the remaining isolated margin safer instead of withdrawing it.
	equity := p.Margin.Add(p.UnrealizedPnL(mark))
	ratio := equity.Div(notional)
	target := mmrOf(notional).Add(buffer)
	return ratio.Cmp(target) >= 0
}
