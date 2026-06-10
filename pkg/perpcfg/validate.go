package perpcfg

import (
	"fmt"
	"strings"

	"github.com/xargin/opentrade/pkg/dec"
)

// maxScale bounds price/qty decimal scales. 18 matches the DECIMAL(36,18)
// columns the perp projections already use.
const maxScale = 32

var one = dec.FromInt(1)

// ValidateSpec checks a contract spec at CreatePerpSymbol time (ADR-0075 §1).
func ValidateSpec(s PerpSymbol) error {
	if err := validateSymbolName(s.Symbol); err != nil {
		return err
	}
	switch s.ContractType {
	case ContractLinearPerp:
	case ContractLinearFuture, ContractInversePerp, ContractInverseFuture:
		return fmt.Errorf("perpcfg: contract_type %s is reserved and disabled until ADR-0076", s.ContractType)
	default:
		return fmt.Errorf("perpcfg: unknown contract_type %q", s.ContractType)
	}
	if s.BaseAsset == "" || s.QuoteAsset == "" || s.SettleAsset == "" {
		return fmt.Errorf("perpcfg: base/quote/settle assets are required")
	}
	if s.SettleAsset != s.QuoteAsset {
		return fmt.Errorf("perpcfg: linear contracts settle in the quote asset (settle %s != quote %s)", s.SettleAsset, s.QuoteAsset)
	}
	if s.ContractSize.Sign() <= 0 {
		return fmt.Errorf("perpcfg: contract_size must be > 0")
	}
	if s.PriceScale < 0 || s.PriceScale > maxScale {
		return fmt.Errorf("perpcfg: price_scale must be in [0, %d]", maxScale)
	}
	if s.QtyScale < 0 || s.QtyScale > maxScale {
		return fmt.Errorf("perpcfg: qty_scale must be in [0, %d]", maxScale)
	}
	return nil
}

func validateSymbolName(symbol string) error {
	if symbol == "" {
		return fmt.Errorf("perpcfg: symbol is required")
	}
	for i := 0; i < len(symbol); i++ {
		c := symbol[i]
		if c == '/' || c == 0 || c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			return fmt.Errorf("perpcfg: symbol %q contains an illegal character", symbol)
		}
	}
	return nil
}

// ValidateConfig checks a versioned parameter set before publish. spec is the
// owning contract spec (scale consistency); pass the zero value to skip the
// scale cross-checks (e.g. when only the config row is at hand).
func ValidateConfig(spec PerpSymbol, c PerpSymbolConfig) error {
	if c.Symbol == "" {
		return fmt.Errorf("perpcfg: config symbol is required")
	}
	if spec.Symbol != "" && spec.Symbol != c.Symbol {
		return fmt.Errorf("perpcfg: config symbol %s does not match spec %s", c.Symbol, spec.Symbol)
	}
	if c.ConfigVersion == 0 {
		return fmt.Errorf("perpcfg: config_version must be > 0")
	}
	if !c.Status.Valid() {
		return fmt.Errorf("perpcfg: invalid status %q", c.Status)
	}
	if err := validatePrecision(spec, c.Precision); err != nil {
		return err
	}
	if err := validateOrderLimits(c.OrderLimits); err != nil {
		return err
	}
	if err := ValidateRiskTiers(c.RiskTiers); err != nil {
		return err
	}
	if err := validateFunding(c.Funding); err != nil {
		return err
	}
	if err := validatePricing(c.Pricing); err != nil {
		return err
	}
	if err := validateFees(c.Fees); err != nil {
		return err
	}
	if err := validateProtection(c.PriceProtection); err != nil {
		return err
	}
	switch c.RiskApply {
	case RiskApplyStaged:
	case RiskApplyImmediate:
	default:
		return fmt.Errorf("perpcfg: risk_apply must be %s or %s", RiskApplyStaged, RiskApplyImmediate)
	}
	if c.RepricePolicy != nil {
		if c.RiskApply != RiskApplyImmediate {
			return fmt.Errorf("perpcfg: reprice_policy only applies to risk_apply=%s", RiskApplyImmediate)
		}
		if c.RepricePolicy.PolicyID == "" {
			return fmt.Errorf("perpcfg: reprice_policy.policy_id is required")
		}
		if c.RepricePolicy.MaxAffectedAccounts < 0 {
			return fmt.Errorf("perpcfg: reprice_policy.max_affected_accounts must be >= 0")
		}
	}
	if c.EffectiveFromMs < 0 {
		return fmt.Errorf("perpcfg: effective_from_ms must be >= 0")
	}
	return nil
}

// ValidateRiskApply enforces the §3 staging guardrail across versions: a
// tightening tier table may only be IMMEDIATE when a RiskRepricePolicy is
// attached. prevTiers come from the previous published version (nil for the
// first publish).
func ValidateRiskApply(prevTiers []RiskTier, c PerpSymbolConfig) error {
	if c.RiskApply != RiskApplyImmediate {
		return nil
	}
	if TightensRisk(prevTiers, c.RiskTiers) && c.RepricePolicy == nil {
		return fmt.Errorf("perpcfg: tightening risk tiers with risk_apply=IMMEDIATE requires a reprice_policy (ADR-0075 §3)")
	}
	return nil
}

func validatePrecision(spec PerpSymbol, p Precision) error {
	if p.TickSize.Sign() <= 0 {
		return fmt.Errorf("perpcfg: precision.tick_size must be > 0")
	}
	if p.QtyStep.Sign() <= 0 {
		return fmt.Errorf("perpcfg: precision.qty_step must be > 0")
	}
	if spec.Symbol != "" {
		if !fitsScale(p.TickSize, spec.PriceScale) {
			return fmt.Errorf("perpcfg: tick_size %s does not fit price_scale %d", p.TickSize, spec.PriceScale)
		}
		if !fitsScale(p.QtyStep, spec.QtyScale) {
			return fmt.Errorf("perpcfg: qty_step %s does not fit qty_scale %d", p.QtyStep, spec.QtyScale)
		}
	}
	return nil
}

// fitsScale reports whether v is representable with at most `scale` decimals.
func fitsScale(v dec.Decimal, scale int32) bool {
	return v.Equal(v.Truncate(scale))
}

func validateOrderLimits(l OrderLimits) error {
	for name, v := range map[string]dec.Decimal{
		"min_price": l.MinPrice, "max_price": l.MaxPrice,
		"min_order_qty": l.MinOrderQty, "max_order_qty": l.MaxOrderQty,
		"min_notional": l.MinNotional,
	} {
		if v.Sign() < 0 {
			return fmt.Errorf("perpcfg: order_limits.%s must be >= 0", name)
		}
	}
	if l.MinPrice.Sign() > 0 && l.MaxPrice.Sign() > 0 && l.MinPrice.Cmp(l.MaxPrice) > 0 {
		return fmt.Errorf("perpcfg: order_limits.min_price > max_price")
	}
	if l.MinOrderQty.Sign() > 0 && l.MaxOrderQty.Sign() > 0 && l.MinOrderQty.Cmp(l.MaxOrderQty) > 0 {
		return fmt.Errorf("perpcfg: order_limits.min_order_qty > max_order_qty")
	}
	return nil
}

// ValidateRiskTiers checks the §3 tier table: 1-based contiguous risk_id,
// strictly ascending caps with only the last open-ended, sane ratios, and
// MMR < 1/max_leverage per tier (otherwise a fresh position at full leverage
// would be born liquidatable).
func ValidateRiskTiers(tiers []RiskTier) error {
	if len(tiers) == 0 {
		return fmt.Errorf("perpcfg: at least one risk tier is required")
	}
	prevCap := zero
	for i, t := range tiers {
		if t.RiskID != uint32(i+1) {
			return fmt.Errorf("perpcfg: risk_tiers[%d].risk_id must be %d, got %d", i, i+1, t.RiskID)
		}
		last := i == len(tiers)-1
		if t.MaxNotional.Sign() == 0 && !last {
			return fmt.Errorf("perpcfg: only the last risk tier may have an open-ended max_notional")
		}
		if t.MaxNotional.Sign() < 0 {
			return fmt.Errorf("perpcfg: risk_tiers[%d].max_notional must be >= 0", i)
		}
		if t.MaxNotional.Sign() > 0 && t.MaxNotional.Cmp(prevCap) <= 0 {
			return fmt.Errorf("perpcfg: risk tier caps must be strictly ascending (tier %d)", t.RiskID)
		}
		if t.MaxNotional.Sign() > 0 {
			prevCap = t.MaxNotional
		}
		if t.MaintMarginRatio.Sign() <= 0 || t.MaintMarginRatio.Cmp(one) >= 0 {
			return fmt.Errorf("perpcfg: risk_tiers[%d].maintenance_margin_ratio must be in (0, 1)", i)
		}
		if t.MaxLeverage.Sign() <= 0 {
			return fmt.Errorf("perpcfg: risk_tiers[%d].max_leverage must be > 0", i)
		}
		if t.LiqFeeRate.Sign() < 0 || t.LiqFeeRate.Cmp(one) >= 0 {
			return fmt.Errorf("perpcfg: risk_tiers[%d].liq_fee_rate must be in [0, 1)", i)
		}
		// IM at max leverage is 1/max_leverage of notional; maintenance must sit
		// strictly below it or a fully levered open is instantly liquidatable.
		if t.MaintMarginRatio.Mul(t.MaxLeverage).Cmp(one) >= 0 {
			return fmt.Errorf("perpcfg: risk_tiers[%d]: maintenance_margin_ratio %s × max_leverage %s >= 1 — a full-leverage open would be born liquidatable",
				i, t.MaintMarginRatio, t.MaxLeverage)
		}
		// Tiers grow more conservative with notional: MMR must not decrease and
		// max_leverage must not increase from one tier to the next.
		if i > 0 {
			if t.MaintMarginRatio.Cmp(tiers[i-1].MaintMarginRatio) < 0 {
				return fmt.Errorf("perpcfg: risk_tiers[%d].maintenance_margin_ratio decreases vs tier %d", i, i)
			}
			if t.MaxLeverage.Cmp(tiers[i-1].MaxLeverage) > 0 {
				return fmt.Errorf("perpcfg: risk_tiers[%d].max_leverage increases vs tier %d", i, i)
			}
		}
	}
	return nil
}

func validateFunding(f FundingParams) error {
	if f.IntervalSeconds <= 0 {
		return fmt.Errorf("perpcfg: funding.interval_seconds must be > 0")
	}
	switch f.PremiumSource {
	case "", "impact_mid":
	default:
		return fmt.Errorf("perpcfg: funding.premium_source %q is not supported (v1: impact_mid)", f.PremiumSource)
	}
	if f.Cap.Sign() < 0 {
		return fmt.Errorf("perpcfg: funding.cap must be >= 0")
	}
	if f.Floor.Sign() > 0 {
		return fmt.Errorf("perpcfg: funding.floor must be <= 0")
	}
	if f.Clamp.Sign() < 0 {
		return fmt.Errorf("perpcfg: funding.clamp must be >= 0")
	}
	if f.SettlementDelayMs < 0 {
		return fmt.Errorf("perpcfg: funding.settlement_delay_ms must be >= 0")
	}
	return nil
}

func validatePricing(p PricingParams) error {
	if p.MarkEmaAlpha.Sign() <= 0 || p.MarkEmaAlpha.Cmp(one) > 0 {
		return fmt.Errorf("perpcfg: pricing.mark_ema_alpha must be in (0, 1]")
	}
	if p.MarkBasisCap.Sign() < 0 {
		return fmt.Errorf("perpcfg: pricing.mark_basis_cap must be >= 0")
	}
	if p.ImpactNotional.Sign() < 0 {
		return fmt.Errorf("perpcfg: pricing.impact_notional must be >= 0")
	}
	if p.IndexDeviationBand.Sign() < 0 {
		return fmt.Errorf("perpcfg: pricing.index_deviation_band must be >= 0")
	}
	if p.IndexQuorum < 0 {
		return fmt.Errorf("perpcfg: pricing.index_quorum must be >= 0")
	}
	if p.IndexMaxAgeMs < 0 {
		return fmt.Errorf("perpcfg: pricing.index_max_age_ms must be >= 0")
	}
	seen := map[string]struct{}{}
	for _, src := range p.IndexSources {
		if src.Name == "" {
			return fmt.Errorf("perpcfg: pricing.index_sources entries need a name")
		}
		if _, dup := seen[src.Name]; dup {
			return fmt.Errorf("perpcfg: pricing.index_sources has duplicate %q", src.Name)
		}
		seen[src.Name] = struct{}{}
		if src.Weight.Sign() <= 0 {
			return fmt.Errorf("perpcfg: pricing.index_sources[%s].weight must be > 0", src.Name)
		}
	}
	if len(p.IndexSources) > 0 && p.IndexQuorum > len(p.IndexSources) {
		return fmt.Errorf("perpcfg: pricing.index_quorum %d exceeds source count %d", p.IndexQuorum, len(p.IndexSources))
	}
	return nil
}

func validateFees(f FeeParams) error {
	if f.MakerFeeRate.Abs().Cmp(one) >= 0 {
		return fmt.Errorf("perpcfg: fees.maker_fee_rate must be in (-1, 1)")
	}
	if f.TakerFeeRate.Sign() < 0 || f.TakerFeeRate.Cmp(one) >= 0 {
		return fmt.Errorf("perpcfg: fees.taker_fee_rate must be in [0, 1)")
	}
	return nil
}

func validateProtection(p PriceProtection) error {
	if p.LimitPriceBandBps < 0 || p.MarketSlippageBps < 0 {
		return fmt.Errorf("perpcfg: price_protection bps fields must be >= 0")
	}
	switch strings.ToLower(p.ReferencePriceSource) {
	case "", "mark", "index", "last":
	default:
		return fmt.Errorf("perpcfg: price_protection.reference_price_source must be mark | index | last")
	}
	return nil
}
