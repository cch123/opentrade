package perpcfg

import (
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

var zero = dec.FromInt(0)

// ContractType enumerates catalog product classes (ADR-0075 §1). Only
// LINEAR_PERP is enabled; LINEAR_FUTURE and the inverse values are reserved
// so the ADR-0076 expansion can be expressed by the same catalog without a
// schema change — validation rejects them until that ADR lands.
type ContractType string

const (
	ContractLinearPerp    ContractType = "LINEAR_PERP"
	ContractLinearFuture  ContractType = "LINEAR_FUTURE"  // reserved (ADR-0076)
	ContractInversePerp   ContractType = "INVERSE_PERP"   // reserved, disabled
	ContractInverseFuture ContractType = "INVERSE_FUTURE" // reserved, disabled
)

// PerpSymbol is the stable contract-spec half of the catalog (perp_symbols).
// These fields do not change after listing; everything an operator may need
// to adjust lives in the versioned PerpSymbolConfig.
type PerpSymbol struct {
	Symbol       string       `json:"symbol"`
	ContractType ContractType `json:"contract_type"`
	BaseAsset    string       `json:"base_asset"`   // e.g. BTC
	QuoteAsset   string       `json:"quote_asset"`  // e.g. USDT
	SettleAsset  string       `json:"settle_asset"` // == quote for linear contracts
	ContractSize dec.Decimal  `json:"contract_size"`
	PriceScale   int32        `json:"price_scale"` // max decimals carried by price fields
	QtyScale     int32        `json:"qty_scale"`   // max decimals carried by qty fields
	Alias        string       `json:"alias,omitempty"`
	CreatedAtMs  int64        `json:"created_at_ms,omitempty"`
}

// Precision is the order-grid half of the config. Glossary: tick_size is
// BN/Bybit tickSize (minimum price increment); qty_step is BN stepSize /
// Bybit qtyStep (minimum qty increment).
type Precision struct {
	TickSize dec.Decimal `json:"tick_size"`
	QtyStep  dec.Decimal `json:"qty_step"`
}

// OrderLimits bounds a single order at admission (ADR-0075 §1 订单约束).
// Zero means "unbounded" for every field.
type OrderLimits struct {
	MinPrice    dec.Decimal `json:"min_price"`
	MaxPrice    dec.Decimal `json:"max_price"`
	MinOrderQty dec.Decimal `json:"min_order_qty"`
	MaxOrderQty dec.Decimal `json:"max_order_qty"`
	MinNotional dec.Decimal `json:"min_notional"` // price × qty lower bound (BN minNotional / Bybit minNotionalValue)
}

// RiskTier is one notional bucket of the §3 risk table. risk_id is 1-based and
// must equal the tier's position in the list; tiers are ordered by ascending
// max_notional with only the last allowed to be open-ended (0).
// initial_margin_ratio is informational (display / API parity with Bybit) —
// admission derives IM from leverage, so only max_leverage gates orders.
type RiskTier struct {
	RiskID             uint32      `json:"risk_id"`
	MaxNotional        dec.Decimal `json:"max_notional"`
	InitialMarginRatio dec.Decimal `json:"initial_margin_ratio,omitempty"`
	MaintMarginRatio   dec.Decimal `json:"maintenance_margin_ratio"`
	MaxLeverage        dec.Decimal `json:"max_leverage"`
	LiqFeeRate         dec.Decimal `json:"liq_fee_rate"`
}

// FundingParams is the §3 funding block. Glossary mapping to perp-pricing's
// calc.Config: interest_rate → InterestDaily, cap/floor → the settled-rate
// clamp (FundingCap; floor 0 falls back to -cap), clamp → PremiumBand (the
// Binance ±0.05% band on interest - premium).
type FundingParams struct {
	IntervalSeconds   int64       `json:"interval_seconds"`
	PremiumSource     string      `json:"premium_source,omitempty"` // "" / "impact_mid": depth-weighted impact prices (the only v1 source)
	InterestRate      dec.Decimal `json:"interest_rate"`            // daily
	Cap               dec.Decimal `json:"cap"`                      // upper clamp on the settled rate (>= 0)
	Floor             dec.Decimal `json:"floor"`                    // lower clamp (<= 0); 0 = -cap
	Clamp             dec.Decimal `json:"clamp"`                    // ± premium band
	SettlementDelayMs int64       `json:"settlement_delay_ms,omitempty"`
}

// IndexSource is one constituent of the composite index (ADR-0069).
type IndexSource struct {
	Name   string      `json:"name"` // "self:<spot-symbol>" or an external feed name
	Weight dec.Decimal `json:"weight"`
}

// PricingParams carries the §1 pricing parameters that are not funding-rate
// inputs: the composite index configuration (ADR-0069) and the mark formula
// knobs. Kept as its own column so pricing and funding can be tuned
// independently, while both still follow the same config_version.
type PricingParams struct {
	SpotSymbol         string        `json:"spot_symbol,omitempty"` // self index source; "" derives <base>-<quote>
	IndexSources       []IndexSource `json:"index_sources,omitempty"`
	IndexQuorum        int           `json:"index_quorum,omitempty"`
	IndexMaxAgeMs      int64         `json:"index_max_age_ms,omitempty"`
	IndexDeviationBand dec.Decimal   `json:"index_deviation_band"`
	MarkEmaAlpha       dec.Decimal   `json:"mark_ema_alpha"` // EMA smoothing of the basis, (0,1]
	MarkBasisCap       dec.Decimal   `json:"mark_basis_cap"` // clamp on |mark - index|; 0 = none
	ImpactNotional     dec.Decimal   `json:"impact_notional"`
}

// FeeParams is the §1 fee block. Negative maker rate = rebate. Enforcement
// (fee accounting) is ADR-0079; the catalog carries the params now so every
// later fill can reference the version it was charged under.
type FeeParams struct {
	MakerFeeRate dec.Decimal `json:"maker_fee_rate"`
	TakerFeeRate dec.Decimal `json:"taker_fee_rate"`
	FeeRuleID    string      `json:"fee_rule_id,omitempty"`
}

// PriceProtection is the §1 price-protection block. Enforcement is ADR-0080;
// carried
// in the catalog so the publish / version plumbing exists from day one.
type PriceProtection struct {
	LimitPriceBandBps    int64  `json:"limit_price_band_bps,omitempty"` // limit price must sit within ±band of the reference; 0 = disabled
	MarketSlippageBps    int64  `json:"market_slippage_bps,omitempty"`  // default market-order protection; 0 = disabled
	ReferencePriceSource string `json:"reference_price_source,omitempty"` // mark | index | last
}

// RiskApply states how a version's risk tiers apply to existing positions
// (ADR-0075 §3).
type RiskApply string

const (
	// RiskApplyStaged: only new opens / size increases use this version's
	// tiers; existing positions keep evaluating at the version they last
	// changed under. Default for tightening changes.
	RiskApplyStaged RiskApply = "STAGED"
	// RiskApplyImmediate: existing positions re-evaluate at this version. A
	// tightening change may only be IMMEDIATE with a RiskRepricePolicy.
	RiskApplyImmediate RiskApply = "IMMEDIATE"
)

// RiskRepricePolicy is the §3 publish artifact that authorizes an IMMEDIATE
// tightening: it pins who approved repricing existing positions, the dry-run
// budget, and whether a resulting batch liquidation is acceptable.
type RiskRepricePolicy struct {
	PolicyID string `json:"policy_id"`
	// MaxAffectedAccounts is the declared budget: the dry-run's count of
	// accounts whose maintenance requirement would increase must stay <=
	// this, or the publish fails (0 = the projection must report zero
	// affected accounts).
	MaxAffectedAccounts int64 `json:"max_affected_accounts"`
	// AllowMassLiquidation must be true for the publish to proceed when the
	// dry-run reports any account that would breach maintenance outright.
	AllowMassLiquidation bool   `json:"allow_mass_liquidation"`
	Reason               string `json:"reason,omitempty"`
}

// PerpSymbolConfig is one published, versioned parameter set
// (perp_symbol_configs). config_version is symbol-scoped and strictly
// monotonic; a rollback publishes a NEW version whose content copies
// SourceVersion (never overwrites history, ADR-0075 §5).
type PerpSymbolConfig struct {
	Symbol          string             `json:"symbol"`
	ConfigVersion   uint64             `json:"config_version"`
	Status          Status             `json:"status"`
	Precision       Precision          `json:"precision"`
	OrderLimits     OrderLimits        `json:"order_limits"`
	RiskTiers       []RiskTier         `json:"risk_tiers"`
	Funding         FundingParams      `json:"funding"`
	Pricing         PricingParams      `json:"pricing"`
	Fees            FeeParams          `json:"fees"`
	PriceProtection PriceProtection    `json:"price_protection"`
	RiskApply       RiskApply          `json:"risk_apply"`
	RepricePolicy   *RiskRepricePolicy `json:"reprice_policy,omitempty"`
	EffectiveFromMs int64              `json:"effective_from_ms"`
	CreatedBy       string             `json:"created_by,omitempty"`
	Reason          string             `json:"reason,omitempty"`
	SourceVersion   uint64             `json:"source_version,omitempty"` // non-zero on rollback copies
	CreatedAtMs     int64              `json:"created_at_ms,omitempty"`
}

// RiskModel converts the tier table into the engine-facing form. Defaults are
// zero — a catalog-driven symbol has no scalar fallback; the tier table is
// authoritative (the open-ended last tier covers all notional).
func (c *PerpSymbolConfig) RiskModel() perpstate.RiskModel {
	tiers := make([]perpstate.RiskTier, 0, len(c.RiskTiers))
	for _, t := range c.RiskTiers {
		tiers = append(tiers, perpstate.RiskTier{
			TierMaxNotional:  t.MaxNotional,
			MaintMarginRatio: t.MaintMarginRatio,
			MaxLeverage:      t.MaxLeverage,
			LiqFeeRate:       t.LiqFeeRate,
		})
	}
	return perpstate.NewRiskModel(tiers, zero, zero, zero)
}

// TightensRisk reports whether next is more restrictive than prev anywhere —
// the §3 trigger that forces a version into STAGED mode (or demands a
// RiskRepricePolicy for IMMEDIATE). Structural changes that cannot be compared
// tier-by-tier are conservatively treated as tightening.
func TightensRisk(prev, next []RiskTier) bool {
	if len(prev) == 0 {
		return false // first table: nothing pre-existing to tighten against
	}
	if len(prev) != len(next) {
		return true
	}
	for i := range next {
		p, n := prev[i], next[i]
		if n.MaintMarginRatio.Cmp(p.MaintMarginRatio) > 0 {
			return true
		}
		if n.MaxLeverage.Cmp(p.MaxLeverage) < 0 {
			return true
		}
		// max_notional 0 = open-ended; bounding a previously open-ended tier
		// or shrinking a bound both tighten.
		switch {
		case p.MaxNotional.Sign() == 0 && n.MaxNotional.Sign() != 0:
			return true
		case p.MaxNotional.Sign() != 0 && n.MaxNotional.Sign() != 0 &&
			n.MaxNotional.Cmp(p.MaxNotional) < 0:
			return true
		}
	}
	return false
}
