package service

// catalog.go wires the ADR-0075 SymbolConfig catalog into the service: the
// order-admission gates (status machine, precision, order limits, fail-closed
// on unknown/stale config), the per-symbol risk resolver the engine judges
// positions through, and the config-version journal stamps.
//
// Catalog disabled (nil cache) keeps the legacy flag-driven behavior
// everywhere and stamps version 0, which also tells Match to skip the
// handshake.

import (
	"context"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpcfg"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// riskMemo caches built RiskModels per (symbol, version) — building one
// sorts and copies the tier table, and the resolver runs inside engine
// critical sections.
type riskMemo struct {
	mu sync.Mutex
	m  map[string]perpstate.RiskModel
}

func memoKey(symbol string, ver uint64) string {
	return symbol + "@" + strconv.FormatUint(ver, 10)
}

// resolveRisk implements engine.RiskResolver: the effective risk version
// comes from the cache (staged pins, ADR-0075 §3), the model is memoized.
// Called under the engine lock; only takes leaf locks (cache RLock, memo).
func (s *Service) resolveRisk(symbol string, pinned uint64) (perpstate.RiskModel, uint64, bool) {
	if s.catalog == nil {
		return perpstate.RiskModel{}, 0, false
	}
	ver, ok := s.catalog.EffectiveRiskVersion(symbol, pinned, s.now())
	if !ok {
		return perpstate.RiskModel{}, 0, false
	}
	m, ok := s.riskModelAt(symbol, ver)
	return m, ver, ok
}

func (s *Service) riskModelAt(symbol string, ver uint64) (perpstate.RiskModel, bool) {
	key := memoKey(symbol, ver)
	s.riskMemo.mu.Lock()
	if m, ok := s.riskMemo.m[key]; ok {
		s.riskMemo.mu.Unlock()
		return m, true
	}
	s.riskMemo.mu.Unlock()
	cfg, ok := s.catalog.At(symbol, ver)
	if !ok {
		return perpstate.RiskModel{}, false
	}
	m := cfg.RiskModel()
	s.riskMemo.mu.Lock()
	s.riskMemo.m[key] = m
	s.riskMemo.mu.Unlock()
	return m, true
}

// riskModelForOrder resolves the ACTIVE model for service-level admission
// (tier-cap check). Legacy mode falls back to the flag-built model; in
// catalog mode a missing symbol yields the zero model — unreachable in
// practice because admission fail-closes on the missing view first.
func (s *Service) riskModelForOrder(symbol string) perpstate.RiskModel {
	if s.catalog == nil {
		return s.risk
	}
	if m, _, ok := s.resolveRisk(symbol, 0); ok {
		return m
	}
	return perpstate.RiskModel{}
}

// riskModelForPosition resolves the model judging the (user, symbol, idx)
// leg's existing position — pinned-version-aware — plus the resolved version
// for journal stamping. Legacy mode: flag model, version 0.
func (s *Service) riskModelForPosition(user uint64, symbol string, idx uint8) (perpstate.RiskModel, uint64) {
	if s.catalog == nil {
		return s.risk, 0
	}
	var pinned uint64
	if p, ok := s.eng.PositionRaw(user, symbol, idx); ok {
		pinned = p.RiskConfigVersion
	}
	if m, ver, ok := s.resolveRisk(symbol, pinned); ok {
		return m, ver
	}
	return perpstate.RiskModel{}, 0
}

// riskPolicyID returns the reprice policy id attached to a version, for the
// §3 journal provenance ("which policy forced this version onto existing
// positions").
func (s *Service) riskPolicyID(symbol string, ver uint64) string {
	if s.catalog == nil || ver == 0 {
		return ""
	}
	if cfg, ok := s.catalog.At(symbol, ver); ok && cfg.RepricePolicy != nil {
		return cfg.RepricePolicy.PolicyID
	}
	return ""
}

// liquidationEnabled reports whether a usable MMR governs symbol — the
// per-symbol generalization of the old global HasMMR gate.
func (s *Service) liquidationEnabled(symbol string) bool {
	if s.catalog == nil {
		return s.risk.HasMMR()
	}
	m, _, ok := s.resolveRisk(symbol, 0)
	return ok && m.HasMMR()
}

// admitAgainstCatalog runs the ADR-0075 admission gates for a new order and
// returns the config version to stamp into the OrderEvent. reason == ""
// means pass. The version is captured from the SAME view the gates ran
// against — if the config flips before Match processes the order, Match's
// handshake rejects it (the designed propagation-window behavior).
func (s *Service) admitAgainstCatalog(symbol string, price, qty dec.Decimal, isMarket, postOnly, reduceOnly bool) (uint64, string) {
	if s.catalog == nil {
		return 0, ""
	}
	if s.catalog.Stale() {
		return 0, "symbol_config_stale"
	}
	view, ok := s.catalog.Active(symbol)
	if !ok {
		return 0, "unknown_symbol_config"
	}
	if !view.Cfg.Status.CanPlaceOrder(postOnly, reduceOnly) {
		return 0, perpcfg.RejectSymbolStatus
	}
	if r := view.Cfg.CheckOrder(price, qty, isMarket); r != "" {
		return 0, r
	}
	return view.Cfg.ConfigVersion, ""
}

// cancelAllowed checks the status machine for a user cancel. A symbol
// missing from the catalog fails OPEN here: cancels reduce exposure, and the
// fail-closed rule exists to guard new exposure.
func (s *Service) cancelAllowed(symbol string) bool {
	if s.catalog == nil {
		return true
	}
	if v, ok := s.catalog.Active(symbol); ok {
		return v.Cfg.Status.CanCancelOrder()
	}
	return true
}

// bookAccepts reports whether Match would admit a system (non-post-only)
// order in the symbol's current status — the liquidation flow uses it to
// skip the doomed Match round-trip and go straight to the backstop when the
// book is closed to new orders (CANCEL_ONLY etc.).
func (s *Service) bookAccepts(symbol string) bool {
	if s.catalog == nil {
		return true
	}
	if v, ok := s.catalog.Active(symbol); ok {
		return v.Cfg.Status.BookAllowsPlace(false)
	}
	return false
}

// feePin is the ADR-0079 §1 fee quadruple resolved at order admission and
// carried by the order (and its snapshot) for deterministic settlement.
type feePin struct {
	RuleID string
	Maker  dec.Decimal // signed; < 0 only when negative maker fees are enabled
	Taker  dec.Decimal // >= 0; also the fee-buffer rate
	Asset  string      // settle asset (spec, version-independent)
	// MakerSuppressed records that a negative maker rate was degraded to
	// zero here (AllowNegativeMakerFee off) — settlement journals it as
	// rebate_suppressed on maker fills.
	MakerSuppressed bool
}

// feePinFor resolves the fee pin for (user, symbol) against the SAME catalog
// view admission gated on: content is fetched by the admitted version
// (immutable rows), never via a second Active() read — no TOCTOU window
// between the gates and the pin. Precedence: user+symbol override >
// user-global override > SymbolConfig.FeeParams. Catalog disabled (version
// 0) keeps fees at zero — the legacy-mode behavior.
func (s *Service) feePinFor(user uint64, symbol string, cfgVersion uint64) (feePin, string) {
	if s.catalog == nil || cfgVersion == 0 {
		return feePin{}, ""
	}
	cfg, ok := s.catalog.At(symbol, cfgVersion)
	if !ok {
		return feePin{}, "unknown_symbol_config"
	}
	spec, ok := s.catalog.SpecOf(symbol)
	if !ok {
		return feePin{}, "unknown_symbol_config"
	}
	pin := feePin{
		RuleID: cfg.Fees.FeeRuleID,
		Maker:  cfg.Fees.MakerFeeRate,
		Taker:  cfg.Fees.TakerFeeRate,
		Asset:  spec.SettleAsset,
	}
	if pin.RuleID == "" {
		pin.RuleID = "sym:" + symbol + "@v" + strconv.FormatUint(cfgVersion, 10)
	}
	if ov, ok := s.eng.CustomerFeeOverride(user, symbol); ok {
		pin.RuleID, pin.Maker, pin.Taker = ov.RuleID, ov.MakerRate, ov.TakerRate
	}
	// System accounts trade fee-exempt (the backstop's disposal flow must not
	// pay the platform its own money).
	if s.cfg.BackstopAccount != 0 && user == s.cfg.BackstopAccount {
		return feePin{RuleID: "system-exempt", Maker: zero, Taker: zero, Asset: pin.Asset}, ""
	}
	if pin.Maker.Sign() < 0 && !s.cfg.AllowNegativeMakerFee {
		pin.Maker = zero
		pin.MakerSuppressed = true
	}
	return pin, ""
}

// activeConfigVersion is the settle-time stamp for journal records (0 in
// legacy mode or when the symbol has no effective config).
func (s *Service) activeConfigVersion(symbol string) uint64 {
	if s.catalog == nil {
		return 0
	}
	if v, ok := s.catalog.Active(symbol); ok {
		return v.Cfg.ConfigVersion
	}
	return 0
}

// RunCatalogRefresh keeps the engine's derived risk state in step with the
// catalog: whenever any symbol's ACTIVE version changes (a publish landing
// or an effective_from_ms boundary passing), the precomputed liq-price
// index is rebuilt against the new tiers. Risk DECISIONS always resolve
// live through resolveRisk — this loop only refreshes the candidate-scan
// index, so the worst case between ticks is a ≤interval delay in surfacing
// newly-liquidatable candidates (the authoritative re-check still judges
// with live tiers). interval should match the catalog poll interval.
func (s *Service) RunCatalogRefresh(ctx context.Context, interval time.Duration) {
	if s.catalog == nil {
		return
	}
	if interval <= 0 {
		interval = time.Second
	}
	last := s.activeVersionFingerprint()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := s.activeVersionFingerprint()
			if !maps.Equal(cur, last) {
				s.eng.RebuildRiskIndex()
				last = cur
			}
		}
	}
}

func (s *Service) activeVersionFingerprint() map[string]uint64 {
	out := map[string]uint64{}
	for _, sym := range s.catalog.Symbols() {
		if v, ok := s.catalog.Active(sym); ok {
			out[sym] = v.Cfg.ConfigVersion
		}
	}
	return out
}
