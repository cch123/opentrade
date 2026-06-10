// Package engine is perp-counter's stateful container around pkg/perpstate
// (ADR-0068 A1). It holds per-user USDT futures-margin wallets,
// per-(user, symbol) positions, the latest mark per symbol, and a local
// insurance-fund cache used by the single-instance fallback. ADR-0071 moves the
// authoritative fund to perp-risk once perp-counter is sharded; shards still
// compute InsuranceDelta here so the journal stream remains the recovery and
// coordinator input.
//
// Map access is mutex-guarded for safety. Per-user write serialization
// (ADR-0068 invariant #1: all of a user's position mutations happen on one
// sequencer) is the service layer's responsibility (M3); engine methods
// assume they are already called in that serialized context.
package engine

import (
	"sort"
	"sync"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// Wallet is a user's USDT futures-margin ledger (ADR-0074 §2 buckets).
//
//	Available     = free_balance: cash not locked by isolated position margin
//	                or open-order reservations.
//	Reserved      = order_margin_reserved for ISOLATED-mode orders; converts
//	                into position margin on fill (ADR-0041).
//	CrossReserved = order_margin_reserved for CROSS-mode orders; released
//	                back to Available on fill/cancel (a cross position holds
//	                no margin bucket). Kept separate from Reserved because
//	                cross pool equity counts it (recoverable by cancelling)
//	                while isolated reservations are excluded.
//
// Everything else the API reports (available_to_trade, cross requirements)
// is derived from positions + marks + risk tiers, never stored here.
type Wallet struct {
	Available     dec.Decimal
	Reserved      dec.Decimal
	CrossReserved dec.Decimal
}

// TransferStatus is the outcome of a futures-wallet transfer (AssetHolder
// saga leg, ADR-0057).
type TransferStatus uint8

const (
	// TransferConfirmed: the balance moved.
	TransferConfirmed TransferStatus = iota + 1
	// TransferRejected: business reject (insufficient balance). Terminal.
	TransferRejected
)

// TransferOutcome is the cached result of a transfer, keyed by transfer_id for
// idempotency (AssetHolder contract: a repeat returns the first outcome).
type TransferOutcome struct {
	Status         TransferStatus
	AvailableAfter dec.Decimal
	ReservedAfter  dec.Decimal
	RejectReason   string
}

// CustomerLimit is one ADR-0074 §10 leverage cap row. Symbol "" applies to
// every symbol; a symbol-scoped row overrides the global row.
type CustomerLimit struct {
	MaxLeverage dec.Decimal
	Reason      string
	UpdatedBy   string
	UpdatedMs   int64
}

// symbolPositions is one (user, symbol)'s position container (ADR-0077 §1):
// the per-symbol position mode plus up to three legs indexed by position_idx
// (0 = one-way net, 1 = hedge long, 2 = hedge short). Mode and legs share one
// container so flat-only mode-switch validation, snapshot, and replay cover a
// single boundary. A leg, once created, is retained flat — it carries the
// per-leg recovery watermarks (last_match_seq / funding_round_seen /
// last_adl_round), same retention rule as the pre-hedge net record.
type symbolPositions struct {
	mode perpstate.PositionMode
	legs [3]*perpstate.Position
}

// liveLegs appends the non-nil legs to dst in idx order.
func (sp *symbolPositions) liveLegs(dst []*perpstate.Position) []*perpstate.Position {
	if sp == nil {
		return dst
	}
	for _, p := range sp.legs {
		if p != nil {
			dst = append(dst, p)
		}
	}
	return dst
}

// Engine is the in-memory perp account state.
type Engine struct {
	mu        sync.RWMutex
	wallets   map[uint64]*Wallet
	positions map[uint64]map[string]*symbolPositions
	marks     map[string]dec.Decimal
	insurance map[string]dec.Decimal
	transfers map[string]TransferOutcome // transfer_id → outcome (AssetHolder idempotency, ADR-0057)

	// ADR-0074 state. levLimits[user][symbol] ("" = user-global) is the admin
	// leverage cap; ops caches config-op outcomes by client_op_id (same
	// pattern as transfers); crossUsers[symbol] tracks who holds a cross
	// position in the symbol so a mark tick can evaluate the affected
	// account pools without a full scan.
	levLimits  map[uint64]map[string]CustomerLimit
	ops        map[string]OpOutcome
	crossUsers map[string]map[uint64]struct{}
	autoAdd    map[string]map[uint64]struct{} // symbol → users with a live auto-add-enabled isolated position

	// liqIndex is ADR-0072 derived state: it is rebuilt from positions after
	// restore and updated in the same write critical section as every position
	// mutation. Cross positions are excluded — their liquidation trigger is
	// the account pool, not a per-position price (ADR-0074 §4 rule #6). risk
	// resolves each position's effective MMR (tier + RiskID); it is installed
	// by the service because tier tables belong to service config, while
	// Engine owns the position mutation boundary.
	liqIndex *liqIndex
	risk     perpstate.RiskModel
	riskSet  bool

	// riskResolver is the ADR-0075 per-symbol risk source: it resolves the
	// SymbolConfig-versioned risk model governing exposure in a symbol,
	// honoring a position's pinned staged version. When set it takes
	// precedence over the legacy scalar/global model for symbols it covers;
	// symbols it does not cover fall back to the legacy model so existing
	// exposure never silently loses risk evaluation.
	riskResolver RiskResolver
}

// RiskResolver resolves the risk model for symbol at a position's pinned
// config version (ADR-0075 §3; pinnedVersion 0 = the symbol's active
// version). Returns the model, the resolved config version, and whether the
// symbol is catalog-managed. Implementations are called under e.mu and must
// not call back into the Engine.
type RiskResolver func(symbol string, pinnedVersion uint64) (perpstate.RiskModel, uint64, bool)

// New returns an empty engine.
func New() *Engine {
	return &Engine{
		wallets:    map[uint64]*Wallet{},
		positions:  map[uint64]map[string]*symbolPositions{},
		marks:      map[string]dec.Decimal{},
		insurance:  map[string]dec.Decimal{},
		transfers:  map[string]TransferOutcome{},
		levLimits:  map[uint64]map[string]CustomerLimit{},
		ops:        map[string]OpOutcome{},
		crossUsers: map[string]map[uint64]struct{}{},
		autoAdd:    map[string]map[uint64]struct{}{},
		liqIndex:   newLiqIndex(),
	}
}

// TransferIn credits the futures wallet for a saga leg (funding→futures deposit,
// ADR-0057), idempotent on transferID. The second return is true when the id
// was already applied (a DUPLICATED hit returning the original outcome).
func (e *Engine) TransferIn(user uint64, transferID string, amt dec.Decimal) (TransferOutcome, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.transfers[transferID]; ok {
		return prev, true
	}
	w := e.walletLocked(user)
	w.Available = w.Available.Add(amt)
	out := TransferOutcome{Status: TransferConfirmed, AvailableAfter: w.Available, ReservedAfter: w.Reserved}
	e.transfers[transferID] = out
	return out, false
}

// TransferOut debits free margin for a saga leg (futures→funding withdraw),
// idempotent on transferID. An insufficient balance is cached as a REJECTED
// outcome so the same id never succeeds later (the saga must use a fresh id).
// The withdrawable bound is available_to_withdraw, not Available: a cross
// account's free cash also backs its cross positions' initial requirement,
// so unrealized losses / IM must stay covered after the cash leaves
// (ADR-0074 §2).
func (e *Engine) TransferOut(user uint64, transferID string, amt dec.Decimal) (TransferOutcome, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.transfers[transferID]; ok {
		return prev, true
	}
	w := e.walletLocked(user)
	if e.availableToWithdrawLocked(user).Cmp(amt) < 0 {
		out := TransferOutcome{Status: TransferRejected, RejectReason: "insufficient_available",
			AvailableAfter: w.Available, ReservedAfter: w.Reserved}
		e.transfers[transferID] = out
		return out, false
	}
	w.Available = w.Available.Sub(amt)
	out := TransferOutcome{Status: TransferConfirmed, AvailableAfter: w.Available, ReservedAfter: w.Reserved}
	e.transfers[transferID] = out
	return out, false
}

func (e *Engine) walletLocked(user uint64) *Wallet {
	w := e.wallets[user]
	if w == nil {
		w = &Wallet{Available: zero, Reserved: zero, CrossReserved: zero}
		e.wallets[user] = w
	}
	return w
}

// symLocked returns (user, symbol)'s position container, creating it (mode
// ONE_WAY) on first touch. Caller holds e.mu.
func (e *Engine) symLocked(user uint64, symbol string) *symbolPositions {
	bySym := e.positions[user]
	if bySym == nil {
		bySym = map[string]*symbolPositions{}
		e.positions[user] = bySym
	}
	sp := bySym[symbol]
	if sp == nil {
		sp = &symbolPositions{}
		bySym[symbol] = sp
	}
	return sp
}

// symPeekLocked returns the container or nil without creating. Caller holds
// e.mu (any).
func (e *Engine) symPeekLocked(user uint64, symbol string) *symbolPositions {
	bySym := e.positions[user]
	if bySym == nil {
		return nil
	}
	return bySym[symbol]
}

// legLocked returns the (user, symbol, idx) leg, creating it on first touch.
// Caller holds e.mu. idx is clamped into [0,2] by the caller's admission
// validation; an out-of-range idx here is a programming error and panics via
// the slice bound.
func (e *Engine) legLocked(user uint64, symbol string, idx uint8) *perpstate.Position {
	sp := e.symLocked(user, symbol)
	p := sp.legs[idx]
	if p == nil {
		p = &perpstate.Position{UserID: user, Symbol: symbol, PositionIdx: idx,
			Mode: perpstate.MarginIsolated,
			Size: zero, Entry: zero, Margin: zero, Leverage: zero, Realized: zero}
		// Per-symbol config uniformity (ADR-0077 §7): a fresh leg inherits the
		// sibling's config so margin mode / leverage / risk_id / auto-add stay
		// uniform across the symbol's legs regardless of creation order.
		for _, sib := range sp.legs {
			if sib != nil {
				p.Mode = sib.Mode
				p.Leverage = sib.Leverage
				p.RiskID = sib.RiskID
				p.AutoAddMargin = sib.AutoAddMargin
				p.AutoAddMax = sib.AutoAddMax
				break
			}
		}
		sp.legs[idx] = p
	}
	return p
}

// modeLegsLocked returns the legs the (user, symbol) container's CURRENT
// position mode trades on, creating them on demand: idx 0 for ONE_WAY, idx
// 1+2 for HEDGE. Multi-leg config ops operate on exactly this set. Caller
// holds e.mu.
func (e *Engine) modeLegsLocked(user uint64, symbol string) []*perpstate.Position {
	if e.symLocked(user, symbol).mode == perpstate.PositionHedge {
		return []*perpstate.Position{
			e.legLocked(user, symbol, perpstate.IdxLong),
			e.legLocked(user, symbol, perpstate.IdxShort),
		}
	}
	return []*perpstate.Position{e.legLocked(user, symbol, perpstate.IdxNet)}
}

// legPeekLocked returns the leg or nil without creating. Caller holds e.mu
// (any).
func (e *Engine) legPeekLocked(user uint64, symbol string, idx uint8) *perpstate.Position {
	sp := e.symPeekLocked(user, symbol)
	if sp == nil || int(idx) >= len(sp.legs) {
		return nil
	}
	return sp.legs[idx]
}

// legsLocked returns the existing legs of (user, symbol) in idx order,
// including flat ones. Caller holds e.mu (any).
func (e *Engine) legsLocked(user uint64, symbol string) []*perpstate.Position {
	return e.symPeekLocked(user, symbol).liveLegs(nil)
}

// applyFillByIdx dispatches a fill through the right settlement core for the
// record's position_idx: net positions keep flip semantics, hedge legs clamp
// and never flip (ADR-0077 §2). The excess is the clamped-off qty a leg could
// not absorb.
func applyFillByIdx(p *perpstate.Position, f perpstate.Fill) (perpstate.FillResult, dec.Decimal) {
	if p.PositionIdx != perpstate.IdxNet {
		return p.ApplyFillLeg(f)
	}
	return p.ApplyFill(f), zero
}

var zero = dec.FromInt(0)

// Deposit credits the futures wallet (asset-service funding→futures
// TransferIn, ADR-0057). Returns the new available balance.
func (e *Engine) Deposit(user uint64, amt dec.Decimal) dec.Decimal {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	w.Available = w.Available.Add(amt)
	return w.Available
}

// Withdraw debits free margin (futures→funding TransferOut). Returns false
// when available_to_withdraw is insufficient (see TransferOut).
func (e *Engine) Withdraw(user uint64, amt dec.Decimal) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.availableToWithdrawLocked(user).Cmp(amt) < 0 {
		return false
	}
	w := e.walletLocked(user)
	w.Available = w.Available.Sub(amt)
	return true
}

// availableToWithdrawLocked is the ADR-0074 §2 derived bound:
// max(0, min(free_balance, cross equity - cross initial requirement)). For a
// user with no cross positions it equals Available. Caller holds e.mu (any).
func (e *Engine) availableToWithdrawLocked(user uint64) dec.Decimal {
	w := e.wallets[user]
	if w == nil {
		return zero
	}
	cross := e.crossPositionsLocked(user)
	if len(cross) == 0 {
		return w.Available
	}
	h := e.crossHealthLocked(w, cross)
	headroom := h.Equity.Sub(h.InitialRequirement)
	out := dec.Min(w.Available, headroom)
	if out.Sign() < 0 {
		return zero
	}
	return out
}

// Reserve holds initial margin for a new ISOLATED-mode order
// (Available→Reserved). Returns false when available is insufficient — the
// perp pre-trade risk gate (ADR-0068 §4; the check spot deliberately skips).
func (e *Engine) Reserve(user uint64, im dec.Decimal) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	if w.Available.Cmp(im) < 0 {
		return false
	}
	w.Available = w.Available.Sub(im)
	w.Reserved = w.Reserved.Add(im)
	return true
}

// Release returns held isolated-order initial margin to available
// (Reserved→Available) on cancel / reject. Clamped to what is actually
// reserved.
func (e *Engine) Release(user uint64, im dec.Decimal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	move := dec.Min(im, w.Reserved)
	w.Reserved = w.Reserved.Sub(move)
	w.Available = w.Available.Add(move)
}

// ReserveCross holds initial margin for a CROSS-mode order
// (Available→CrossReserved, ADR-0074 §4 rule #4).
func (e *Engine) ReserveCross(user uint64, im dec.Decimal) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	if w.Available.Cmp(im) < 0 {
		return false
	}
	w.Available = w.Available.Sub(im)
	w.CrossReserved = w.CrossReserved.Add(im)
	return true
}

// ReleaseCross returns held cross-order initial margin to available — on
// cancel / reject / expire, and on each fill for the filled proportion (a
// cross fill converts the reservation back to free cash; the exposure is
// carried as a derived requirement, not a margin bucket).
func (e *Engine) ReleaseCross(user uint64, im dec.Decimal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	move := dec.Min(im, w.CrossReserved)
	w.CrossReserved = w.CrossReserved.Sub(move)
	w.Available = w.Available.Add(move)
}

// SetMark records the latest mark price for a symbol (from the mark-price
// topic, ADR-0068 §5).
func (e *Engine) SetMark(symbol string, mark dec.Decimal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.marks[symbol] = mark
}

// MarkOf returns the latest mark (zero if unknown).
func (e *Engine) MarkOf(symbol string) dec.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.marks[symbol]
}

// SetRiskModel installs the legacy global risk model and rebuilds the
// derived liq-price index (ADR-0072). The model is held by Engine after
// service startup so every position mutation can resolve its effective MMR
// (tier table + the position's RiskID, ADR-0074 §9) without widening all
// mutation method signatures. Callers that change risk tiers must call this
// again to rebuild. With an ADR-0075 catalog this is the fallback for
// symbols the resolver does not cover.
func (e *Engine) SetRiskModel(m perpstate.RiskModel) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.risk = m
	e.riskSet = true
	e.rebuildLiquidationIndexLocked()
}

// SetRiskResolver installs the ADR-0075 catalog-backed risk resolver and
// rebuilds the liq-price index against it.
func (e *Engine) SetRiskResolver(r RiskResolver) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.riskResolver = r
	e.rebuildLiquidationIndexLocked()
}

// RebuildRiskIndex recomputes the liq-price index. The catalog refresh loop
// calls this whenever any symbol's effective risk version changes (a publish
// landing or an effective_from_ms boundary passing) — the resolver output
// changed, so every precomputed liq price derived from it must be redone.
func (e *Engine) RebuildRiskIndex() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rebuildLiquidationIndexLocked()
}

// riskModelForLocked resolves the model governing NEW exposure in symbol
// (admission paths — always the active config version). Caller holds e.mu.
func (e *Engine) riskModelForLocked(symbol string) (perpstate.RiskModel, bool) {
	if e.riskResolver != nil {
		if m, _, ok := e.riskResolver(symbol, 0); ok {
			return m, true
		}
	}
	if e.riskSet {
		return e.risk, true
	}
	return perpstate.RiskModel{}, false
}

// riskModelForPositionLocked resolves the model judging an EXISTING
// position: the resolver honors the position's pinned staged version
// (ADR-0075 §3). Caller holds e.mu.
func (e *Engine) riskModelForPositionLocked(p *perpstate.Position) (perpstate.RiskModel, bool) {
	if e.riskResolver != nil {
		if m, _, ok := e.riskResolver(p.Symbol, p.RiskConfigVersion); ok {
			return m, true
		}
	}
	if e.riskSet {
		return e.risk, true
	}
	return perpstate.RiskModel{}, false
}

// hasRiskLocked reports whether ANY risk model governs symbol (catalog or
// legacy). Caller holds e.mu.
func (e *Engine) hasRiskLocked(symbol string) bool {
	_, ok := e.riskModelForLocked(symbol)
	return ok
}

// riskConfiguredLocked reports whether any risk source exists at all —
// legacy global model or catalog resolver. Caller holds e.mu.
func (e *Engine) riskConfiguredLocked() bool {
	return e.riskSet || e.riskResolver != nil
}

// crossModelForLocked adapts the per-position resolution into the
// perpstate.StandardRisk seam. A position whose symbol has no model anywhere
// contributes requirements under the zero model (IM = full notional via the
// 1x floor; MMR = 0) — reachable only when nothing is configured at all,
// which is the pre-existing legacy-dev behavior. Caller holds e.mu.
func (e *Engine) crossModelForLocked(p *perpstate.Position) perpstate.RiskModel {
	m, _ := e.riskModelForPositionLocked(p)
	return m
}

// poolRiskLocked builds the pool risk model with ADR-0075 per-symbol
// resolution. Caller holds e.mu.
func (e *Engine) poolRiskLocked() perpstate.StandardRisk {
	return perpstate.StandardRisk{Model: e.risk, ModelFor: e.crossModelForLocked}
}

// mmrForLocked resolves a position's effective MMR function. nil when no
// risk model with a usable MMR governs the position (liquidation disabled).
func (e *Engine) mmrForLocked(p *perpstate.Position) perpstate.MMRFunc {
	m, ok := e.riskModelForPositionLocked(p)
	if !ok || !m.HasMMR() {
		return nil
	}
	return m.EffectiveMMRFunc(p.RiskID)
}

// MMRFuncForView exposes the effective MMR resolver for a position copy —
// query views (liq price display) resolve through the same per-symbol tier
// math as liquidation.
func (e *Engine) MMRFuncForView(p perpstate.Position) (perpstate.MMRFunc, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if f := e.mmrForLocked(&p); f != nil {
		return f, true
	}
	return nil, false
}

// ApplyFill applies a trade fill to the (user, symbol, idx) leg and routes
// the cash effects between wallet and position margin (ADR-0068 §4):
//   - open/increase: initial margin is drawn from Reserved (held at order
//     time), falling back to Available if under-reserved.
//   - reduce/close:  released margin returns to Available.
//   - realized PnL and fee settle in Available.
//
// leverage seeds a fresh position. excess is the qty a hedge leg clamped off
// (never-flip, ADR-0077 §2) — the caller must surface it as
// REDUCE_ONLY_INVARIANT_BREACH (ADR-0081 §2), not drop it.
func (e *Engine) ApplyFill(user uint64, symbol string, idx uint8, leverage dec.Decimal, f perpstate.Fill) (perpstate.FillResult, dec.Decimal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.legLocked(user, symbol, idx)
	if p.Leverage.Sign() == 0 {
		p.Leverage = leverage
	}
	sizeBefore, sideBefore := p.Size, p.Side
	res, excess := applyFillByIdx(p, f)
	e.stampRiskVersionLocked(p, sizeBefore, sideBefore)
	p.Version++
	e.routeCashLocked(user, res)
	e.syncIndexesLocked(user, symbol, p)
	return res, excess
}

// ApplyFillWithSeq applies a fill guarded by the per-(user, symbol, idx)
// match_seq watermark (ADR-0068 invariant #3): a fill whose seq <= the stored
// watermark is a replay and is skipped (applied=false); on apply the
// watermark advances. seq == 0 bypasses the guard (in-process tests / the
// second leg of a self-trade). Guard + apply + advance + cash routing all
// happen under one lock — no TOCTOU between checking the watermark and
// mutating the position. excess: see ApplyFill.
func (e *Engine) ApplyFillWithSeq(user uint64, symbol string, idx uint8, leverage dec.Decimal, seq uint64, f perpstate.Fill) (perpstate.FillResult, dec.Decimal, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.legLocked(user, symbol, idx)
	if seq != 0 && seq <= p.LastMatchSeq {
		return perpstate.FillResult{}, zero, false
	}
	if p.Leverage.Sign() == 0 {
		p.Leverage = leverage
	}
	sizeBefore, sideBefore := p.Size, p.Side
	res, excess := applyFillByIdx(p, f)
	e.stampRiskVersionLocked(p, sizeBefore, sideBefore)
	if seq != 0 {
		p.LastMatchSeq = seq
	}
	p.Version++
	e.routeCashLocked(user, res)
	e.syncIndexesLocked(user, symbol, p)
	return res, excess, true
}

// stampRiskVersionLocked pins the position to the symbol's CURRENT effective
// risk version when the fill created new exposure — open, size increase, or
// flip (ADR-0075 §3: staged tightenings apply to new exposure only; the pin
// is what existing exposure keeps evaluating under). Reduces and closes keep
// the pin. Caller holds e.mu and must call this BEFORE syncIndexesLocked so
// the liq-price index is computed under the new pin.
func (e *Engine) stampRiskVersionLocked(p *perpstate.Position, sizeBefore dec.Decimal, sideBefore perpstate.Side) {
	if e.riskResolver == nil {
		return
	}
	increased := p.Size.Cmp(sizeBefore) > 0 || (p.Side != sideBefore && !p.IsFlat())
	if !increased {
		return
	}
	if _, ver, ok := e.riskResolver(p.Symbol, 0); ok {
		p.RiskConfigVersion = ver
	}
}

// routeCashLocked moves a fill's cash effects between wallet and position
// margin. Caller holds e.mu. Flat positions are retained (size 0) so their
// match_seq watermark + realized history survive; queries filter them out.
func (e *Engine) routeCashLocked(user uint64, res perpstate.FillResult) {
	w := e.walletLocked(user)
	if res.MarginAdded.Sign() > 0 {
		// The normal path consumes the reservation taken at PlaceOrder. The
		// Available fallback is deliberate: old fixtures and some internal
		// recovery paths can apply fills without a matching reservation, and
		// making the position/margin state authoritative is safer than dropping
		// a fill after Match has executed it.
		fromReserved := dec.Min(res.MarginAdded, w.Reserved)
		w.Reserved = w.Reserved.Sub(fromReserved)
		if rem := res.MarginAdded.Sub(fromReserved); rem.Sign() > 0 {
			w.Available = w.Available.Sub(rem)
		}
	}
	if res.MarginReleased.Sign() > 0 {
		w.Available = w.Available.Add(res.MarginReleased)
	}
	w.Available = w.Available.Add(res.Realized).Sub(res.Fee)
}

// ApplyFunding settles one funding interval against every live leg of
// (user, symbol) at the latest mark (ADR-0068 §7). Returns the summed signed
// delta (negative = the user paid) — per-leg amounts are not netted in the
// journal path (SettleFundingUser); this legacy entry point only reports the
// wallet-visible total. Isolated funding lands in position margin; cross
// funding settles in the wallet free balance (ADR-0074). No-op (zero) when
// no leg is live.
func (e *Engine) ApplyFunding(user uint64, symbol string, rate dec.Decimal) dec.Decimal {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := zero
	for _, p := range e.legsLocked(user, symbol) {
		if p.IsFlat() {
			continue
		}
		delta := p.ApplyFunding(e.marks[symbol], rate)
		e.routeFundingLocked(user, p, delta)
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
		total = total.Add(delta)
	}
	return total
}

// routeFundingLocked credits/debits a cross position's funding into the
// wallet free balance (perpstate leaves cross Margin untouched). Isolated
// funding already landed in position margin. Caller holds e.mu.
func (e *Engine) routeFundingLocked(user uint64, p *perpstate.Position, delta dec.Decimal) {
	if p.Mode != perpstate.MarginCross || delta.Sign() == 0 {
		return
	}
	w := e.walletLocked(user)
	w.Available = w.Available.Add(delta)
}

// FundingResult is one position's funding settlement outcome.
type FundingResult struct {
	UserID   uint64
	Symbol   string
	Payment  dec.Decimal        // signed margin delta (negative = position paid)
	Position perpstate.Position // post-settlement copy (for the journal)
}

// SettleFunding applies one funding round to every non-flat leg in symbol at
// the current mark (ADR-0068 §7; per-leg, no long/short netting — ADR-0077
// §5). The per-leg funding_round_seen watermark makes it idempotent: a
// round_id <= the watermark is skipped (replay / restart safe, ADR-0068
// invariant #3). round_id is the funding boundary's unix seconds (monotonic
// per symbol). Returns the per-leg results sorted by (user, idx) for
// deterministic journaling.
func (e *Engine) SettleFunding(symbol string, roundID int64, rate dec.Decimal) []FundingResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []FundingResult
	for user, bySym := range e.positions {
		out = e.settleFundingLegsLocked(out, user, symbol, bySym[symbol], roundID, rate)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UserID != out[j].UserID {
			return out[i].UserID < out[j].UserID
		}
		return out[i].Position.PositionIdx < out[j].Position.PositionIdx
	})
	return out
}

// settleFundingLegsLocked settles one round against every live leg of one
// (user, symbol), appending per-leg results. Caller holds e.mu.
func (e *Engine) settleFundingLegsLocked(out []FundingResult, user uint64, symbol string, sp *symbolPositions, roundID int64, rate dec.Decimal) []FundingResult {
	if sp == nil {
		return out
	}
	mark := e.marks[symbol]
	for _, p := range sp.legs {
		if p == nil || p.IsFlat() {
			continue
		}
		if roundID <= p.FundingRoundSeen {
			continue // already settled this round
		}
		delta := p.ApplyFunding(mark, rate)
		e.routeFundingLocked(user, p, delta)
		p.FundingRoundSeen = roundID
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
		out = append(out, FundingResult{UserID: user, Symbol: symbol, Payment: delta, Position: *p})
	}
	return out
}

// UsersWithPosition returns the users holding any non-flat leg in symbol,
// sorted. The service fans funding settlement out across these users, each
// under its own sequencer (ADR-0068 invariant #1), instead of mutating every
// position in one bulk pass — so a user's funding and fills stay totally
// ordered (the funding amount depends on size, so it must not interleave with
// a concurrent fill).
func (e *Engine) UsersWithPosition(symbol string) []uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out []uint64
	for user, bySym := range e.positions {
		for _, p := range bySym[symbol].liveLegs(nil) {
			if !p.IsFlat() {
				out = append(out, user)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// SettleFundingUser settles one funding round against every live leg of
// (user, symbol) at the current mark, guarded by the per-leg
// funding_round_seen watermark (ADR-0068 invariant #3: roundID <= seen is a
// replay and is skipped). Both hedge legs settle in this single locked step
// (ADR-0077 §5) so a fill cannot interleave between them. Returns one result
// per settled leg (empty = nothing applied). The caller MUST run this inside
// the user's sequencer (invariant #1).
func (e *Engine) SettleFundingUser(user uint64, symbol string, roundID int64, rate dec.Decimal) []FundingResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.settleFundingLegsLocked(nil, user, symbol, e.symPeekLocked(user, symbol), roundID, rate)
}

// AddInsurance adjusts the local insurance cache. In a single-instance
// deployment this cache is equivalent to the global fund; in an ADR-0071
// sharded deployment it is deliberately non-authoritative and exists only so
// legacy tests / local fallback flows can fold the same InsuranceDelta that the
// perp-risk coordinator consumes from perp-journal.
func (e *Engine) AddInsurance(symbol string, delta dec.Decimal) dec.Decimal {
	e.mu.Lock()
	defer e.mu.Unlock()
	cur := e.insurance[symbol]
	cur = cur.Add(delta)
	e.insurance[symbol] = cur
	return cur
}

// InsuranceFund returns the local insurance cache. Callers must not use this as
// a global deficit signal when an external perp-risk coordinator is enabled
// (ADR-0071 invariant #12/#15); the service layer owns that deployment guard.
func (e *Engine) InsuranceFund(symbol string) dec.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.insurance[symbol]
}

// LiquidationCandidate is a position leg that breached maintenance margin and
// is up for liquidation (ADR-0068 §8; per-leg in hedge mode, ADR-0077 §4).
type LiquidationCandidate struct {
	UserID          uint64
	Symbol          string
	PositionIdx     uint8
	Side            perpstate.Side
	Size            dec.Decimal
	Mark            dec.Decimal
	LiqPrice        dec.Decimal
	BankruptcyPrice dec.Decimal // where the reduce_only liquidation order is placed
	MaintMarginRate dec.Decimal
	PositionVersion uint64
}

// LiquidatablePositions returns the ISOLATED positions whose precomputed
// liq_price has been crossed by the current mark (ADR-0072). Each position's
// MMR resolves through its effective tier (RiskID-aware, ADR-0074 §9). Cross
// positions never appear here — their trigger is the account pool, evaluated
// via CrossPoolCheck (ADR-0074 §4 rule #6). The index is only a candidate
// accelerator: every returned entry is still rechecked through CollateralPool
// under the read lock so future false positives remain harmless.
func (e *Engine) LiquidatablePositions(symbol string) []LiquidationCandidate {
	e.mu.RLock()
	defer e.mu.RUnlock()
	mark := e.marks[symbol]
	// Per-position MMR resolution below handles catalog vs legacy; this is
	// only the cheap short-circuit for a fully unconfigured engine.
	if mark.Sign() <= 0 || !e.riskConfiguredLocked() {
		return nil
	}
	var out []LiquidationCandidate
	for _, entry := range e.liqIndex.crossed(symbol, mark) {
		p := e.legPeekLocked(entry.userID, symbol, entry.positionIdx)
		if cand, ok := e.liquidationCandidateLocked(entry.userID, symbol, p, mark); ok {
			out = append(out, cand)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UserID != out[j].UserID {
			return out[i].UserID < out[j].UserID
		}
		return out[i].PositionIdx < out[j].PositionIdx
	})
	return out
}

func (e *Engine) liquidationCandidateLocked(user uint64, symbol string, p *perpstate.Position, mark dec.Decimal) (LiquidationCandidate, bool) {
	if p == nil || p.IsFlat() || p.Mode == perpstate.MarginCross {
		return LiquidationCandidate{}, false
	}
	mmrOf := e.mmrForLocked(p)
	if mmrOf == nil {
		return LiquidationCandidate{}, false
	}
	marks := map[string]dec.Decimal{symbol: mark}
	pool := perpstate.Isolated(p)
	if !pool.Liquidatable(marks, mmrOf) {
		return LiquidationCandidate{}, false
	}
	h := pool.Eval(marks)
	return LiquidationCandidate{
		UserID: user, Symbol: symbol, PositionIdx: p.PositionIdx, Side: p.Side, Size: p.Size,
		Mark: mark, LiqPrice: p.LiqPrice(mmrOf), BankruptcyPrice: p.BankruptcyPrice(),
		MaintMarginRate: mmrOf(h.Notional), PositionVersion: p.Version,
	}, true
}

// LiquidationCheck re-evaluates a single (user, symbol, idx) leg against the
// maintenance margin rate under the lock, returning the candidate when it still
// breaches. The service calls this inside the user's sequencer to re-verify
// before acting (the scan that found it ran lock-free and the position may have
// moved since — TOCTOU guard, ADR-0068 invariant #1).
func (e *Engine) LiquidationCheck(user uint64, symbol string, idx uint8) (LiquidationCandidate, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.liquidationCandidateLocked(user, symbol, e.legPeekLocked(user, symbol, idx), e.marks[symbol])
}

// ReduceToTarget computes ADR-0070's partial-liquidation quantity for a single
// isolated leg using a locked snapshot. The service still re-checks the
// candidate inside the user's sequencer; this helper only centralizes the pure
// pool math so callers do not bypass the CollateralPool boundary.
func (e *Engine) ReduceToTarget(user uint64, symbol string, idx uint8, buffer dec.Decimal) dec.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p := e.legPeekLocked(user, symbol, idx)
	if p == nil || p.IsFlat() || p.Mode == perpstate.MarginCross {
		return zero
	}
	return perpstate.ReduceToTarget(perpstate.Isolated(p),
		map[string]dec.Decimal{symbol: e.marks[symbol]}, e.mmrForLocked(p), buffer)
}

// ApplyLiquidationFill applies one fill of the bankruptcy reduce_only order to a
// position being liquidated (ADR-0068 §8). Unlike ApplyFill it routes the freed
// equity to the symbol's insurance fund instead of the user's wallet — in
// isolated margin the user forfeits the position margin on liquidation. The
// signed insurance delta is (margin released + realized PnL at the fill): a
// surplus (filled better than bankruptcy) grows the fund, a deficit (filled
// worse) draws it down. Applying per fill makes partial liquidation fills
// correct — the sum across fills equals the single-shot ForceClose equity.
// Guarded by the same per-(user, symbol) match_seq watermark as ApplyFillWithSeq
// (replay → applied=false). Caller runs inside the user's sequencer.
func (e *Engine) ApplyLiquidationFill(user uint64, symbol string, idx uint8, seq uint64, f perpstate.Fill) (res perpstate.FillResult, insuranceDelta, excess dec.Decimal, applied bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.legLocked(user, symbol, idx)
	if seq != 0 && seq <= p.LastMatchSeq {
		return perpstate.FillResult{}, zero, zero, false
	}
	// A hedge leg clamps (never flips); the bankruptcy order is sized to the
	// leg at dispatch, so excess only appears when the leg shrank in between
	// (e.g. an ADL task landed first). The caller surfaces it as an invariant
	// breach (ADR-0081 §2) — it is never absorbed as a flip.
	res, excess = applyFillByIdx(p, f)
	if seq != 0 {
		p.LastMatchSeq = seq
	}
	p.Version++
	// Equity freed by this reduce goes to insurance, not the wallet.
	insuranceDelta = res.MarginReleased.Add(res.Realized).Sub(res.Fee)
	e.insurance[symbol] = e.insurance[symbol].Add(insuranceDelta)
	e.syncIndexesLocked(user, symbol, p)
	return res, insuranceDelta, excess, true
}

// ApplyPartialLiquidationFill applies a forced reduce that leaves the surviving
// isolated position healthier instead of withdrawing released margin to the
// user's wallet. That accounting choice is the core ADR-0070 tradeoff: partial
// liquidation should shrink notional and preserve residual equity for the
// remaining position; only the configured liquidation fee is moved to insurance.
func (e *Engine) ApplyPartialLiquidationFill(user uint64, symbol string, idx uint8, seq uint64, f perpstate.Fill, liqFeeRate dec.Decimal) (res perpstate.FillResult, insuranceDelta dec.Decimal, applied bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.legLocked(user, symbol, idx)
	if p.IsFlat() || f.Side != p.Side.Opposite() {
		return perpstate.FillResult{}, zero, false
	}
	if seq != 0 && seq <= p.LastMatchSeq {
		return perpstate.FillResult{}, zero, false
	}
	// closeQty is clamped to the leg, so reducePositionKeepingEquity never
	// overshoots — partial liquidation cannot flip a net position or a leg.
	closeQty := dec.Min(f.Qty, p.Size)
	res = reducePositionKeepingEquity(p, f.Price, closeQty, liqFeeRate)
	if seq != 0 {
		p.LastMatchSeq = seq
	}
	p.Version++
	insuranceDelta = res.Fee
	e.insurance[symbol] = e.insurance[symbol].Add(insuranceDelta)
	e.syncIndexesLocked(user, symbol, p)
	return res, insuranceDelta, true
}

// BackstopTakeover closes qty internally at price and records the other side on
// the configured system account. It is intentionally engine-local: once the
// service escalates here, Match liquidity is no longer part of correctness.
func (e *Engine) BackstopTakeover(user uint64, symbol string, idx uint8, qty, price dec.Decimal, backstopUser uint64, partial bool, liqFeeRate dec.Decimal) (res perpstate.FillResult, insuranceDelta dec.Decimal, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.legPeekLocked(user, symbol, idx)
	if p == nil || p.IsFlat() {
		return perpstate.FillResult{}, zero, false
	}
	originalSide := p.Side
	closeQty := dec.Min(qty, p.Size)
	fillSide := p.Side.Opposite()
	if partial && closeQty.Cmp(p.Size) < 0 {
		// Partial backstop follows the same accounting as partial Match fills:
		// the surviving position keeps its released equity, and insurance only
		// receives the liquidation fee. Full takeover wipes the position and
		// sends the final equity surplus/deficit to insurance.
		res = reducePositionKeepingEquity(p, price, closeQty, liqFeeRate)
		insuranceDelta = res.Fee
	} else {
		// closeQty is clamped to the leg, so this exact-size close cannot
		// flip a net position or a leg.
		res, _ = applyFillByIdx(p, perpstate.Fill{Side: fillSide, Price: price, Qty: closeQty})
		insuranceDelta = res.MarginReleased.Add(res.Realized).Sub(res.Fee)
	}
	p.Version++
	e.syncIndexesLocked(user, symbol, p)
	e.insurance[symbol] = e.insurance[symbol].Add(insuranceDelta)
	if closeQty.Sign() > 0 {
		e.applyBackstopInventoryLocked(backstopUser, symbol, originalSide, price, closeQty)
	}
	return res, insuranceDelta, true
}

// ADLCandidate is a profitable opposite-side position leg that can absorb
// taken-over inventory (per-leg ranking, ADR-0077 §4). SacrificePerQty is
// retained for deterministic ranking and stale-profitability checks, but
// ADR-0073 no longer treats it as a direct insurance-fund credit.
type ADLCandidate struct {
	UserID          uint64
	Symbol          string
	PositionIdx     uint8
	Side            perpstate.Side
	Size            dec.Decimal
	Score           dec.Decimal
	SacrificePerQty dec.Decimal
	LastMatchSeq    uint64
	PositionVersion uint64
}

// SelectAdlCandidates ranks profitable opposite-side positions by the Binance-
// style score used in ADR-0070: unrealized profit rate times effective leverage.
// Candidates that would not give up mark-to-ADL-price profit are skipped because
// they are not valid profitable counterparties for consuming takeover inventory.
func (e *Engine) SelectAdlCandidates(symbol string, liquidatedSide perpstate.Side, adlPrice dec.Decimal, excludeUser uint64) []ADLCandidate {
	return e.selectAdlCandidates(symbol, liquidatedSide.Opposite(), adlPrice, excludeUser)
}

// SelectAnyAdlCandidates is the ADR-0071 coordinator-facing ranking query. The
// coordinator may not know the liquidated side from older journal records, so
// the shard reports every profitable position that would surrender value at the
// requested ADL price. The eventual task is still version-checked before
// mutation, so a stale candidate report cannot directly move a user position.
func (e *Engine) SelectAnyAdlCandidates(symbol string, adlPrice dec.Decimal, excludeUser uint64) []ADLCandidate {
	return e.selectAdlCandidates(symbol, 0, adlPrice, excludeUser)
}

func (e *Engine) selectAdlCandidates(symbol string, wantSide perpstate.Side, adlPrice dec.Decimal, excludeUser uint64) []ADLCandidate {
	e.mu.RLock()
	defer e.mu.RUnlock()
	mark := e.marks[symbol]
	if mark.Sign() <= 0 {
		return nil
	}
	var out []ADLCandidate
	for user, bySym := range e.positions {
		if user == excludeUser {
			continue
		}
		for _, p := range bySym[symbol].liveLegs(nil) {
			if p.IsFlat() || (wantSide != 0 && p.Side != wantSide) {
				continue
			}
			// Cross positions are exempt from ADL v1 (their equity is pool-level;
			// the margin-based score below has no meaning for them). The
			// Margin<=0 guard already excludes them — the mode check makes the
			// rule explicit rather than incidental.
			if p.Mode == perpstate.MarginCross {
				continue
			}
			upnl := p.UnrealizedPnL(mark)
			if upnl.Sign() <= 0 || p.Margin.Sign() <= 0 {
				continue
			}
			sacrificePerQty := adlSacrificePerQty(p.Side, mark, adlPrice)
			if sacrificePerQty.Sign() <= 0 {
				continue
			}
			equity := p.Margin.Add(upnl)
			if equity.Sign() <= 0 {
				continue
			}
			profitRate := upnl.Div(p.Margin)
			effLev := p.Notional(mark).Div(equity)
			out = append(out, ADLCandidate{
				UserID: user, Symbol: symbol, PositionIdx: p.PositionIdx, Side: p.Side, Size: p.Size,
				Score: profitRate.Mul(effLev), SacrificePerQty: sacrificePerQty,
				LastMatchSeq: p.LastMatchSeq, PositionVersion: p.Version,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if c := out[i].Score.Cmp(out[j].Score); c != 0 {
			return c > 0
		}
		if out[i].UserID != out[j].UserID {
			return out[i].UserID < out[j].UserID
		}
		return out[i].PositionIdx < out[j].PositionIdx
	})
	return out
}

// ApplyAdlClose force-closes a profitable counterparty leg at adlPrice. The
// user receives normal close cash at that price. The returned factQty is the
// only quantity a RiskPool coordinator may deduct from a TakenOverLot; ADL
// itself does not credit the insurance fund (ADR-0073).
func (e *Engine) ApplyAdlClose(user uint64, symbol string, idx uint8, qty, adlPrice dec.Decimal, adlRound uint64) (res perpstate.FillResult, factQty dec.Decimal, applied bool) {
	return e.ApplyAdlCloseGuarded(user, symbol, idx, qty, adlPrice, 0, 0, 0, adlRound, false)
}

// ApplyAdlCloseGuarded executes an ADR-0071 coordinator task against one
// (user, symbol, idx) leg (ADR-0077 §4: the task targets a leg, never the
// user-symbol aggregate). expectedSide, expectedPosSeq, and expectedVersion
// are the shard read-view stamps the coordinator observed. Checking all three
// is intentional: LastMatchSeq catches fills from Match, while
// Position.Version also catches local mutations such as funding or an earlier
// ADL that can leave LastMatchSeq unchanged.
func (e *Engine) ApplyAdlCloseGuarded(user uint64, symbol string, idx uint8, qty, adlPrice dec.Decimal, expectedSide perpstate.Side, expectedPosSeq, expectedVersion, adlRound uint64, enforceObserved bool) (res perpstate.FillResult, factQty dec.Decimal, applied bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.legPeekLocked(user, symbol, idx)
	if p == nil || p.IsFlat() || p.Mode == perpstate.MarginCross {
		return perpstate.FillResult{}, zero, false
	}
	if enforceObserved {
		if expectedSide != 0 && p.Side != expectedSide {
			return perpstate.FillResult{}, zero, false
		}
		if p.LastMatchSeq != expectedPosSeq || p.Version != expectedVersion {
			return perpstate.FillResult{}, zero, false
		}
	}
	if adlRound != 0 && adlRound <= p.LastAdlRound {
		return perpstate.FillResult{}, zero, false
	}
	mark := e.marks[symbol]
	sacrificePerQty := adlSacrificePerQty(p.Side, mark, adlPrice)
	if mark.Sign() <= 0 || sacrificePerQty.Sign() <= 0 || p.UnrealizedPnL(mark).Sign() <= 0 {
		return perpstate.FillResult{}, zero, false
	}
	// closeQty is clamped to the leg, so this close cannot flip a net
	// position or a leg.
	closeQty := dec.Min(qty, p.Size)
	res, _ = applyFillByIdx(p, perpstate.Fill{Side: p.Side.Opposite(), Price: adlPrice, Qty: closeQty})
	if adlRound != 0 {
		p.LastAdlRound = adlRound
	}
	p.Version++
	e.routeCashLocked(user, res)
	e.syncIndexesLocked(user, symbol, p)
	return res, closeQty, true
}

func reducePositionKeepingEquity(p *perpstate.Position, price, closeQty, liqFeeRate dec.Decimal) perpstate.FillResult {
	res := perpstate.FillResult{Fee: price.Mul(closeQty).Mul(liqFeeRate)}
	if closeQty.Sign() <= 0 || p.IsFlat() {
		return res
	}
	if closeQty.Cmp(p.Size) >= 0 {
		return p.ApplyFill(perpstate.Fill{Side: p.Side.Opposite(), Price: price, Qty: closeQty, Fee: res.Fee})
	}
	// Partial liquidation is not a user withdrawal. We realize the closed
	// slice's PnL into the remaining isolated margin and report no
	// MarginReleased, so the wallet cannot reclaim collateral while the
	// position is still in distress. The only cash extracted is the configured
	// liquidation fee, which the caller routes to insurance.
	if p.Side == perpstate.SideBuy {
		res.Realized = price.Sub(p.Entry).Mul(closeQty)
	} else {
		res.Realized = p.Entry.Sub(price).Mul(closeQty)
	}
	p.Size = p.Size.Sub(closeQty)
	p.Margin = p.Margin.Add(res.Realized).Sub(res.Fee)
	p.Realized = p.Realized.Add(res.Realized)
	return res
}

func adlSacrificePerQty(side perpstate.Side, mark, adlPrice dec.Decimal) dec.Decimal {
	switch side {
	case perpstate.SideBuy:
		return mark.Sub(adlPrice)
	case perpstate.SideSell:
		return adlPrice.Sub(mark)
	default:
		return zero
	}
}

func (e *Engine) applyBackstopInventoryLocked(user uint64, symbol string, side perpstate.Side, price, qty dec.Decimal) {
	// Backstop inventory is always net-keyed (idx 0): the system account does
	// not participate in hedge mode (ADR-0077 §4).
	p := e.legLocked(user, symbol, perpstate.IdxNet)
	// Backstop inventory is a system-risk ledger, not user margin. We therefore
	// mutate size/entry directly instead of routing IM through a wallet reserve.
	if p.IsFlat() || p.Side == side {
		newSize := p.Size.Add(qty)
		if newSize.Sign() > 0 {
			p.Entry = p.Entry.Mul(p.Size).Add(price.Mul(qty)).Div(newSize)
		}
		p.Size = newSize
		p.Side = side
		p.Mode = perpstate.MarginIsolated
		p.Version++
		e.syncIndexesLocked(user, symbol, p)
		return
	}
	closeQty := dec.Min(qty, p.Size)
	if p.Side == perpstate.SideBuy {
		p.Realized = p.Realized.Add(price.Sub(p.Entry).Mul(closeQty))
	} else {
		p.Realized = p.Realized.Add(p.Entry.Sub(price).Mul(closeQty))
	}
	p.Size = p.Size.Sub(closeQty)
	remaining := qty.Sub(closeQty)
	if p.Size.Sign() == 0 {
		p.Entry = zero
		if remaining.Sign() > 0 {
			p.Side = side
			p.Size = remaining
			p.Entry = price
		} else {
			p.Side = 0
		}
	}
	p.Version++
	e.syncIndexesLocked(user, symbol, p)
}

// ForceClose liquidates a position fully at fillPrice (the price the
// bankruptcy reduce_only order filled at, ADR-0068 §8). The position is
// wiped and its equity at fillPrice (margin + realized) settles into the
// symbol's insurance fund: a surplus (filled better than bankruptcy) adds to
// the fund, a deficit (filled past bankruptcy) draws it down. Isolated — only
// this position's margin is at risk; the wallet's free balance is untouched.
// Returns the signed insurance delta; ok=false when there's nothing to close.
//
// The order cancellation + Match dispatch of the bankruptcy order happen in
// the service layer; this is the settlement once the liquidation fill is
// known.
func (e *Engine) ForceClose(user uint64, symbol string, idx uint8, fillPrice dec.Decimal) (insuranceDelta dec.Decimal, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.legPeekLocked(user, symbol, idx)
	if p == nil || p.IsFlat() {
		return zero, false
	}
	var realized dec.Decimal
	if p.Side == perpstate.SideBuy {
		realized = fillPrice.Sub(p.Entry).Mul(p.Size)
	} else {
		realized = p.Entry.Sub(fillPrice).Mul(p.Size)
	}
	equity := p.Margin.Add(realized)
	p.Realized = p.Realized.Add(realized)
	p.Size = zero
	p.Entry = zero
	p.Margin = zero
	p.Side = 0
	p.Version++
	e.insurance[symbol] = e.insurance[symbol].Add(equity)
	e.syncIndexesLocked(user, symbol, p)
	return equity, true
}

// syncIndexesLocked refreshes the derived views after a mutation of one leg:
// the isolated liq-price index (cross positions are removed, not indexed)
// and the cross / auto-add membership indexes. Membership is per
// (user, symbol) over ALL legs — recomputed from the container, not from the
// mutated leg alone, or closing one leg would wrongly drop a user whose other
// leg still qualifies. Caller holds e.mu.
func (e *Engine) syncIndexesLocked(user uint64, symbol string, p *perpstate.Position) {
	if e.liqIndex == nil {
		e.liqIndex = newLiqIndex()
	}
	e.liqIndex.upsert(user, symbol, p.PositionIdx, p, e.mmrForLocked(p))
	anyCross, anyAutoAdd := false, false
	for _, leg := range e.legsLocked(user, symbol) {
		if leg.IsFlat() {
			continue
		}
		if leg.Mode == perpstate.MarginCross {
			anyCross = true
		} else if leg.AutoAddMargin {
			anyAutoAdd = true
		}
	}
	setMembership(e.crossUsers, symbol, user, anyCross)
	setMembership(e.autoAdd, symbol, user, anyAutoAdd)
}

// setMembership adds/removes user from a symbol-keyed membership index.
func setMembership(idx map[string]map[uint64]struct{}, symbol string, user uint64, in bool) {
	byUser := idx[symbol]
	if in {
		if byUser == nil {
			byUser = map[uint64]struct{}{}
			idx[symbol] = byUser
		}
		byUser[user] = struct{}{}
		return
	}
	if byUser != nil {
		delete(byUser, user)
		if len(byUser) == 0 {
			delete(idx, symbol)
		}
	}
}

func (e *Engine) rebuildLiquidationIndexLocked() {
	if e.liqIndex == nil {
		e.liqIndex = newLiqIndex()
	}
	e.liqIndex.rebuild(e.positions, e.mmrForLocked)
	e.crossUsers = map[string]map[uint64]struct{}{}
	e.autoAdd = map[string]map[uint64]struct{}{}
	for user, bySym := range e.positions {
		for symbol, sp := range bySym {
			anyCross, anyAutoAdd := false, false
			for _, p := range sp.liveLegs(nil) {
				if p.IsFlat() {
					continue
				}
				if p.Mode == perpstate.MarginCross {
					anyCross = true
				} else if p.AutoAddMargin {
					anyAutoAdd = true
				}
			}
			setMembership(e.crossUsers, symbol, user, anyCross)
			setMembership(e.autoAdd, symbol, user, anyAutoAdd)
		}
	}
}

// WalletOf returns a copy of the user's wallet.
func (e *Engine) WalletOf(user uint64) Wallet {
	e.mu.RLock()
	defer e.mu.RUnlock()
	w := e.wallets[user]
	if w == nil {
		return Wallet{Available: zero, Reserved: zero}
	}
	return *w
}

// PositionOf returns a copy of the (user, symbol, idx) leg and whether it
// exists (and is non-flat).
func (e *Engine) PositionOf(user uint64, symbol string, idx uint8) (perpstate.Position, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p := e.legPeekLocked(user, symbol, idx)
	if p == nil || p.IsFlat() {
		return perpstate.Position{}, false
	}
	return *p, true
}

// PositionRaw returns a copy of the stored leg, including a flat one (size 0,
// retained for its match_seq watermark). ok=false only when the leg was never
// created. Used to build post-change journal snapshots.
func (e *Engine) PositionRaw(user uint64, symbol string, idx uint8) (perpstate.Position, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p := e.legPeekLocked(user, symbol, idx)
	if p == nil {
		return perpstate.Position{}, false
	}
	return *p, true
}

// PositionsOf returns copies of all of a user's non-flat legs, sorted by
// (symbol, idx) for stable output.
func (e *Engine) PositionsOf(user uint64) []perpstate.Position {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bySym := e.positions[user]
	out := make([]perpstate.Position, 0, len(bySym))
	for _, sp := range bySym {
		for _, p := range sp.liveLegs(nil) {
			if !p.IsFlat() {
				out = append(out, *p)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].PositionIdx < out[j].PositionIdx
	})
	return out
}

// PositionModeOf returns the (user, symbol) position mode (ONE_WAY when no
// container exists — the default).
func (e *Engine) PositionModeOf(user uint64, symbol string) perpstate.PositionMode {
	e.mu.RLock()
	defer e.mu.RUnlock()
	sp := e.symPeekLocked(user, symbol)
	if sp == nil {
		return perpstate.PositionOneWay
	}
	return sp.mode
}
