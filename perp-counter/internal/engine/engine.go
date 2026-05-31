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

// Wallet is a user's USDT futures-margin balance.
type Wallet struct {
	Available dec.Decimal // free margin
	Reserved  dec.Decimal // initial margin held against open orders (ADR-0041)
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

// Engine is the in-memory perp account state.
type Engine struct {
	mu        sync.RWMutex
	wallets   map[uint64]*Wallet
	positions map[uint64]map[string]*perpstate.Position
	marks     map[string]dec.Decimal
	insurance map[string]dec.Decimal
	transfers map[string]TransferOutcome // transfer_id → outcome (AssetHolder idempotency, ADR-0057)

	// liqIndex is ADR-0072 derived state: it is rebuilt from positions after
	// restore and updated in the same write critical section as every position
	// mutation. liqMMR is configured by the service because risk tiers belong to
	// service config, while Engine owns the position mutation boundary.
	liqIndex *liqIndex
	liqMMR   perpstate.MMRFunc
}

// New returns an empty engine.
func New() *Engine {
	return &Engine{
		wallets:   map[uint64]*Wallet{},
		positions: map[uint64]map[string]*perpstate.Position{},
		marks:     map[string]dec.Decimal{},
		insurance: map[string]dec.Decimal{},
		transfers: map[string]TransferOutcome{},
		liqIndex:  newLiqIndex(),
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
func (e *Engine) TransferOut(user uint64, transferID string, amt dec.Decimal) (TransferOutcome, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if prev, ok := e.transfers[transferID]; ok {
		return prev, true
	}
	w := e.walletLocked(user)
	if w.Available.Cmp(amt) < 0 {
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
		w = &Wallet{Available: zero, Reserved: zero}
		e.wallets[user] = w
	}
	return w
}

func (e *Engine) positionLocked(user uint64, symbol string) *perpstate.Position {
	bySym := e.positions[user]
	if bySym == nil {
		bySym = map[string]*perpstate.Position{}
		e.positions[user] = bySym
	}
	p := bySym[symbol]
	if p == nil {
		p = &perpstate.Position{UserID: user, Symbol: symbol, Mode: perpstate.MarginIsolated,
			Size: zero, Entry: zero, Margin: zero, Leverage: zero, Realized: zero}
		bySym[symbol] = p
	}
	return p
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
// when available is insufficient.
func (e *Engine) Withdraw(user uint64, amt dec.Decimal) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	if w.Available.Cmp(amt) < 0 {
		return false
	}
	w.Available = w.Available.Sub(amt)
	return true
}

// Reserve holds initial margin for a new order (Available→Reserved). Returns
// false when available is insufficient — the perp pre-trade risk gate
// (ADR-0068 §4; the check spot deliberately skips).
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

// Release returns held initial margin to available (Reserved→Available) on
// cancel / reject. Clamped to what is actually reserved.
func (e *Engine) Release(user uint64, im dec.Decimal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	move := dec.Min(im, w.Reserved)
	w.Reserved = w.Reserved.Sub(move)
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

// SetLiquidationMMRFunc configures ADR-0072's derived liq-price index. The
// resolver is intentionally held by Engine after service startup so every
// position mutation can update the index without widening all mutation method
// signatures. Callers that change risk tiers must call this again to rebuild.
func (e *Engine) SetLiquidationMMRFunc(mmrOf perpstate.MMRFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.liqMMR = mmrOf
	e.rebuildLiquidationIndexLocked()
}

// ApplyFill applies a trade fill to (user, symbol)'s position and routes the
// cash effects between wallet and position margin (ADR-0068 §4):
//   - open/increase: initial margin is drawn from Reserved (held at order
//     time), falling back to Available if under-reserved.
//   - reduce/close:  released margin returns to Available.
//   - realized PnL and fee settle in Available.
//
// leverage seeds a fresh position. Returns the FillResult for journaling.
func (e *Engine) ApplyFill(user uint64, symbol string, leverage dec.Decimal, f perpstate.Fill) perpstate.FillResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.positionLocked(user, symbol)
	if p.Leverage.Sign() == 0 {
		p.Leverage = leverage
	}
	res := p.ApplyFill(f)
	p.Version++
	e.routeCashLocked(user, res)
	e.syncLiquidationIndexLocked(user, symbol, p)
	return res
}

// ApplyFillWithSeq applies a fill guarded by the per-(user, symbol) match_seq
// watermark (ADR-0068 invariant #3): a fill whose seq <= the stored watermark
// is a replay and is skipped (applied=false); on apply the watermark
// advances. seq == 0 bypasses the guard (in-process tests / legacy). Guard +
// apply + advance + cash routing all happen under one lock — no TOCTOU
// between checking the watermark and mutating the position.
func (e *Engine) ApplyFillWithSeq(user uint64, symbol string, leverage dec.Decimal, seq uint64, f perpstate.Fill) (perpstate.FillResult, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.positionLocked(user, symbol)
	if seq != 0 && seq <= p.LastMatchSeq {
		return perpstate.FillResult{}, false
	}
	if p.Leverage.Sign() == 0 {
		p.Leverage = leverage
	}
	res := p.ApplyFill(f)
	if seq != 0 {
		p.LastMatchSeq = seq
	}
	p.Version++
	e.routeCashLocked(user, res)
	e.syncLiquidationIndexLocked(user, symbol, p)
	return res, true
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

// ApplyFunding settles one funding interval against (user, symbol) at the
// latest mark (ADR-0068 §7). Returns the signed margin delta (negative =
// the position paid). No-op (zero) when the position is absent or flat.
func (e *Engine) ApplyFunding(user uint64, symbol string, rate dec.Decimal) dec.Decimal {
	e.mu.Lock()
	defer e.mu.Unlock()
	bySym := e.positions[user]
	if bySym == nil {
		return zero
	}
	p := bySym[symbol]
	if p == nil || p.IsFlat() {
		return zero
	}
	delta := p.ApplyFunding(e.marks[symbol], rate)
	p.Version++
	e.syncLiquidationIndexLocked(user, symbol, p)
	return delta
}

// FundingResult is one position's funding settlement outcome.
type FundingResult struct {
	UserID   uint64
	Symbol   string
	Payment  dec.Decimal        // signed margin delta (negative = position paid)
	Position perpstate.Position // post-settlement copy (for the journal)
}

// SettleFunding applies one funding round to every non-flat position in
// symbol at the current mark (ADR-0068 §7). The per-position
// funding_round_seen watermark makes it idempotent: a round_id <= the
// watermark is skipped (replay / restart safe, ADR-0068 invariant #3).
// round_id is the funding boundary's unix seconds (monotonic per symbol).
// Returns the per-position results sorted by user for deterministic journaling.
func (e *Engine) SettleFunding(symbol string, roundID int64, rate dec.Decimal) []FundingResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	mark := e.marks[symbol]
	var out []FundingResult
	for user, bySym := range e.positions {
		p := bySym[symbol]
		if p == nil || p.IsFlat() {
			continue
		}
		if roundID <= p.FundingRoundSeen {
			continue // already settled this round
		}
		delta := p.ApplyFunding(mark, rate)
		p.FundingRoundSeen = roundID
		p.Version++
		e.syncLiquidationIndexLocked(user, symbol, p)
		out = append(out, FundingResult{UserID: user, Symbol: symbol, Payment: delta, Position: *p})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out
}

// UsersWithPosition returns the users holding a non-flat position in symbol,
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
		if p := bySym[symbol]; p != nil && !p.IsFlat() {
			out = append(out, user)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// SettleFundingUser settles one funding round against (user, symbol) at the
// current mark, guarded by the per-position funding_round_seen watermark
// (ADR-0068 invariant #3: roundID <= seen is a replay and is skipped). Returns
// the result and whether it applied. The caller MUST run this inside the user's
// sequencer (invariant #1). Guard + apply + advance are one locked step.
func (e *Engine) SettleFundingUser(user uint64, symbol string, roundID int64, rate dec.Decimal) (FundingResult, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bySym := e.positions[user]
	if bySym == nil {
		return FundingResult{}, false
	}
	p := bySym[symbol]
	if p == nil || p.IsFlat() {
		return FundingResult{}, false
	}
	if roundID <= p.FundingRoundSeen {
		return FundingResult{}, false
	}
	delta := p.ApplyFunding(e.marks[symbol], rate)
	p.FundingRoundSeen = roundID
	p.Version++
	e.syncLiquidationIndexLocked(user, symbol, p)
	return FundingResult{UserID: user, Symbol: symbol, Payment: delta, Position: *p}, true
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

// LiquidationCandidate is a position that breached maintenance margin and is
// up for liquidation (ADR-0068 §8).
type LiquidationCandidate struct {
	UserID          uint64
	Symbol          string
	Side            perpstate.Side
	Size            dec.Decimal
	Mark            dec.Decimal
	LiqPrice        dec.Decimal
	BankruptcyPrice dec.Decimal // where the reduce_only liquidation order is placed
	MaintMarginRate dec.Decimal
	PositionVersion uint64
}

// LiquidatablePositions returns the positions whose precomputed liq_price has
// been crossed by the current mark (ADR-0072). The index is only a candidate
// accelerator: every returned entry is still rechecked through CollateralPool
// under the read lock so the public behavior stays identical to the old
// full-scan path and future false positives remain harmless.
func (e *Engine) LiquidatablePositions(symbol string, mmrOf perpstate.MMRFunc) []LiquidationCandidate {
	e.mu.RLock()
	defer e.mu.RUnlock()
	mark := e.marks[symbol]
	if mark.Sign() <= 0 {
		return nil
	}
	if mmrOf == nil {
		mmrOf = e.liqMMR
	}
	if mmrOf == nil {
		return nil
	}
	if e.liqMMR != nil && e.liqIndex != nil {
		return e.liquidatablePositionsFromIndexLocked(symbol, mark, mmrOf)
	}
	return e.liquidatablePositionsFullScanLocked(symbol, mark, mmrOf)
}

func (e *Engine) liquidatablePositionsFromIndexLocked(symbol string, mark dec.Decimal, mmrOf perpstate.MMRFunc) []LiquidationCandidate {
	var out []LiquidationCandidate
	for _, entry := range e.liqIndex.crossed(symbol, mark) {
		bySym := e.positions[entry.userID]
		if bySym == nil {
			continue
		}
		if cand, ok := e.liquidationCandidateLocked(entry.userID, symbol, bySym[symbol], mark, mmrOf); ok {
			out = append(out, cand)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out
}

func (e *Engine) liquidatablePositionsFullScanLocked(symbol string, mark dec.Decimal, mmrOf perpstate.MMRFunc) []LiquidationCandidate {
	var out []LiquidationCandidate
	for user, bySym := range e.positions {
		if cand, ok := e.liquidationCandidateLocked(user, symbol, bySym[symbol], mark, mmrOf); ok {
			out = append(out, cand)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out
}

func (e *Engine) liquidationCandidateLocked(user uint64, symbol string, p *perpstate.Position, mark dec.Decimal, mmrOf perpstate.MMRFunc) (LiquidationCandidate, bool) {
	if p == nil || p.IsFlat() {
		return LiquidationCandidate{}, false
	}
	marks := map[string]dec.Decimal{symbol: mark}
	pool := perpstate.Isolated(p)
	if !pool.Liquidatable(marks, mmrOf) {
		return LiquidationCandidate{}, false
	}
	h := pool.Eval(marks)
	return LiquidationCandidate{
		UserID: user, Symbol: symbol, Side: p.Side, Size: p.Size,
		Mark: mark, LiqPrice: p.LiqPrice(mmrOf), BankruptcyPrice: p.BankruptcyPrice(),
		MaintMarginRate: mmrOf(h.Notional), PositionVersion: p.Version,
	}, true
}

// LiquidationCheck re-evaluates a single (user, symbol) position against the
// maintenance margin rate under the lock, returning the candidate when it still
// breaches. The service calls this inside the user's sequencer to re-verify
// before acting (the scan that found it ran lock-free and the position may have
// moved since — TOCTOU guard, ADR-0068 invariant #1).
func (e *Engine) LiquidationCheck(user uint64, symbol string, mmrOf perpstate.MMRFunc) (LiquidationCandidate, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bySym := e.positions[user]
	if bySym == nil {
		return LiquidationCandidate{}, false
	}
	p := bySym[symbol]
	if p == nil || p.IsFlat() {
		return LiquidationCandidate{}, false
	}
	mark := e.marks[symbol]
	marks := map[string]dec.Decimal{symbol: mark}
	pool := perpstate.Isolated(p)
	if !pool.Liquidatable(marks, mmrOf) {
		return LiquidationCandidate{}, false
	}
	h := pool.Eval(marks)
	return LiquidationCandidate{
		UserID: user, Symbol: symbol, Side: p.Side, Size: p.Size,
		Mark: mark, LiqPrice: p.LiqPrice(mmrOf), BankruptcyPrice: p.BankruptcyPrice(),
		MaintMarginRate: mmrOf(h.Notional), PositionVersion: p.Version,
	}, true
}

// ReduceToTarget computes ADR-0070's partial-liquidation quantity for a single
// isolated position using a locked snapshot. The service still re-checks the
// candidate inside the user's sequencer; this helper only centralizes the pure
// pool math so callers do not bypass the CollateralPool boundary.
func (e *Engine) ReduceToTarget(user uint64, symbol string, mmrOf perpstate.MMRFunc, buffer dec.Decimal) dec.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bySym := e.positions[user]
	if bySym == nil {
		return zero
	}
	p := bySym[symbol]
	if p == nil || p.IsFlat() {
		return zero
	}
	return perpstate.ReduceToTarget(perpstate.Isolated(p),
		map[string]dec.Decimal{symbol: e.marks[symbol]}, mmrOf, buffer)
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
func (e *Engine) ApplyLiquidationFill(user uint64, symbol string, seq uint64, f perpstate.Fill) (res perpstate.FillResult, insuranceDelta dec.Decimal, applied bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.positionLocked(user, symbol)
	if seq != 0 && seq <= p.LastMatchSeq {
		return perpstate.FillResult{}, zero, false
	}
	res = p.ApplyFill(f)
	if seq != 0 {
		p.LastMatchSeq = seq
	}
	p.Version++
	// Equity freed by this reduce goes to insurance, not the wallet.
	insuranceDelta = res.MarginReleased.Add(res.Realized).Sub(res.Fee)
	e.insurance[symbol] = e.insurance[symbol].Add(insuranceDelta)
	e.syncLiquidationIndexLocked(user, symbol, p)
	return res, insuranceDelta, true
}

// ApplyPartialLiquidationFill applies a forced reduce that leaves the surviving
// isolated position healthier instead of withdrawing released margin to the
// user's wallet. That accounting choice is the core ADR-0070 tradeoff: partial
// liquidation should shrink notional and preserve residual equity for the
// remaining position; only the configured liquidation fee is moved to insurance.
func (e *Engine) ApplyPartialLiquidationFill(user uint64, symbol string, seq uint64, f perpstate.Fill, liqFeeRate dec.Decimal) (res perpstate.FillResult, insuranceDelta dec.Decimal, applied bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.positionLocked(user, symbol)
	if p.IsFlat() || f.Side != p.Side.Opposite() {
		return perpstate.FillResult{}, zero, false
	}
	if seq != 0 && seq <= p.LastMatchSeq {
		return perpstate.FillResult{}, zero, false
	}
	closeQty := dec.Min(f.Qty, p.Size)
	res = reducePositionKeepingEquity(p, f.Price, closeQty, liqFeeRate)
	if seq != 0 {
		p.LastMatchSeq = seq
	}
	p.Version++
	insuranceDelta = res.Fee
	e.insurance[symbol] = e.insurance[symbol].Add(insuranceDelta)
	e.syncLiquidationIndexLocked(user, symbol, p)
	return res, insuranceDelta, true
}

// BackstopTakeover closes qty internally at price and records the other side on
// the configured system account. It is intentionally engine-local: once the
// service escalates here, Match liquidity is no longer part of correctness.
func (e *Engine) BackstopTakeover(user uint64, symbol string, qty, price dec.Decimal, backstopUser uint64, partial bool, liqFeeRate dec.Decimal) (res perpstate.FillResult, insuranceDelta dec.Decimal, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bySym := e.positions[user]
	if bySym == nil {
		return perpstate.FillResult{}, zero, false
	}
	p := bySym[symbol]
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
		res = p.ApplyFill(perpstate.Fill{Side: fillSide, Price: price, Qty: closeQty})
		insuranceDelta = res.MarginReleased.Add(res.Realized).Sub(res.Fee)
	}
	p.Version++
	e.syncLiquidationIndexLocked(user, symbol, p)
	e.insurance[symbol] = e.insurance[symbol].Add(insuranceDelta)
	if closeQty.Sign() > 0 {
		e.applyBackstopInventoryLocked(backstopUser, symbol, originalSide, price, closeQty)
	}
	return res, insuranceDelta, true
}

// ADLCandidate is a profitable opposite-side position that can absorb
// taken-over inventory. SacrificePerQty is retained for deterministic ranking
// and stale-profitability checks, but ADR-0073 no longer treats it as a direct
// insurance-fund credit.
type ADLCandidate struct {
	UserID          uint64
	Symbol          string
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
		p := bySym[symbol]
		if p == nil || p.IsFlat() || (wantSide != 0 && p.Side != wantSide) {
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
			UserID: user, Symbol: symbol, Side: p.Side, Size: p.Size,
			Score: profitRate.Mul(effLev), SacrificePerQty: sacrificePerQty,
			LastMatchSeq: p.LastMatchSeq, PositionVersion: p.Version,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if c := out[i].Score.Cmp(out[j].Score); c != 0 {
			return c > 0
		}
		return out[i].UserID < out[j].UserID
	})
	return out
}

// ApplyAdlClose force-closes a profitable counterparty at adlPrice. The user
// receives normal close cash at that price. The returned factQty is the only
// quantity a RiskPool coordinator may deduct from a TakenOverLot; ADL itself
// does not credit the insurance fund (ADR-0073).
func (e *Engine) ApplyAdlClose(user uint64, symbol string, qty, adlPrice dec.Decimal, adlRound uint64) (res perpstate.FillResult, factQty dec.Decimal, applied bool) {
	return e.ApplyAdlCloseGuarded(user, symbol, qty, adlPrice, 0, 0, 0, adlRound, false)
}

// ApplyAdlCloseGuarded executes an ADR-0071 coordinator task. expectedSide,
// expectedPosSeq, and expectedVersion are the shard read-view stamps the
// coordinator observed. Checking all three is intentional: LastMatchSeq catches
// fills from Match, while Position.Version also catches local mutations such as
// funding or an earlier ADL that can leave LastMatchSeq unchanged.
func (e *Engine) ApplyAdlCloseGuarded(user uint64, symbol string, qty, adlPrice dec.Decimal, expectedSide perpstate.Side, expectedPosSeq, expectedVersion, adlRound uint64, enforceObserved bool) (res perpstate.FillResult, factQty dec.Decimal, applied bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bySym := e.positions[user]
	if bySym == nil {
		return perpstate.FillResult{}, zero, false
	}
	p := bySym[symbol]
	if p == nil || p.IsFlat() {
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
	closeQty := dec.Min(qty, p.Size)
	res = p.ApplyFill(perpstate.Fill{Side: p.Side.Opposite(), Price: adlPrice, Qty: closeQty})
	if adlRound != 0 {
		p.LastAdlRound = adlRound
	}
	p.Version++
	e.routeCashLocked(user, res)
	e.syncLiquidationIndexLocked(user, symbol, p)
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
	p := e.positionLocked(user, symbol)
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
		e.syncLiquidationIndexLocked(user, symbol, p)
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
	e.syncLiquidationIndexLocked(user, symbol, p)
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
func (e *Engine) ForceClose(user uint64, symbol string, fillPrice dec.Decimal) (insuranceDelta dec.Decimal, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bySym := e.positions[user]
	if bySym == nil {
		return zero, false
	}
	p := bySym[symbol]
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
	e.syncLiquidationIndexLocked(user, symbol, p)
	return equity, true
}

func (e *Engine) syncLiquidationIndexLocked(user uint64, symbol string, p *perpstate.Position) {
	if e.liqIndex == nil {
		e.liqIndex = newLiqIndex()
	}
	e.liqIndex.upsert(user, symbol, p, e.liqMMR)
}

func (e *Engine) rebuildLiquidationIndexLocked() {
	if e.liqIndex == nil {
		e.liqIndex = newLiqIndex()
	}
	e.liqIndex.rebuild(e.positions, e.liqMMR)
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

// PositionOf returns a copy of (user, symbol)'s position and whether it
// exists (and is non-flat).
func (e *Engine) PositionOf(user uint64, symbol string) (perpstate.Position, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bySym := e.positions[user]
	if bySym == nil {
		return perpstate.Position{}, false
	}
	p := bySym[symbol]
	if p == nil || p.IsFlat() {
		return perpstate.Position{}, false
	}
	return *p, true
}

// PositionRaw returns a copy of the stored position, including a flat one
// (size 0, retained for its match_seq watermark). ok=false only when the
// position was never created. Used to build post-change journal snapshots.
func (e *Engine) PositionRaw(user uint64, symbol string) (perpstate.Position, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bySym := e.positions[user]
	if bySym == nil {
		return perpstate.Position{}, false
	}
	p := bySym[symbol]
	if p == nil {
		return perpstate.Position{}, false
	}
	return *p, true
}

// PositionsOf returns copies of all of a user's non-flat positions, sorted
// by symbol for stable output.
func (e *Engine) PositionsOf(user uint64) []perpstate.Position {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bySym := e.positions[user]
	out := make([]perpstate.Position, 0, len(bySym))
	for _, p := range bySym {
		if !p.IsFlat() {
			out = append(out, *p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}
