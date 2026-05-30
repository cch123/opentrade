// Package engine is perp-counter's stateful container around pkg/perpstate
// (ADR-0068 A1). It holds per-user USDT futures-margin wallets,
// per-(user, symbol) positions, the latest mark per symbol, and per-symbol
// insurance funds, and routes the cash effects of fills / funding between
// the wallet and position margin.
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

// Engine is the in-memory perp account state.
type Engine struct {
	mu        sync.RWMutex
	wallets   map[string]*Wallet
	positions map[string]map[string]*perpstate.Position
	marks     map[string]dec.Decimal
	insurance map[string]dec.Decimal
}

// New returns an empty engine.
func New() *Engine {
	return &Engine{
		wallets:   map[string]*Wallet{},
		positions: map[string]map[string]*perpstate.Position{},
		marks:     map[string]dec.Decimal{},
		insurance: map[string]dec.Decimal{},
	}
}

func (e *Engine) walletLocked(user string) *Wallet {
	w := e.wallets[user]
	if w == nil {
		w = &Wallet{Available: zero, Reserved: zero}
		e.wallets[user] = w
	}
	return w
}

func (e *Engine) positionLocked(user, symbol string) *perpstate.Position {
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
func (e *Engine) Deposit(user string, amt dec.Decimal) dec.Decimal {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.walletLocked(user)
	w.Available = w.Available.Add(amt)
	return w.Available
}

// Withdraw debits free margin (futures→funding TransferOut). Returns false
// when available is insufficient.
func (e *Engine) Withdraw(user string, amt dec.Decimal) bool {
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
func (e *Engine) Reserve(user string, im dec.Decimal) bool {
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
func (e *Engine) Release(user string, im dec.Decimal) {
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

// ApplyFill applies a trade fill to (user, symbol)'s position and routes the
// cash effects between wallet and position margin (ADR-0068 §4):
//   - open/increase: initial margin is drawn from Reserved (held at order
//     time), falling back to Available if under-reserved.
//   - reduce/close:  released margin returns to Available.
//   - realized PnL and fee settle in Available.
//
// leverage seeds a fresh position. Returns the FillResult for journaling.
func (e *Engine) ApplyFill(user, symbol string, leverage dec.Decimal, f perpstate.Fill) perpstate.FillResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.positionLocked(user, symbol)
	if p.Leverage.Sign() == 0 {
		p.Leverage = leverage
	}
	res := p.ApplyFill(f)
	e.routeCashLocked(user, res)
	return res
}

// ApplyFillWithSeq applies a fill guarded by the per-(user, symbol) match_seq
// watermark (ADR-0068 invariant #3): a fill whose seq <= the stored watermark
// is a replay and is skipped (applied=false); on apply the watermark
// advances. seq == 0 bypasses the guard (in-process tests / legacy). Guard +
// apply + advance + cash routing all happen under one lock — no TOCTOU
// between checking the watermark and mutating the position.
func (e *Engine) ApplyFillWithSeq(user, symbol string, leverage dec.Decimal, seq uint64, f perpstate.Fill) (perpstate.FillResult, bool) {
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
	e.routeCashLocked(user, res)
	return res, true
}

// routeCashLocked moves a fill's cash effects between wallet and position
// margin. Caller holds e.mu. Flat positions are retained (size 0) so their
// match_seq watermark + realized history survive; queries filter them out.
func (e *Engine) routeCashLocked(user string, res perpstate.FillResult) {
	w := e.walletLocked(user)
	if res.MarginAdded.Sign() > 0 {
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
func (e *Engine) ApplyFunding(user, symbol string, rate dec.Decimal) dec.Decimal {
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
	return p.ApplyFunding(e.marks[symbol], rate)
}

// FundingResult is one position's funding settlement outcome.
type FundingResult struct {
	UserID   string
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
func (e *Engine) UsersWithPosition(symbol string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out []string
	for user, bySym := range e.positions {
		if p := bySym[symbol]; p != nil && !p.IsFlat() {
			out = append(out, user)
		}
	}
	sort.Strings(out)
	return out
}

// SettleFundingUser settles one funding round against (user, symbol) at the
// current mark, guarded by the per-position funding_round_seen watermark
// (ADR-0068 invariant #3: roundID <= seen is a replay and is skipped). Returns
// the result and whether it applied. The caller MUST run this inside the user's
// sequencer (invariant #1). Guard + apply + advance are one locked step.
func (e *Engine) SettleFundingUser(user, symbol string, roundID int64, rate dec.Decimal) (FundingResult, bool) {
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
	return FundingResult{UserID: user, Symbol: symbol, Payment: delta, Position: *p}, true
}

// AddInsurance adjusts a symbol's insurance fund (ADR-0068 §9). delta may be
// negative (fund covers a shortfall). Returns the new balance.
func (e *Engine) AddInsurance(symbol string, delta dec.Decimal) dec.Decimal {
	e.mu.Lock()
	defer e.mu.Unlock()
	cur := e.insurance[symbol]
	cur = cur.Add(delta)
	e.insurance[symbol] = cur
	return cur
}

// InsuranceFund returns a symbol's insurance balance.
func (e *Engine) InsuranceFund(symbol string) dec.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.insurance[symbol]
}

// LiquidationCandidate is a position that breached maintenance margin and is
// up for liquidation (ADR-0068 §8).
type LiquidationCandidate struct {
	UserID          string
	Symbol          string
	Side            perpstate.Side
	Size            dec.Decimal
	Mark            dec.Decimal
	BankruptcyPrice dec.Decimal // where the reduce_only liquidation order is placed
}

// LiquidatablePositions scans every non-flat position in symbol at the
// current mark and returns those whose isolated collateral pool breaches the
// maintenance margin rate (ADR-0068 §8). It keys off the CollateralPool seam
// (perpstate.Isolated) — invariant #6 — so cross margin reuses this path. The
// method is read-only; the liquidation flow (cancel the position's orders →
// submit the bankruptcy reduce_only order → book insurance) runs in the
// per-user sequencer once Match is wired.
func (e *Engine) LiquidatablePositions(symbol string, mmr dec.Decimal) []LiquidationCandidate {
	e.mu.RLock()
	defer e.mu.RUnlock()
	mark := e.marks[symbol]
	marks := map[string]dec.Decimal{symbol: mark}
	var out []LiquidationCandidate
	for user, bySym := range e.positions {
		p := bySym[symbol]
		if p == nil || p.IsFlat() {
			continue
		}
		if perpstate.Isolated(p).Liquidatable(marks, mmr) {
			out = append(out, LiquidationCandidate{
				UserID: user, Symbol: symbol, Side: p.Side, Size: p.Size,
				Mark: mark, BankruptcyPrice: p.BankruptcyPrice(),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out
}

// LiquidationCheck re-evaluates a single (user, symbol) position against the
// maintenance margin rate under the lock, returning the candidate when it still
// breaches. The service calls this inside the user's sequencer to re-verify
// before acting (the scan that found it ran lock-free and the position may have
// moved since — TOCTOU guard, ADR-0068 invariant #1).
func (e *Engine) LiquidationCheck(user, symbol string, mmr dec.Decimal) (LiquidationCandidate, bool) {
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
	if !perpstate.Isolated(p).Liquidatable(map[string]dec.Decimal{symbol: mark}, mmr) {
		return LiquidationCandidate{}, false
	}
	return LiquidationCandidate{
		UserID: user, Symbol: symbol, Side: p.Side, Size: p.Size,
		Mark: mark, BankruptcyPrice: p.BankruptcyPrice(),
	}, true
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
func (e *Engine) ApplyLiquidationFill(user, symbol string, seq uint64, f perpstate.Fill) (res perpstate.FillResult, insuranceDelta dec.Decimal, applied bool) {
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
	// Equity freed by this reduce goes to insurance, not the wallet.
	insuranceDelta = res.MarginReleased.Add(res.Realized).Sub(res.Fee)
	e.insurance[symbol] = e.insurance[symbol].Add(insuranceDelta)
	return res, insuranceDelta, true
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
func (e *Engine) ForceClose(user, symbol string, fillPrice dec.Decimal) (insuranceDelta dec.Decimal, ok bool) {
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
	e.insurance[symbol] = e.insurance[symbol].Add(equity)
	return equity, true
}

// WalletOf returns a copy of the user's wallet.
func (e *Engine) WalletOf(user string) Wallet {
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
func (e *Engine) PositionOf(user, symbol string) (perpstate.Position, bool) {
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
func (e *Engine) PositionRaw(user, symbol string) (perpstate.Position, bool) {
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
func (e *Engine) PositionsOf(user string) []perpstate.Position {
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
