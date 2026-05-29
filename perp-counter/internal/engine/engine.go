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
