// Package perpstate holds the pure state machine for USDT-margined linear
// perpetual contracts (ADR-0068): positions, margin math, settlement
// algebra, funding, and the collateral-pool abstraction that keeps the
// isolated→cross margin seam additive (ADR-0068 §3.1).
//
// This package is intentionally free of Kafka / proto / gRPC: it is the
// position+margin algebra that perp-counter (the account-truth service,
// ADR-0068 §2 A1) drives inside its per-user sequencer, and that
// trade-dump's shadow can replay. All money is dec.Decimal (no float).
//
// Conventions (linear USDT):
//   - Size is base units, always >= 0; direction lives in Side.
//   - Price / EntryPrice / mark are quote (USDT) per base.
//   - PnL is USDT = priceDiff * size.
//   - Margin is USDT held against the position (isolated: a dedicated pool).
package perpstate

import "github.com/xargin/opentrade/pkg/dec"

var zero = dec.FromInt(0)

// Side is the direction of a position or the side of an order fill.
type Side uint8

const (
	SideBuy  Side = 1 // long
	SideSell Side = 2 // short
)

func (s Side) String() string {
	switch s {
	case SideBuy:
		return "buy"
	case SideSell:
		return "sell"
	default:
		return "unknown"
	}
}

// Opposite returns the other side. Side(0) (unset) maps to unknown/0.
func (s Side) Opposite() Side {
	switch s {
	case SideBuy:
		return SideSell
	case SideSell:
		return SideBuy
	default:
		return s
	}
}

// MarginMode selects how a position draws margin (ADR-0068 §3.1). MVP only
// implements Isolated; Cross is reserved so the field exists from day 1 and
// the collateral-pool seam (pool.go) stays the single place the two modes
// diverge.
type MarginMode uint8

const (
	MarginIsolated MarginMode = 1
	MarginCross    MarginMode = 2 // reserved (future); not implemented
)

func (m MarginMode) String() string {
	switch m {
	case MarginIsolated:
		return "isolated"
	case MarginCross:
		return "cross"
	default:
		return "unknown"
	}
}

// Position is a user's holding in one perp symbol. Mutated only through the
// per-user sequencer in perp-counter (ADR-0068 invariant #1), so the type
// itself carries no lock.
//
// When Size == 0 the position is flat and Side / EntryPrice are meaningless
// (kept zeroed). Leverage is fixed at first open for the MVP; tiered
// leverage (risk tier) is future work.
type Position struct {
	UserID   string
	Symbol   string
	Side     Side
	Size     dec.Decimal // base units, >= 0
	Entry    dec.Decimal // weighted-average entry price (USDT)
	Margin   dec.Decimal // USDT held against this position (isolated pool)
	Leverage dec.Decimal // > 0
	Mode     MarginMode

	Realized dec.Decimal // cumulative realized PnL (incl. funding), USDT, reporting

	// Recovery / idempotency water marks (persisted in snapshot, ADR-0068
	// §3.2 + invariant #3). The service advances these; the algebra here
	// does not touch them.
	LastMatchSeq     uint64 // per-(user,symbol) match_seq guard
	FundingRoundSeen int64  // last applied funding_round_id
	Version          uint64 // optimistic/projection guard
}

// IsFlat reports whether the position holds no size.
func (p *Position) IsFlat() bool { return p.Size.Sign() == 0 }

// Notional returns |size| * mark (USDT exposure at the given mark price).
func (p *Position) Notional(mark dec.Decimal) dec.Decimal {
	if p.IsFlat() {
		return zero
	}
	return mark.Mul(p.Size)
}

// UnrealizedPnL is mark-based PnL (ADR-0068 invariant #2: unrealized uses
// mark, never last trade price). Zero when flat.
func (p *Position) UnrealizedPnL(mark dec.Decimal) dec.Decimal {
	if p.IsFlat() {
		return zero
	}
	if p.Side == SideBuy { // long gains as price rises
		return mark.Sub(p.Entry).Mul(p.Size)
	}
	return p.Entry.Sub(mark).Mul(p.Size) // short gains as price falls
}
