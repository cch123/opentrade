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

// PositionMode selects how a user's exposure in one symbol is keyed
// (ADR-0077): one net position (idx 0, buy reduces a short / flips), or two
// hedge legs (idx 1 = long leg, idx 2 = short leg) that move independently
// and never flip. The mode is per-(user, symbol) config; the zero value is
// the pre-hedge default so absent config / old snapshots read as ONE_WAY.
type PositionMode uint8

const (
	PositionOneWay PositionMode = 0
	PositionHedge  PositionMode = 1
)

func (m PositionMode) String() string {
	switch m {
	case PositionOneWay:
		return "one_way"
	case PositionHedge:
		return "hedge"
	default:
		return "unknown"
	}
}

// Position index values (ADR-0077 §1, aligned with Bybit positionIdx).
const (
	IdxNet   uint8 = 0 // ONE_WAY net position
	IdxLong  uint8 = 1 // HEDGE long leg
	IdxShort uint8 = 2 // HEDGE short leg
)

// LegSide returns the canonical direction a hedge leg's size carries:
// IdxLong → buy, IdxShort → sell. IdxNet (and anything else) has no
// canonical direction and returns 0.
func LegSide(idx uint8) Side {
	switch idx {
	case IdxLong:
		return SideBuy
	case IdxShort:
		return SideSell
	default:
		return 0
	}
}

// ValidateOrderIntent is the ADR-0077 §2 fail-closed admission matrix: in
// ONE_WAY only idx 0 is accepted (reduce_only keeps its net-mode meaning);
// in HEDGE the order must name a leg and (side, idx, reduce_only) must agree
// — reduce_only is a redundant degree of freedom used as a double-encoding
// consistency check, never inferred. Returns "" when valid, else the reject
// reason.
func ValidateOrderIntent(mode PositionMode, idx uint8, side Side, reduceOnly bool) string {
	if mode != PositionHedge {
		if idx != IdxNet {
			return "position_idx_requires_hedge_mode"
		}
		return ""
	}
	legSide := LegSide(idx)
	if legSide == 0 {
		return "position_idx_required_in_hedge_mode"
	}
	if reduceOnly != (side != legSide) {
		// Opening a leg trades on its canonical side with reduce_only=false;
		// closing trades the opposite side with reduce_only=true. The other
		// four (side, idx, reduce_only) combinations are contradictions.
		return "position_intent_mismatch"
	}
	return ""
}

// MarginMode selects how a position draws margin (ADR-0068 §3.1, ADR-0074).
// Isolated positions hold a dedicated Margin bucket; cross positions hold no
// cash bucket (Margin stays 0) — their margin requirement is a derived risk
// value against the account-level cross pool (ADR-0074 §4 rule #2). The
// collateral-pool seam (pool.go / poolrisk.go) is the single place the two
// modes diverge.
type MarginMode uint8

const (
	MarginIsolated MarginMode = 1
	MarginCross    MarginMode = 2
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
	UserID uint64
	Symbol string
	// PositionIdx is the third key segment (ADR-0077): 0 = one-way net,
	// 1 = hedge long leg, 2 = hedge short leg. A leg's direction is part of
	// its identity — settlement uses ApplyFillLeg, which never flips.
	PositionIdx uint8
	Side        Side
	Size        dec.Decimal // base units, >= 0
	Entry       dec.Decimal // weighted-average entry price (USDT)
	Margin      dec.Decimal // USDT held against this position (isolated only; cross keeps 0)
	Leverage    dec.Decimal // > 0; ADR-0074 §8: persistent position config, not a per-order field
	Mode        MarginMode

	// ADR-0074 position config. RiskID is the user-selected risk-limit tier
	// (1-based into the symbol's tier table; 0 = auto-select the lowest tier
	// covering current notional). Risk math always uses the MORE conservative
	// of (auto tier, RiskID) — see RiskModel.EffectiveTierIndex.
	RiskID uint32
	// AutoAddMargin (isolated only): on a mark tick, before the liquidation
	// check, top the position margin up from the wallet's free balance when
	// health drops below the configured trigger (ADR-0074 §7). AutoAddMax
	// caps the transfer per event (0 = uncapped).
	AutoAddMargin bool
	AutoAddMax    dec.Decimal

	Realized dec.Decimal // cumulative realized PnL (incl. funding), USDT, reporting

	// RiskConfigVersion pins which SymbolConfig version's risk tiers govern
	// this position (ADR-0075 §3 staged application): stamped on open /
	// increase, so a later STAGED tightening only affects new exposure while
	// this position keeps evaluating at its pinned version. 0 = no pin (use
	// the symbol's active version — pre-catalog positions and legacy mode).
	RiskConfigVersion uint64

	// Recovery / idempotency water marks (persisted in snapshot, ADR-0068
	// §3.2 + invariant #3). The service advances these; the algebra here
	// does not touch them.
	LastMatchSeq     uint64 // per-(user,symbol) match_seq guard
	LastAdlRound     uint64 // ADR-0070 ADL task guard; prevents repeated forced closes
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
