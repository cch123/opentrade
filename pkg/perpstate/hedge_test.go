package perpstate

// ADR-0077 hedge-mode algebra tests: leg identity (LegSide), the fail-closed
// order-intent matrix, and ApplyFillLeg's never-flip clamp semantics.

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func dh(s string) dec.Decimal { return dec.New(s) }

func eqh(t *testing.T, got dec.Decimal, want, what string) {
	t.Helper()
	if got.Cmp(dh(want)) != 0 {
		t.Fatalf("%s: got %s want %s", what, got.String(), want)
	}
}

func TestLegSide(t *testing.T) {
	if LegSide(IdxLong) != SideBuy {
		t.Fatal("idx 1 must be the long leg")
	}
	if LegSide(IdxShort) != SideSell {
		t.Fatal("idx 2 must be the short leg")
	}
	if LegSide(IdxNet) != 0 {
		t.Fatal("idx 0 has no canonical direction")
	}
	if LegSide(7) != 0 {
		t.Fatal("out-of-range idx has no canonical direction")
	}
}

// TestValidateOrderIntent_Matrix walks ADR-0077 §2's full table.
func TestValidateOrderIntent_Matrix(t *testing.T) {
	cases := []struct {
		mode       PositionMode
		idx        uint8
		side       Side
		reduceOnly bool
		want       string
	}{
		// ONE_WAY: only idx 0, reduce_only free.
		{PositionOneWay, IdxNet, SideBuy, false, ""},
		{PositionOneWay, IdxNet, SideSell, true, ""},
		{PositionOneWay, IdxLong, SideBuy, false, "position_idx_requires_hedge_mode"},
		{PositionOneWay, IdxShort, SideSell, true, "position_idx_requires_hedge_mode"},
		// HEDGE valid quadrants.
		{PositionHedge, IdxLong, SideBuy, false, ""},   // open long
		{PositionHedge, IdxShort, SideSell, false, ""}, // open short
		{PositionHedge, IdxLong, SideSell, true, ""},   // close long
		{PositionHedge, IdxShort, SideBuy, true, ""},   // close short
		// HEDGE invalid quadrants (double-encoding contradictions).
		{PositionHedge, IdxLong, SideBuy, true, "position_intent_mismatch"},
		{PositionHedge, IdxShort, SideSell, true, "position_intent_mismatch"},
		{PositionHedge, IdxShort, SideBuy, false, "position_intent_mismatch"},
		{PositionHedge, IdxLong, SideSell, false, "position_intent_mismatch"},
		// HEDGE without a leg.
		{PositionHedge, IdxNet, SideBuy, false, "position_idx_required_in_hedge_mode"},
		{PositionHedge, 9, SideBuy, false, "position_idx_required_in_hedge_mode"},
	}
	for i, c := range cases {
		if got := ValidateOrderIntent(c.mode, c.idx, c.side, c.reduceOnly); got != c.want {
			t.Fatalf("case %d (%v idx=%d %v ro=%v): got %q want %q",
				i, c.mode, c.idx, c.side, c.reduceOnly, got, c.want)
		}
	}
}

func newLeg(idx uint8) *Position {
	return &Position{UserID: 1, Symbol: "BTC-USDT-PERP", PositionIdx: idx,
		Mode: MarginIsolated, Leverage: dh("10"),
		Size: dh("0"), Entry: dh("0"), Margin: dh("0"), Realized: dh("0")}
}

func TestApplyFillLeg_OpenIncreaseReduceClose(t *testing.T) {
	p := newLeg(IdxLong)
	res, excess := p.ApplyFillLeg(Fill{Side: SideBuy, Price: dh("100"), Qty: dh("1")})
	eqh(t, excess, "0", "open excess")
	eqh(t, res.MarginAdded, "10", "open IM")
	eqh(t, p.Size, "1", "size after open")
	if p.Side != SideBuy {
		t.Fatal("long leg carries buy side")
	}

	_, excess = p.ApplyFillLeg(Fill{Side: SideBuy, Price: dh("110"), Qty: dh("1")})
	eqh(t, excess, "0", "increase excess")
	eqh(t, p.Size, "2", "size after increase")
	eqh(t, p.Entry, "105", "weighted entry")

	res, excess = p.ApplyFillLeg(Fill{Side: SideSell, Price: dh("120"), Qty: dh("1")})
	eqh(t, excess, "0", "reduce excess")
	eqh(t, res.Realized, "15", "realized on reduce (120-105)")
	eqh(t, p.Size, "1", "size after reduce")

	_, excess = p.ApplyFillLeg(Fill{Side: SideSell, Price: dh("120"), Qty: dh("1")})
	eqh(t, excess, "0", "close excess")
	if !p.IsFlat() || p.Side != 0 {
		t.Fatalf("leg should be flat with side reset: %+v", p)
	}
}

// TestApplyFillLeg_OvershootClampsNeverFlips is the ADR-0077 §2 never-flip
// invariant: a close overshoot is clamped, the excess reported, and the leg
// MUST NOT open in the opposite direction (contrast: net mode flips).
func TestApplyFillLeg_OvershootClampsNeverFlips(t *testing.T) {
	p := newLeg(IdxLong)
	p.ApplyFillLeg(Fill{Side: SideBuy, Price: dh("100"), Qty: dh("1")})

	res, excess := p.ApplyFillLeg(Fill{Side: SideSell, Price: dh("110"), Qty: dh("3")})
	eqh(t, excess, "2", "clamped-off excess")
	eqh(t, res.Realized, "10", "realized only for the clamped close")
	eqh(t, res.MarginAdded, "0", "no new exposure IM — never flips")
	if !p.IsFlat() || p.Side != 0 {
		t.Fatalf("leg must end flat, not flipped: %+v", p)
	}

	// Same overshoot in net mode flips — documents the deliberate divergence.
	n := newLeg(IdxNet)
	n.ApplyFill(Fill{Side: SideBuy, Price: dh("100"), Qty: dh("1")})
	n.ApplyFill(Fill{Side: SideSell, Price: dh("110"), Qty: dh("3")})
	if n.IsFlat() || n.Side != SideSell {
		t.Fatalf("net record should have flipped short: %+v", n)
	}
	eqh(t, n.Size, "2", "net flip size")
}

// TestApplyFillLeg_OppositeFillOnFlatLeg: the TOCTOU residue case — the leg
// already closed when the reduce fill lands. Direction is identity: the fill
// must NOT open the wrong-side position; everything is excess.
func TestApplyFillLeg_OppositeFillOnFlatLeg(t *testing.T) {
	p := newLeg(IdxLong)
	res, excess := p.ApplyFillLeg(Fill{Side: SideSell, Price: dh("100"), Qty: dh("2")})
	eqh(t, excess, "2", "full qty is excess")
	eqh(t, res.Realized, "0", "nothing realized")
	eqh(t, res.MarginReleased, "0", "nothing released")
	if !p.IsFlat() {
		t.Fatalf("flat leg must stay flat: %+v", p)
	}
}

func TestApplyFillLeg_ShortLegSymmetric(t *testing.T) {
	p := newLeg(IdxShort)
	_, excess := p.ApplyFillLeg(Fill{Side: SideSell, Price: dh("100"), Qty: dh("2")})
	eqh(t, excess, "0", "open short excess")
	if p.Side != SideSell {
		t.Fatal("short leg carries sell side")
	}
	res, excess := p.ApplyFillLeg(Fill{Side: SideBuy, Price: dh("90"), Qty: dh("2")})
	eqh(t, excess, "0", "close short excess")
	eqh(t, res.Realized, "20", "short profit (100-90)*2")
	if !p.IsFlat() {
		t.Fatal("short leg should be flat")
	}
}

// A net-mode record misrouted into ApplyFillLeg fails closed: no direction is
// guessed, the whole qty comes back as excess.
func TestApplyFillLeg_NetRecordFailsClosed(t *testing.T) {
	p := newLeg(IdxNet)
	res, excess := p.ApplyFillLeg(Fill{Side: SideBuy, Price: dh("100"), Qty: dh("1")})
	eqh(t, excess, "1", "misroute is all excess")
	eqh(t, res.MarginAdded, "0", "no mutation")
	if !p.IsFlat() {
		t.Fatalf("net record must be untouched: %+v", p)
	}
}

// Funding stays per-leg algebra: a long leg pays at positive rate while a
// short leg receives — no netting inside the type (ADR-0077 §5).
func TestApplyFunding_PerLegNoNetting(t *testing.T) {
	long := newLeg(IdxLong)
	long.ApplyFillLeg(Fill{Side: SideBuy, Price: dh("100"), Qty: dh("10")})
	short := newLeg(IdxShort)
	short.ApplyFillLeg(Fill{Side: SideSell, Price: dh("100"), Qty: dh("8")})

	rate := dh("0.001")
	mark := dh("100")
	eqh(t, long.ApplyFunding(mark, rate), "-1", "long pays 1000*0.001")
	eqh(t, short.ApplyFunding(mark, rate), "0.8", "short receives 800*0.001")
}
