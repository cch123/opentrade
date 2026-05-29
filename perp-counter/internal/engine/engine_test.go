package engine

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func d(s string) dec.Decimal { return dec.New(s) }

func eq(t *testing.T, got dec.Decimal, want, what string) {
	t.Helper()
	if got.Cmp(d(want)) != 0 {
		t.Fatalf("%s: got %s want %s", what, got.String(), want)
	}
}

func TestEngine_OpenConsumesReservedMargin(t *testing.T) {
	e := New()
	e.Deposit("u1", d("1000"))
	if !e.Reserve("u1", d("10")) { // IM for 1 @100 lev10
		t.Fatal("reserve should succeed")
	}
	w := e.WalletOf("u1")
	eq(t, w.Available, "990", "available after reserve")
	eq(t, w.Reserved, "10", "reserved after reserve")

	e.ApplyFill("u1", "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1"), Fee: d("0")})

	w = e.WalletOf("u1")
	eq(t, w.Reserved, "0", "reserved consumed into position margin")
	eq(t, w.Available, "990", "available unchanged (IM came from reserved)")
	p, ok := e.PositionOf("u1", "BTC-USDT-PERP")
	if !ok {
		t.Fatal("position should exist")
	}
	eq(t, p.Size, "1", "size")
	eq(t, p.Margin, "10", "position margin")
}

func TestEngine_CloseReleasesMarginAndPnL(t *testing.T) {
	e := New()
	e.Deposit("u1", d("1000"))
	e.Reserve("u1", d("10"))
	e.ApplyFill("u1", "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1"), Fee: d("0")})
	// Close at 110 → realized +10, release margin 10.
	e.ApplyFill("u1", "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideSell, Price: d("110"), Qty: d("1"), Fee: d("0")})

	w := e.WalletOf("u1")
	eq(t, w.Available, "1010", "available = 990 + released 10 + pnl 10")
	eq(t, w.Reserved, "0", "no reserved left")
	if _, ok := e.PositionOf("u1", "BTC-USDT-PERP"); ok {
		t.Fatal("closed position should be gone")
	}
}

func TestEngine_ReserveInsufficient(t *testing.T) {
	e := New()
	e.Deposit("u1", d("5"))
	if e.Reserve("u1", d("10")) {
		t.Fatal("reserve must fail when available < im")
	}
	w := e.WalletOf("u1")
	eq(t, w.Available, "5", "available unchanged on failed reserve")
	eq(t, w.Reserved, "0", "reserved unchanged on failed reserve")
}

func TestEngine_FeeReducesAvailable(t *testing.T) {
	e := New()
	e.Deposit("u1", d("1000"))
	e.Reserve("u1", d("10"))
	e.ApplyFill("u1", "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1"), Fee: d("0.4")})
	w := e.WalletOf("u1")
	eq(t, w.Available, "989.6", "available reduced by fee 0.4")
}

func TestEngine_SnapshotRoundTrip(t *testing.T) {
	e := New()
	e.Deposit("u1", d("1000"))
	e.Deposit("u2", d("500"))
	e.Reserve("u1", d("10"))
	e.ApplyFill("u1", "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1"), Fee: d("0")})
	e.Reserve("u2", d("20"))
	e.ApplyFill("u2", "ETH-USDT-PERP", d("5"),
		perpstate.Fill{Side: perpstate.SideSell, Price: d("200"), Qty: d("0.5"), Fee: d("0")})
	e.SetMark("BTC-USDT-PERP", d("101"))
	e.SetMark("ETH-USDT-PERP", d("199"))
	e.AddInsurance("BTC-USDT-PERP", d("3.5"))

	snap := e.Snapshot()
	blob, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(blob, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	e2 := New()
	e2.Restore(decoded)
	got := e2.Snapshot()

	if !reflect.DeepEqual(snap, got) {
		t.Fatalf("snapshot round-trip mismatch:\n before=%+v\n after =%+v", snap, got)
	}
	// Spot-check a restored value survived serialization.
	w := e2.WalletOf("u1")
	eq(t, w.Available, "990", "restored u1 available")
	p, ok := e2.PositionOf("u2", "ETH-USDT-PERP")
	if !ok {
		t.Fatal("restored u2 position missing")
	}
	if p.Side != perpstate.SideSell {
		t.Fatalf("restored side = %v, want sell", p.Side)
	}
	eq(t, p.Entry, "200", "restored entry")
}

func TestEngine_ApplyFillWithSeq_GuardsReplay(t *testing.T) {
	e := New()
	e.Deposit("u1", d("1000"))
	e.Reserve("u1", d("10"))
	buy1 := func() perpstate.Fill {
		return perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1")}
	}

	if _, ok := e.ApplyFillWithSeq("u1", "BTC-USDT-PERP", d("10"), 5, buy1()); !ok {
		t.Fatal("seq 5 should apply")
	}
	p, _ := e.PositionOf("u1", "BTC-USDT-PERP")
	eq(t, p.Size, "1", "size after first apply")

	if _, ok := e.ApplyFillWithSeq("u1", "BTC-USDT-PERP", d("10"), 5, buy1()); ok {
		t.Fatal("replay of seq 5 must be skipped")
	}
	if _, ok := e.ApplyFillWithSeq("u1", "BTC-USDT-PERP", d("10"), 3, buy1()); ok {
		t.Fatal("older seq 3 must be skipped")
	}
	p, _ = e.PositionOf("u1", "BTC-USDT-PERP")
	eq(t, p.Size, "1", "size unchanged after skipped replays")

	if _, ok := e.ApplyFillWithSeq("u1", "BTC-USDT-PERP", d("10"), 6, buy1()); !ok {
		t.Fatal("newer seq 6 should apply")
	}
	p, _ = e.PositionOf("u1", "BTC-USDT-PERP")
	eq(t, p.Size, "2", "size after seq 6")
}

// openPos opens a position with ample wallet, reserving + filling once.
func openPos(e *Engine, user, sym string, side perpstate.Side, price, qty, lev string) {
	e.Deposit(user, d("100000"))
	e.Reserve(user, perpstate.InitMargin(d(price), d(qty), d(lev)))
	e.ApplyFill(user, sym, d(lev), perpstate.Fill{Side: side, Price: d(price), Qty: d(qty)})
}

func TestEngine_SettleFunding(t *testing.T) {
	e := New()
	openPos(e, "u1", "BTC-USDT-PERP", perpstate.SideBuy, "100", "1", "10")  // long, margin 10
	openPos(e, "u2", "BTC-USDT-PERP", perpstate.SideSell, "100", "1", "10") // short, margin 10
	e.SetMark("BTC-USDT-PERP", d("100"))

	// rate 0.0001, notional 100 → payment 0.01: long pays, short receives.
	res := e.SettleFunding("BTC-USDT-PERP", 1000, d("0.0001"))
	if len(res) != 2 {
		t.Fatalf("want 2 funding results, got %d", len(res))
	}
	eq(t, res[0].Payment, "-0.01", "u1 (long) pays")   // sorted by user: u1 first
	eq(t, res[1].Payment, "0.01", "u2 (short) receives")
	p1, _ := e.PositionOf("u1", "BTC-USDT-PERP")
	eq(t, p1.Margin, "9.99", "u1 margin after funding")
	p2, _ := e.PositionOf("u2", "BTC-USDT-PERP")
	eq(t, p2.Margin, "10.01", "u2 margin after funding")

	// Replay same round → skipped, margins unchanged.
	if res := e.SettleFunding("BTC-USDT-PERP", 1000, d("0.0001")); len(res) != 0 {
		t.Fatalf("replay round must settle nothing, got %d", len(res))
	}
	p1, _ = e.PositionOf("u1", "BTC-USDT-PERP")
	eq(t, p1.Margin, "9.99", "u1 margin unchanged after replay")

	// New round applies again.
	if res := e.SettleFunding("BTC-USDT-PERP", 2000, d("0.0001")); len(res) != 2 {
		t.Fatalf("new round should settle 2, got %d", len(res))
	}
}

func TestEngine_LiquidatablePositions(t *testing.T) {
	e := New()
	openPos(e, "u1", "BTC-USDT-PERP", perpstate.SideBuy, "100", "1", "10") // long, margin 10
	mmr := d("0.005")

	e.SetMark("BTC-USDT-PERP", d("100"))
	if got := e.LiquidatablePositions("BTC-USDT-PERP", mmr); len(got) != 0 {
		t.Fatalf("healthy long at entry should not be liquidatable, got %d", len(got))
	}
	e.SetMark("BTC-USDT-PERP", d("95"))
	if got := e.LiquidatablePositions("BTC-USDT-PERP", mmr); len(got) != 0 {
		t.Fatalf("long at 95 (ratio ~0.0526) should be safe, got %d", len(got))
	}
	e.SetMark("BTC-USDT-PERP", d("90"))
	got := e.LiquidatablePositions("BTC-USDT-PERP", mmr)
	if len(got) != 1 {
		t.Fatalf("long at 90 (equity 0) should be liquidatable, got %d", len(got))
	}
	eq(t, got[0].BankruptcyPrice, "90", "bankruptcy price 100-10/1")
	if got[0].Side != perpstate.SideBuy {
		t.Fatalf("candidate side = %v, want buy", got[0].Side)
	}
}
