package engine

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
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
	e.Deposit(1001, d("1000"))
	if !e.Reserve(1001, d("10")) { // IM for 1 @100 lev10
		t.Fatal("reserve should succeed")
	}
	w := e.WalletOf(1001)
	eq(t, w.Available, "990", "available after reserve")
	eq(t, w.Reserved, "10", "reserved after reserve")

	e.ApplyFill(1001, "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1"), Fee: d("0")})

	w = e.WalletOf(1001)
	eq(t, w.Reserved, "0", "reserved consumed into position margin")
	eq(t, w.Available, "990", "available unchanged (IM came from reserved)")
	p, ok := e.PositionOf(1001, "BTC-USDT-PERP")
	if !ok {
		t.Fatal("position should exist")
	}
	eq(t, p.Size, "1", "size")
	eq(t, p.Margin, "10", "position margin")
}

func TestEngine_CloseReleasesMarginAndPnL(t *testing.T) {
	e := New()
	e.Deposit(1001, d("1000"))
	e.Reserve(1001, d("10"))
	e.ApplyFill(1001, "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1"), Fee: d("0")})
	// Close at 110 → realized +10, release margin 10.
	e.ApplyFill(1001, "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideSell, Price: d("110"), Qty: d("1"), Fee: d("0")})

	w := e.WalletOf(1001)
	eq(t, w.Available, "1010", "available = 990 + released 10 + pnl 10")
	eq(t, w.Reserved, "0", "no reserved left")
	if _, ok := e.PositionOf(1001, "BTC-USDT-PERP"); ok {
		t.Fatal("closed position should be gone")
	}
}

func TestEngine_ReserveInsufficient(t *testing.T) {
	e := New()
	e.Deposit(1001, d("5"))
	if e.Reserve(1001, d("10")) {
		t.Fatal("reserve must fail when available < im")
	}
	w := e.WalletOf(1001)
	eq(t, w.Available, "5", "available unchanged on failed reserve")
	eq(t, w.Reserved, "0", "reserved unchanged on failed reserve")
}

func TestEngine_FeeReducesAvailable(t *testing.T) {
	e := New()
	e.Deposit(1001, d("1000"))
	e.Reserve(1001, d("10"))
	e.ApplyFill(1001, "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1"), Fee: d("0.4")})
	w := e.WalletOf(1001)
	eq(t, w.Available, "989.6", "available reduced by fee 0.4")
}

func TestEngine_SnapshotRoundTrip(t *testing.T) {
	e := New()
	e.Deposit(1001, d("1000"))
	e.Deposit(1002, d("500"))
	e.Reserve(1001, d("10"))
	e.ApplyFill(1001, "BTC-USDT-PERP", d("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1"), Fee: d("0")})
	e.Reserve(1002, d("20"))
	e.ApplyFill(1002, "ETH-USDT-PERP", d("5"),
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
	w := e2.WalletOf(1001)
	eq(t, w.Available, "990", "restored u1 available")
	p, ok := e2.PositionOf(1002, "ETH-USDT-PERP")
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
	e.Deposit(1001, d("1000"))
	e.Reserve(1001, d("10"))
	buy1 := func() perpstate.Fill {
		return perpstate.Fill{Side: perpstate.SideBuy, Price: d("100"), Qty: d("1")}
	}

	if _, ok := e.ApplyFillWithSeq(1001, "BTC-USDT-PERP", d("10"), 5, buy1()); !ok {
		t.Fatal("seq 5 should apply")
	}
	p, _ := e.PositionOf(1001, "BTC-USDT-PERP")
	eq(t, p.Size, "1", "size after first apply")

	if _, ok := e.ApplyFillWithSeq(1001, "BTC-USDT-PERP", d("10"), 5, buy1()); ok {
		t.Fatal("replay of seq 5 must be skipped")
	}
	if _, ok := e.ApplyFillWithSeq(1001, "BTC-USDT-PERP", d("10"), 3, buy1()); ok {
		t.Fatal("older seq 3 must be skipped")
	}
	p, _ = e.PositionOf(1001, "BTC-USDT-PERP")
	eq(t, p.Size, "1", "size unchanged after skipped replays")

	if _, ok := e.ApplyFillWithSeq(1001, "BTC-USDT-PERP", d("10"), 6, buy1()); !ok {
		t.Fatal("newer seq 6 should apply")
	}
	p, _ = e.PositionOf(1001, "BTC-USDT-PERP")
	eq(t, p.Size, "2", "size after seq 6")
}

// openPos opens a position with ample wallet, reserving + filling once.
func openPos(e *Engine, user uint64, sym string, side perpstate.Side, price, qty, lev string) {
	e.Deposit(user, d("100000"))
	e.Reserve(user, perpstate.InitMargin(d(price), d(qty), d(lev)))
	e.ApplyFill(user, sym, d(lev), perpstate.Fill{Side: side, Price: d(price), Qty: d(qty)})
}

func TestEngine_SettleFunding(t *testing.T) {
	e := New()
	openPos(e, 1001, "BTC-USDT-PERP", perpstate.SideBuy, "100", "1", "10")  // long, margin 10
	openPos(e, 1002, "BTC-USDT-PERP", perpstate.SideSell, "100", "1", "10") // short, margin 10
	e.SetMark("BTC-USDT-PERP", d("100"))

	// rate 0.0001, notional 100 → payment 0.01: long pays, short receives.
	res := e.SettleFunding("BTC-USDT-PERP", 1000, d("0.0001"))
	if len(res) != 2 {
		t.Fatalf("want 2 funding results, got %d", len(res))
	}
	eq(t, res[0].Payment, "-0.01", "u1 (long) pays") // sorted by user: u1 first
	eq(t, res[1].Payment, "0.01", "u2 (short) receives")
	p1, _ := e.PositionOf(1001, "BTC-USDT-PERP")
	eq(t, p1.Margin, "9.99", "u1 margin after funding")
	p2, _ := e.PositionOf(1002, "BTC-USDT-PERP")
	eq(t, p2.Margin, "10.01", "u2 margin after funding")

	// Replay same round → skipped, margins unchanged.
	if res := e.SettleFunding("BTC-USDT-PERP", 1000, d("0.0001")); len(res) != 0 {
		t.Fatalf("replay round must settle nothing, got %d", len(res))
	}
	p1, _ = e.PositionOf(1001, "BTC-USDT-PERP")
	eq(t, p1.Margin, "9.99", "u1 margin unchanged after replay")

	// New round applies again.
	if res := e.SettleFunding("BTC-USDT-PERP", 2000, d("0.0001")); len(res) != 2 {
		t.Fatalf("new round should settle 2, got %d", len(res))
	}
}

func TestEngine_LiquidatablePositions(t *testing.T) {
	e := New()
	e.SetRiskModel(perpstate.NewRiskModel(nil, d("0.005"), zero, zero))
	openPos(e, 1001, "BTC-USDT-PERP", perpstate.SideBuy, "100", "1", "10") // long, margin 10

	e.SetMark("BTC-USDT-PERP", d("100"))
	if got := e.LiquidatablePositions("BTC-USDT-PERP"); len(got) != 0 {
		t.Fatalf("healthy long at entry should not be liquidatable, got %d", len(got))
	}
	e.SetMark("BTC-USDT-PERP", d("95"))
	if got := e.LiquidatablePositions("BTC-USDT-PERP"); len(got) != 0 {
		t.Fatalf("long at 95 (ratio ~0.0526) should be safe, got %d", len(got))
	}
	e.SetMark("BTC-USDT-PERP", d("90"))
	got := e.LiquidatablePositions("BTC-USDT-PERP")
	if len(got) != 1 {
		t.Fatalf("long at 90 (equity 0) should be liquidatable, got %d", len(got))
	}
	eq(t, got[0].BankruptcyPrice, "90", "bankruptcy price 100-10/1")
	if got[0].Side != perpstate.SideBuy {
		t.Fatalf("candidate side = %v, want buy", got[0].Side)
	}
}

func TestEngine_LiquidationIndexMatchesFullScanAcrossMutations(t *testing.T) {
	e := New()
	model := perpstate.NewRiskModel(nil, d("0.05"), zero, zero)
	e.SetRiskModel(model)
	symbol := "BTC-USDT-PERP"

	openPos(e, 1001, symbol, perpstate.SideBuy, "100", "1", "10")
	openPos(e, 1002, symbol, perpstate.SideBuy, "100", "1", "20")
	openPos(e, 1003, symbol, perpstate.SideSell, "100", "1", "10")
	assertLiquidationIndexMatchesFullScan(t, e, symbol)

	// Closing to flat must remove the old index key; otherwise later mark gaps
	// would keep producing a false candidate for a position that no longer
	// exists.
	e.ApplyFillWithSeq(1001, symbol, d("10"), 1,
		perpstate.Fill{Side: perpstate.SideSell, Price: d("100"), Qty: d("1")})
	assertLiquidationIndexMatchesFullScan(t, e, symbol)

	// A flip is the most error-prone update because the position leaves the
	// long tree and re-enters the short tree under the same (user,symbol).
	e.ApplyFillWithSeq(1002, symbol, d("20"), 1,
		perpstate.Fill{Side: perpstate.SideSell, Price: d("100"), Qty: d("2")})
	assertLiquidationIndexMatchesFullScan(t, e, symbol)

	e.SetMark(symbol, d("100"))
	e.ApplyFunding(1003, symbol, d("0.01"))
	assertLiquidationIndexMatchesFullScan(t, e, symbol)

	e.ApplyPartialLiquidationFill(1002, symbol, 2,
		perpstate.Fill{Side: perpstate.SideBuy, Price: d("95"), Qty: d("0.25")},
		d("0.001"))
	assertLiquidationIndexMatchesFullScan(t, e, symbol)
}

func TestEngine_LiquidationIndexRebuildsOnRestore(t *testing.T) {
	model := perpstate.NewRiskModel(nil, d("0.05"), zero, zero)
	symbol := "BTC-USDT-PERP"
	e := New()
	e.SetRiskModel(model)
	openPos(e, 1001, symbol, perpstate.SideBuy, "100", "1", "10")
	openPos(e, 1002, symbol, perpstate.SideSell, "100", "1", "10")
	e.SetMark(symbol, d("90"))
	snap := e.Snapshot()

	configuredBeforeRestore := New()
	configuredBeforeRestore.SetRiskModel(model)
	configuredBeforeRestore.Restore(snap)
	assertLiquidationIndexMatchesFullScan(t, configuredBeforeRestore, symbol)

	configuredAfterRestore := New()
	configuredAfterRestore.Restore(snap)
	configuredAfterRestore.SetRiskModel(model)
	assertLiquidationIndexMatchesFullScan(t, configuredAfterRestore, symbol)
}

func assertLiquidationIndexMatchesFullScan(t *testing.T, e *Engine, symbol string) {
	t.Helper()
	for _, mark := range []string{"80", "94", "95", "100", "105", "115"} {
		m := d(mark)
		e.SetMark(symbol, m)
		got := liquidationCandidateSummary(e.LiquidatablePositions(symbol))
		want := liquidationCandidateSummary(fullScanLiquidationsForTest(e, symbol, m))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("mark %s indexed candidates mismatch:\n got  %v\n want %v", mark, got, want)
		}
	}
}

// fullScanLiquidationsForTest is the index-free reference: every position is
// rechecked through the same candidate predicate the indexed path uses.
func fullScanLiquidationsForTest(e *Engine, symbol string, mark dec.Decimal) []LiquidationCandidate {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out []LiquidationCandidate
	for user, bySym := range e.positions {
		if cand, ok := e.liquidationCandidateLocked(user, symbol, bySym[symbol], mark); ok {
			out = append(out, cand)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out
}

func liquidationCandidateSummary(candidates []LiquidationCandidate) []string {
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, fmt.Sprintf("%d|%s|%d|%s|%s|%d",
			c.UserID, c.Symbol, c.Side, c.Size.String(), c.LiqPrice.String(), c.PositionVersion))
	}
	return out
}

func TestEngine_ForceClose(t *testing.T) {
	// Filled exactly at bankruptcy (90) → zero insurance impact.
	e := New()
	openPos(e, 1001, "BTC-USDT-PERP", perpstate.SideBuy, "100", "1", "10") // margin 10, bankruptcy 90
	delta, ok := e.ForceClose(1001, "BTC-USDT-PERP", d("90"))
	if !ok {
		t.Fatal("force close should succeed")
	}
	eq(t, delta, "0", "equity at bankruptcy is 0")
	eq(t, e.InsuranceFund("BTC-USDT-PERP"), "0", "fund unchanged at bankruptcy")
	if _, exists := e.PositionOf(1001, "BTC-USDT-PERP"); exists {
		t.Fatal("position should be wiped")
	}

	// Filled better than bankruptcy (92) → surplus 2 into the fund.
	e = New()
	openPos(e, 1001, "BTC-USDT-PERP", perpstate.SideBuy, "100", "1", "10")
	delta, _ = e.ForceClose(1001, "BTC-USDT-PERP", d("92")) // realized -8, equity 2
	eq(t, delta, "2", "surplus to insurance")
	eq(t, e.InsuranceFund("BTC-USDT-PERP"), "2", "fund grows by surplus")

	// Filled past bankruptcy (88) → deficit 2, fund covers (goes negative).
	e = New()
	openPos(e, 1001, "BTC-USDT-PERP", perpstate.SideBuy, "100", "1", "10")
	delta, _ = e.ForceClose(1001, "BTC-USDT-PERP", d("88")) // realized -12, equity -2
	eq(t, delta, "-2", "deficit drawn from insurance")
	eq(t, e.InsuranceFund("BTC-USDT-PERP"), "-2", "fund covers the shortfall")

	// No position → ok=false.
	if _, ok := e.ForceClose(1002, "BTC-USDT-PERP", d("100")); ok {
		t.Fatal("force close with no position should be ok=false")
	}
}
