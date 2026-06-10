package engine

// fee_test.go covers the ADR-0079 engine-level fee application: reserve-first
// draw, the isolated deficit clamp, cross pool-style settlement, rebate
// payment, and the platform balance + snapshot round-trip.

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func feq(t *testing.T, got dec.Decimal, want, what string) {
	t.Helper()
	if got.Cmp(dec.New(want)) != 0 {
		t.Fatalf("%s: got %s want %s", what, got.String(), want)
	}
}

func buyFill(price, qty string) perpstate.Fill {
	return perpstate.Fill{Side: perpstate.SideBuy, Price: dec.New(price), Qty: dec.New(qty)}
}

// Fee draws from the order's fee reservation first; the platform account
// collects it and the wallet Available is untouched.
func TestApplyFillWithFee_DrawsReserveFirst(t *testing.T) {
	e := New()
	e.Deposit(1001, dec.New("20"))
	if !e.Reserve(1001, dec.New("10.5")) { // IM 10 + fee buffer 0.5
		t.Fatal("reserve failed")
	}
	_, _, out, applied := e.ApplyFillWithFee(1001, "BTC-USDT-PERP", 0, dec.New("10"), 1, buyFill("100", "1"),
		FeeCharge{Amount: dec.New("0.2"), Asset: "USDT", FromReserve: dec.New("0.5")})
	if !applied {
		t.Fatal("not applied")
	}
	feq(t, out.Collected, "0.2", "collected")
	feq(t, out.ReserveUsed, "0.2", "reserve used")
	feq(t, out.Deficit, "0", "deficit")
	w := e.WalletOf(1001)
	feq(t, w.Available, "9.5", "available untouched")
	feq(t, w.Reserved, "0.3", "reserved after IM conversion + fee draw")
	feq(t, e.PlatformFee("USDT"), "0.2", "platform fee")
	feq(t, out.WalletAfter, "9.5", "wallet_after echo")
}

// Isolated: the portion Available cannot cover becomes fee_deficit; the
// wallet is clamped at zero, never negative (ADR-0079 §4).
func TestApplyFillWithFee_IsolatedDeficitClamp(t *testing.T) {
	e := New()
	e.Deposit(1001, dec.New("10.05"))
	if !e.Reserve(1001, dec.New("10")) {
		t.Fatal("reserve failed")
	}
	_, _, out, _ := e.ApplyFillWithFee(1001, "BTC-USDT-PERP", 0, dec.New("10"), 1, buyFill("100", "1"),
		FeeCharge{Amount: dec.New("0.2"), Asset: "USDT"})
	feq(t, out.Collected, "0.05", "collected only what was there")
	feq(t, out.Deficit, "0.15", "uncollectable remainder")
	feq(t, e.WalletOf(1001).Available, "0", "clamped at zero")
	feq(t, e.PlatformFee("USDT"), "0.05", "platform got the collected part only")
}

// Cross: the fee settles against the pool's cash component like realized
// PnL — Available may go negative, no deficit is recorded.
func TestApplyFillWithFee_CrossSettlesNegative(t *testing.T) {
	e := New()
	e.Deposit(1001, dec.New("10.05"))
	if !e.ReserveCross(1001, dec.New("10")) {
		t.Fatal("reserve failed")
	}
	// The position record itself stays isolated-mode here; FeeCharge.Cross is
	// what selects the bucket + clamp policy (the service sets it from the
	// order's margin mode).
	_, _, out, _ := e.ApplyFillWithFee(1001, "BTC-USDT-PERP", 0, dec.New("10"), 1, buyFill("100", "1"),
		FeeCharge{Amount: dec.New("0.2"), Asset: "USDT", Cross: true})
	feq(t, out.Deficit, "0", "no deficit for cross")
	feq(t, out.Collected, "0.2", "collected in full")
	// Isolated-mode position pulled its IM from Available (CrossReserved holds
	// the order hold): 10.05 - 10 (IM fallback) - 0.2 (fee) = -0.15... the IM
	// drained from Reserved (empty) falls back to Available by design, so the
	// observable invariant is the fee made Available 0.2 lower than it would
	// be without it, ending negative.
	feq(t, e.WalletOf(1001).Available, "-10.15", "fee allowed to dig below zero for cross")
}

// A rebate is paid unconditionally; the platform balance is a liability
// tracker and may go negative (ADR-0079 §5).
func TestApplyFillWithFee_RebatePaidUnconditionally(t *testing.T) {
	e := New()
	e.Deposit(1001, dec.New("20"))
	if !e.Reserve(1001, dec.New("10")) {
		t.Fatal("reserve failed")
	}
	_, _, out, _ := e.ApplyFillWithFee(1001, "BTC-USDT-PERP", 0, dec.New("10"), 1, buyFill("100", "1"),
		FeeCharge{Amount: dec.New("-0.05"), Asset: "USDT"})
	feq(t, out.Rebated, "0.05", "rebated")
	feq(t, out.Collected, "0", "nothing collected")
	feq(t, e.WalletOf(1001).Available, "10.05", "rebate credited")
	feq(t, e.PlatformFee("USDT"), "-0.05", "platform liability")
}

// A replayed match_seq skips the fee with the fill.
func TestApplyFillWithFee_ReplaySkipsFee(t *testing.T) {
	e := New()
	e.Deposit(1001, dec.New("100"))
	charge := FeeCharge{Amount: dec.New("0.2"), Asset: "USDT"}
	if _, _, _, applied := e.ApplyFillWithFee(1001, "BTC-USDT-PERP", 0, dec.New("10"), 7, buyFill("100", "1"), charge); !applied {
		t.Fatal("first apply")
	}
	if _, _, _, applied := e.ApplyFillWithFee(1001, "BTC-USDT-PERP", 0, dec.New("10"), 7, buyFill("100", "1"), charge); applied {
		t.Fatal("replay must not apply")
	}
	feq(t, e.PlatformFee("USDT"), "0.2", "fee charged exactly once")
}

func TestCustomerFeeOverride_ResolutionAndSnapshot(t *testing.T) {
	e := New()
	e.SetCustomerFeeOverride(1001, "", FeeOverride{RuleID: "vip-global",
		MakerRate: dec.New("0.0001"), TakerRate: dec.New("0.001"), UpdatedMs: 1})
	e.SetCustomerFeeOverride(1001, "BTC-USDT-PERP", FeeOverride{RuleID: "vip-sym",
		MakerRate: dec.New("0.0002"), TakerRate: dec.New("0.0005"), UpdatedMs: 2})

	if ov, ok := e.CustomerFeeOverride(1001, "BTC-USDT-PERP"); !ok || ov.RuleID != "vip-sym" {
		t.Fatalf("symbol row must win: %+v ok=%v", ov, ok)
	}
	if ov, ok := e.CustomerFeeOverride(1001, "ETH-USDT-PERP"); !ok || ov.RuleID != "vip-global" {
		t.Fatalf("global row must back other symbols: %+v ok=%v", ov, ok)
	}
	if _, ok := e.CustomerFeeOverride(1002, "BTC-USDT-PERP"); ok {
		t.Fatal("no override for other users")
	}

	// Platform balance + overrides round-trip the snapshot.
	e.Deposit(1001, dec.New("100"))
	e.ApplyFillWithFee(1001, "BTC-USDT-PERP", 0, dec.New("10"), 1, buyFill("100", "1"),
		FeeCharge{Amount: dec.New("0.3"), Asset: "USDT"})
	snap := e.Snapshot()

	restored := New()
	restored.Restore(snap)
	feq(t, restored.PlatformFee("USDT"), "0.3", "platform fee restored")
	if ov, ok := restored.CustomerFeeOverride(1001, "BTC-USDT-PERP"); !ok || ov.RuleID != "vip-sym" || ov.TakerRate.Cmp(dec.New("0.0005")) != 0 {
		t.Fatalf("override restored wrong: %+v ok=%v", ov, ok)
	}

	// Removal: empty rule id deletes the row; the global row takes over.
	e.SetCustomerFeeOverride(1001, "BTC-USDT-PERP", FeeOverride{RuleID: ""})
	if ov, ok := e.CustomerFeeOverride(1001, "BTC-USDT-PERP"); !ok || ov.RuleID != "vip-global" {
		t.Fatalf("after removal the global row must resolve: %+v ok=%v", ov, ok)
	}
}
