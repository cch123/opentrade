package counterstate

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

// TestVersion_TransferBumpsBothLayers verifies ADR-0048 backlog item 1
// 方案 B: setBalance (via ApplyTransfer here) bumps Account.Version and
// Balance.Version by 1 on each mutation. The two layers advance together
// but only the mutated asset's Balance moves.
func TestVersion_TransferBumpsBothLayers(t *testing.T) {
	state := NewShardState(0)
	acc := state.Account("u1")

	if got := acc.Version(); got != 0 {
		t.Fatalf("initial account version = %d, want 0", got)
	}

	if _, err := state.ApplyTransfer(TransferRequest{
		UserID: "u1", Asset: "USDT", Amount: dec.New("100"), Type: TransferDeposit,
	}); err != nil {
		t.Fatal(err)
	}
	if got := acc.Version(); got != 1 {
		t.Fatalf("after deposit account version = %d, want 1", got)
	}
	if got := acc.Balance("USDT").Version; got != 1 {
		t.Fatalf("USDT version = %d, want 1", got)
	}

	// A second mutation on the same asset bumps both.
	if _, err := state.ApplyTransfer(TransferRequest{
		UserID: "u1", Asset: "USDT", Amount: dec.New("10"), Type: TransferFreeze,
	}); err != nil {
		t.Fatal(err)
	}
	if got := acc.Version(); got != 2 {
		t.Fatalf("after freeze account version = %d, want 2", got)
	}
	if got := acc.Balance("USDT").Version; got != 2 {
		t.Fatalf("USDT version = %d, want 2", got)
	}

	// A mutation on a different asset bumps account-level but its own
	// balance-level starts fresh at 1.
	if _, err := state.ApplyTransfer(TransferRequest{
		UserID: "u1", Asset: "BTC", Amount: dec.New("0.5"), Type: TransferDeposit,
	}); err != nil {
		t.Fatal(err)
	}
	if got := acc.Version(); got != 3 {
		t.Fatalf("after BTC deposit account version = %d, want 3", got)
	}
	if got := acc.Balance("BTC").Version; got != 1 {
		t.Fatalf("BTC version = %d, want 1 (independent of USDT)", got)
	}
	// USDT remains at 2 — untouched.
	if got := acc.Balance("USDT").Version; got != 2 {
		t.Fatalf("USDT version = %d, want 2 after BTC deposit", got)
	}
}

// TestVersion_SettlementBumpsBothAssets verifies that ApplyPartySettlement
// (which mutates base AND quote in the same call) advances Account.Version
// by 2 and each Balance.Version by 1.
func TestVersion_SettlementBumpsBothAssets(t *testing.T) {
	state := NewShardState(0)
	// Seed and freeze quote on the buyer.
	if _, err := state.ApplyTransfer(TransferRequest{
		UserID: "u1", Asset: "USDT", Amount: dec.New("1000"), Type: TransferDeposit,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyTransfer(TransferRequest{
		UserID: "u1", Asset: "USDT", Amount: dec.New("100"), Type: TransferFreeze,
	}); err != nil {
		t.Fatal(err)
	}
	acc := state.Account("u1")
	accVerBefore := acc.Version()
	usdtVerBefore := acc.Balance("USDT").Version

	// Apply a buy-side settlement: +1 BTC available, -100 USDT frozen (consumed).
	p := PartySettlement{
		UserID:           "u1",
		OrderID:          1,
		BaseDelta:        dec.New("1"),
		QuoteDelta:       dec.Zero,
		FrozenBaseDelta:  dec.Zero,
		FrozenQuoteDelta: dec.New("-100"),
		FilledQtyAfter:   dec.New("1"),
		StatusAfter:      OrderStatusFilled,
	}
	// Need the order to exist for UpdateStatus to work.
	if err := state.Orders().Insert(&Order{
		ID: 1, UserID: "u1", Symbol: "BTC-USDT", Side: SideBid, Type: OrderTypeLimit,
		Price: dec.New("100"), Qty: dec.New("1"), FilledQty: dec.Zero,
		FrozenAsset: "USDT", FrozenAmount: dec.New("100"), FrozenSpent: dec.Zero,
		Status: OrderStatusNew,
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.ApplyPartySettlement("BTC-USDT", p); err != nil {
		t.Fatal(err)
	}

	// Both base (+1 BTC) and quote (-100 frozen USDT) moved → account
	// version bumps by 2 (once per setBalance call).
	if got := acc.Version(); got != accVerBefore+2 {
		t.Fatalf("account version = %d, want %d", got, accVerBefore+2)
	}
	if got := acc.Balance("USDT").Version; got != usdtVerBefore+1 {
		t.Fatalf("USDT version = %d, want %d", got, usdtVerBefore+1)
	}
	if got := acc.Balance("BTC").Version; got != 1 {
		t.Fatalf("BTC version = %d, want 1 (first touch)", got)
	}
}

// TestVersion_RestoreDoesNotBump verifies that snapshot-restore paths
// (PutForRestore / RestoreVersion) preserve the on-disk values verbatim;
// they must NOT advance the counters because that would break monotonic
// semantics across process restart.
func TestVersion_RestoreDoesNotBump(t *testing.T) {
	state := NewShardState(0)
	acc := state.Account("u1")

	acc.PutForRestore("USDT", Balance{
		Available: dec.New("100"), Frozen: dec.New("0"), Version: 42,
	})
	acc.RestoreVersion(99)

	if got := acc.Version(); got != 99 {
		t.Fatalf("restored account version = %d, want 99", got)
	}
	if got := acc.Balance("USDT").Version; got != 42 {
		t.Fatalf("restored USDT version = %d, want 42", got)
	}
}
