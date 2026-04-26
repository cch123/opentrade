package counter

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/xargin/opentrade/pkg/counterstate"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/snapshot"
)

// newFSStore returns a throwaway FSBlobStore for a test. Callers pick a
// key stem (e.g. "shard-0"); the store prefixes the tempdir.
func newFSStore(t *testing.T) snapshot.BlobStore {
	t.Helper()
	return snapshot.NewFSBlobStore(t.TempDir())
}

func TestSaveLoadRoundTrip_Proto(t *testing.T) {
	testSaveLoadRoundTrip(t, snapshot.FormatProto)
}

func TestSaveLoadRoundTrip_JSON(t *testing.T) {
	testSaveLoadRoundTrip(t, snapshot.FormatJSON)
}

func testSaveLoadRoundTrip(t *testing.T, format snapshot.Format) {
	ctx := context.Background()
	store := newFSStore(t)
	snap := &ShardSnapshot{
		Version:     Version,
		ShardID:     0,
		CounterSeq:  99,
		TimestampMS: 1,
		Accounts: []AccountSnapshot{{
			UserID: "u1",
			Balances: []BalanceSnapshot{
				{Asset: "USDT", Available: "100", Frozen: "50"},
			},
		}},
	}
	if err := Save(ctx, store, "counter-shard-0", snap, format); err != nil {
		t.Fatal(err)
	}
	got, err := Load(ctx, store, "counter-shard-0")
	if err != nil {
		t.Fatal(err)
	}
	if got.ShardID != 0 || got.CounterSeq != 99 || len(got.Accounts) != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

// TestSaveLoadRoundTrip_ProtoPreferredOverJSON verifies that when both
// formats live on the store, Load prefers the proto variant (ADR-0049
// probe order).
func TestSaveLoadRoundTrip_ProtoPreferredOverJSON(t *testing.T) {
	ctx := context.Background()
	store := newFSStore(t)
	protoSnap := &ShardSnapshot{Version: Version, ShardID: 0, CounterSeq: 7}
	jsonSnap := &ShardSnapshot{Version: Version, ShardID: 0, CounterSeq: 42}
	if err := Save(ctx, store, "counter-shard-0", protoSnap, snapshot.FormatProto); err != nil {
		t.Fatal(err)
	}
	if err := Save(ctx, store, "counter-shard-0", jsonSnap, snapshot.FormatJSON); err != nil {
		t.Fatal(err)
	}
	got, err := Load(ctx, store, "counter-shard-0")
	if err != nil {
		t.Fatal(err)
	}
	if got.CounterSeq != 7 {
		t.Fatalf("expected proto (seq=7) to win over json (seq=42), got %d", got.CounterSeq)
	}
}

// TestLoad_JSONOnlyMigration covers the upgrade path: only .json on the
// store, Load still returns it so the first-start-after-upgrade works.
func TestLoad_JSONOnlyMigration(t *testing.T) {
	ctx := context.Background()
	store := newFSStore(t)
	snap := &ShardSnapshot{Version: Version, ShardID: 0, CounterSeq: 42}
	if err := Save(ctx, store, "counter-shard-0", snap, snapshot.FormatJSON); err != nil {
		t.Fatal(err)
	}
	got, err := Load(ctx, store, "counter-shard-0")
	if err != nil {
		t.Fatal(err)
	}
	if got.CounterSeq != 42 {
		t.Fatalf("json-only load: got seq=%d", got.CounterSeq)
	}
}

func TestCaptureRestore(t *testing.T) {
	state := counterstate.NewShardState(3)

	// Seed state through normal Transfer API.
	for _, req := range []counterstate.TransferRequest{
		{UserID: "u1", Asset: "USDT", Amount: dec.New("100"), Type: counterstate.TransferDeposit},
		{UserID: "u1", Asset: "USDT", Amount: dec.New("40"), Type: counterstate.TransferFreeze},
		{UserID: "u2", Asset: "BTC", Amount: dec.New("0.5"), Type: counterstate.TransferDeposit},
	} {
		if _, err := state.ApplyTransfer(req); err != nil {
			t.Fatalf("seed transfer: %v", err)
		}
	}

	// Prime LastMatchSeq on two symbols for u1 (ADR-0048 backlog item 2).
	state.Account("u1").AdvanceMatchSeq("BTC-USDT", 101)
	state.Account("u1").AdvanceMatchSeq("ETH-USDT", 7)

	offsets := map[int32]int64{0: 100, 2: 55}
	snap := CaptureFromState(3, state, 42, offsets, 1234, 0)

	// Persist and reload (default proto format, ADR-0049).
	ctx := context.Background()
	store := newFSStore(t)
	if err := Save(ctx, store, "shard-3", snap, snapshot.FormatProto); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(ctx, store, "shard-3")
	if err != nil {
		t.Fatal(err)
	}

	// Restore into a fresh state.
	state2 := counterstate.NewShardState(3)
	if err := RestoreState(3, state2, loaded); err != nil {
		t.Fatalf("RestoreState: %v", err)
	}

	if loaded.CounterSeq != 42 {
		t.Fatalf("counter seq = %d, want 42", loaded.CounterSeq)
	}
	if bal := state2.Balance("u1", "USDT"); bal.Available.String() != "60" || bal.Frozen.String() != "40" {
		t.Fatalf("restored u1 USDT = %+v", bal)
	}
	if bal := state2.Balance("u2", "BTC"); bal.Available.String() != "0.5" {
		t.Fatalf("restored u2 BTC = %+v", bal)
	}
	// Offsets round-trip (ADR-0048).
	got := OffsetsSliceToMap(loaded.Offsets)
	if len(got) != 2 || got[0] != 100 || got[2] != 55 {
		t.Fatalf("offsets round-trip: %v", got)
	}
	// JournalOffset round-trip (ADR-0060 §4.1).
	if loaded.JournalOffset != 1234 {
		t.Fatalf("journal_offset round-trip = %d, want 1234", loaded.JournalOffset)
	}
	// LastMatchSeq round-trip (ADR-0048 backlog item 2).
	if got := state2.Account("u1").LastMatchSeq("BTC-USDT"); got != 101 {
		t.Errorf("BTC-USDT match_seq after restore = %d, want 101", got)
	}
	if got := state2.Account("u1").LastMatchSeq("ETH-USDT"); got != 7 {
		t.Errorf("ETH-USDT match_seq after restore = %d, want 7", got)
	}
	if got := state2.Account("u2").LastMatchSeq("BTC-USDT"); got != 0 {
		t.Errorf("u2 BTC-USDT match_seq after restore = %d, want 0", got)
	}
	// Double-layer version round-trip (ADR-0048 backlog item 1).
	// u1 ran two transfers on USDT → account version=2, USDT version=2.
	// u2 ran one BTC deposit → account version=1, BTC version=1.
	u1Acc := state2.Account("u1")
	if got := u1Acc.Version(); got != 2 {
		t.Errorf("u1 account version after restore = %d, want 2", got)
	}
	if got := u1Acc.Balance("USDT").Version; got != 2 {
		t.Errorf("u1 USDT version after restore = %d, want 2", got)
	}
	if got := state2.Account("u2").Version(); got != 1 {
		t.Errorf("u2 account version after restore = %d, want 2", got)
	}
	if got := state2.Account("u2").Balance("BTC").Version; got != 1 {
		t.Errorf("u2 BTC version after restore = %d, want 1", got)
	}
}

// TestCaptureRestore_Reservations verifies reservations survive snapshot
// round-trips — the only durability story for them (ADR-0041 §Durability).
func TestCaptureRestore_Reservations(t *testing.T) {
	state := counterstate.NewShardState(0)

	// Seed balance and reserve against it.
	if _, err := state.ApplyTransfer(counterstate.TransferRequest{
		UserID: "u1", Asset: "USDT", Amount: dec.New("1000"), Type: counterstate.TransferDeposit,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.CreateReservation("u1", "USDT", "trig-42", dec.New("100")); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	store := newFSStore(t)
	if err := Save(ctx, store, "shard-0", CaptureFromState(0, state, 0, nil, 0, 0), snapshot.FormatProto); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(ctx, store, "shard-0")
	if err != nil {
		t.Fatal(err)
	}

	state2 := counterstate.NewShardState(0)
	if err := RestoreState(0, state2, loaded); err != nil {
		t.Fatal(err)
	}
	r := state2.LookupReservation("trig-42")
	if r == nil || r.Amount.String() != "100" || r.Asset != "USDT" || r.UserID != "u1" {
		t.Fatalf("reservation round-trip lost: %+v", r)
	}
	if bal := state2.Balance("u1", "USDT"); bal.Available.String() != "900" || bal.Frozen.String() != "100" {
		t.Errorf("balance: %+v", bal)
	}
}

func TestRestoreRejectsVersionMismatch(t *testing.T) {
	state := counterstate.NewShardState(0)
	snap := &ShardSnapshot{Version: 99, ShardID: 0}
	if err := RestoreState(0, state, snap); err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestRestoreRejectsShardMismatch(t *testing.T) {
	state := counterstate.NewShardState(1)
	snap := &ShardSnapshot{Version: Version, ShardID: 2}
	if err := RestoreState(1, state, snap); err == nil {
		t.Fatal("expected shard mismatch error")
	}
}

// TestLoad_GetMissingReturnsErrNotExist is the cold-start contract: a
// fresh store must surface absence as os.ErrNotExist so callers can
// branch on errors.Is.
func TestLoad_GetMissingReturnsErrNotExist(t *testing.T) {
	ctx := context.Background()
	store := newFSStore(t)
	_, err := Load(ctx, store, "never-written")
	if err == nil {
		t.Fatal("want error on missing key")
	}
	// Load's contract is to return os.ErrNotExist on cold start.
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("want os.ErrNotExist, got %v", err)
	}
}
