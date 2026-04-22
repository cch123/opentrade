package snapshot

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

// newFSStore returns a throwaway FSBlobStore for a test. Callers pick a
// key stem (e.g. "shard-0"); the store prefixes the tempdir.
func newFSStore(t *testing.T) BlobStore {
	t.Helper()
	return NewFSBlobStore(t.TempDir())
}

func TestSaveLoadRoundTrip_Proto(t *testing.T) {
	testSaveLoadRoundTrip(t, FormatProto)
}

func TestSaveLoadRoundTrip_JSON(t *testing.T) {
	testSaveLoadRoundTrip(t, FormatJSON)
}

func testSaveLoadRoundTrip(t *testing.T, format Format) {
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
	if err := Save(ctx, store, "counter-shard-0", protoSnap, FormatProto); err != nil {
		t.Fatal(err)
	}
	if err := Save(ctx, store, "counter-shard-0", jsonSnap, FormatJSON); err != nil {
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
	if err := Save(ctx, store, "counter-shard-0", snap, FormatJSON); err != nil {
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
	state := engine.NewShardState(3)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)

	// Seed state through normal Transfer API.
	for _, req := range []engine.TransferRequest{
		{UserID: "u1", Asset: "USDT", Amount: dec.New("100"), Type: engine.TransferDeposit},
		{UserID: "u1", Asset: "USDT", Amount: dec.New("40"), Type: engine.TransferFreeze},
		{UserID: "u2", Asset: "BTC", Amount: dec.New("0.5"), Type: engine.TransferDeposit},
	} {
		if _, err := state.ApplyTransfer(req); err != nil {
			t.Fatalf("seed transfer: %v", err)
		}
	}
	seq.SetCounterSeq(42)
	dt.Set("tx-1", "cached-response")

	// Prime LastMatchSeq on two symbols for u1 (ADR-0048 backlog item 2).
	state.Account("u1").AdvanceMatchSeq("BTC-USDT", 101)
	state.Account("u1").AdvanceMatchSeq("ETH-USDT", 7)

	offsets := map[int32]int64{0: 100, 2: 55}
	snap := Capture(3, state, seq, dt, offsets, 0)

	// Persist and reload (default proto format, ADR-0049).
	ctx := context.Background()
	store := newFSStore(t)
	if err := Save(ctx, store, "shard-3", snap, FormatProto); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(ctx, store, "shard-3")
	if err != nil {
		t.Fatal(err)
	}

	// Restore into fresh components.
	state2 := engine.NewShardState(3)
	seq2 := sequencer.New()
	dt2 := dedup.New(time.Hour)
	if err := Restore(3, state2, seq2, dt2, loaded); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if seq2.CounterSeq() != 42 {
		t.Fatalf("counter seq = %d, want 42", seq2.CounterSeq())
	}
	if bal := state2.Balance("u1", "USDT"); bal.Available.String() != "60" || bal.Frozen.String() != "40" {
		t.Fatalf("restored u1 USDT = %+v", bal)
	}
	if bal := state2.Balance("u2", "BTC"); bal.Available.String() != "0.5" {
		t.Fatalf("restored u2 BTC = %+v", bal)
	}
	if _, ok := dt2.Get("tx-1"); !ok {
		t.Fatal("dedup key lost across restore")
	}
	// Offsets round-trip (ADR-0048).
	got := OffsetsSliceToMap(loaded.Offsets)
	if len(got) != 2 || got[0] != 100 || got[2] != 55 {
		t.Fatalf("offsets round-trip: %v", got)
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
	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)

	// Seed balance and reserve against it.
	if _, err := state.ApplyTransfer(engine.TransferRequest{
		UserID: "u1", Asset: "USDT", Amount: dec.New("1000"), Type: engine.TransferDeposit,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.CreateReservation("u1", "USDT", "cond-42", dec.New("100")); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	store := newFSStore(t)
	if err := Save(ctx, store, "shard-0", Capture(0, state, seq, dt, nil, 0), FormatProto); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(ctx, store, "shard-0")
	if err != nil {
		t.Fatal(err)
	}

	state2 := engine.NewShardState(0)
	seq2 := sequencer.New()
	dt2 := dedup.New(time.Hour)
	if err := Restore(0, state2, seq2, dt2, loaded); err != nil {
		t.Fatal(err)
	}
	r := state2.LookupReservation("cond-42")
	if r == nil || r.Amount.String() != "100" || r.Asset != "USDT" || r.UserID != "u1" {
		t.Fatalf("reservation round-trip lost: %+v", r)
	}
	if bal := state2.Balance("u1", "USDT"); bal.Available.String() != "900" || bal.Frozen.String() != "100" {
		t.Errorf("balance: %+v", bal)
	}
}

// TestCaptureRestore_TerminatedRing_Proto verifies the ADR-0062 ring
// survives proto-format serialisation round-trip and lookups behave
// identically before / after restore.
func TestCaptureRestore_TerminatedRing_Proto(t *testing.T) {
	testCaptureRestoreTerminatedRing(t, FormatProto)
}

// TestCaptureRestore_TerminatedRing_JSON covers the debug format
// (ADR-0049) so the JSON schema extension is kept in sync.
func TestCaptureRestore_TerminatedRing_JSON(t *testing.T) {
	testCaptureRestoreTerminatedRing(t, FormatJSON)
}

func testCaptureRestoreTerminatedRing(t *testing.T, format Format) {
	t.Helper()
	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)

	// Seed u1 with a handful of evicted-terminal-order entries straight
	// into the ring. The evictor goroutine will populate these in later
	// milestones; here we exercise the snapshot round-trip directly.
	acc := state.Account("u1")
	for i := uint64(1); i <= 5; i++ {
		acc.RememberTerminated(engine.TerminatedOrderEntry{
			OrderID:       i,
			FinalStatus:   engine.OrderStatusCanceled,
			TerminatedAt:  int64(i) * 1000,
			ClientOrderID: "coid-" + string(rune('A'+int(i)-1)),
			Symbol:        "BTC-USDT",
		})
	}
	// u2 stays empty — verifies absent field restores clean.
	_ = state.Account("u2")

	snap := Capture(0, state, seq, dt, nil, 0)

	// Sanity: captured snapshot carries the ring oldest → newest.
	var u1 *AccountSnapshot
	for i := range snap.Accounts {
		if snap.Accounts[i].UserID == "u1" {
			u1 = &snap.Accounts[i]
			break
		}
	}
	if u1 == nil {
		t.Fatal("u1 missing from capture")
	}
	if got := len(u1.RecentTerminatedOrders); got != 5 {
		t.Fatalf("capture ring len = %d, want 5", got)
	}
	for i, entry := range u1.RecentTerminatedOrders {
		wantID := uint64(i + 1)
		if entry.OrderID != wantID {
			t.Fatalf("capture[%d] order_id = %d, want %d", i, entry.OrderID, wantID)
		}
		if entry.FinalStatus != uint8(engine.OrderStatusCanceled) {
			t.Fatalf("capture[%d] final_status = %d", i, entry.FinalStatus)
		}
	}

	// Persist + reload through the requested format.
	ctx := context.Background()
	store := newFSStore(t)
	if err := Save(ctx, store, "shard-0", snap, format); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(ctx, store, "shard-0")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	state2 := engine.NewShardState(0)
	seq2 := sequencer.New()
	dt2 := dedup.New(time.Hour)
	if err := Restore(0, state2, seq2, dt2, loaded); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Every captured id should be retrievable from the restored ring.
	restored := state2.Account("u1")
	for i := uint64(1); i <= 5; i++ {
		got, ok := restored.LookupTerminated(i)
		if !ok {
			t.Fatalf("id %d missing after %s restore", i, format)
		}
		if got.OrderID != i || got.FinalStatus != engine.OrderStatusCanceled ||
			got.TerminatedAt != int64(i)*1000 ||
			got.Symbol != "BTC-USDT" {
			t.Fatalf("id %d payload corrupted after %s restore: %+v", i, format, got)
		}
	}
	// u2's ring should be empty (no entries captured → no restore work).
	if _, ok := state2.Account("u2").LookupTerminated(42); ok {
		t.Fatalf("u2 restored ring should be empty")
	}
}

// TestCaptureRestore_TerminatedRing_LegacySnapshotCompat verifies that
// loading a snapshot written before ADR-0062 (no recent_terminated_orders
// field) produces empty rings for all accounts, not an error.
func TestCaptureRestore_TerminatedRing_LegacySnapshotCompat(t *testing.T) {
	// Build a pre-ADR-0062 shaped snapshot by hand (no ring field).
	legacy := &ShardSnapshot{
		Version:     Version,
		ShardID:     0,
		TimestampMS: 1,
		Accounts: []AccountSnapshot{
			{
				UserID: "u1",
				Balances: []BalanceSnapshot{
					{Asset: "USDT", Available: "100", Frozen: "0"},
				},
			},
		},
	}

	ctx := context.Background()
	store := newFSStore(t)
	if err := Save(ctx, store, "shard-0", legacy, FormatProto); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(ctx, store, "shard-0")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	if err := Restore(0, state, seq, dt, loaded); err != nil {
		t.Fatalf("Restore legacy snapshot: %v", err)
	}
	// Ring is empty — any lookup misses. Subsequent Remember works.
	acc := state.Account("u1")
	if _, ok := acc.LookupTerminated(1); ok {
		t.Fatal("empty ring should miss after legacy-snapshot restore")
	}
	acc.RememberTerminated(engine.TerminatedOrderEntry{OrderID: 1, FinalStatus: engine.OrderStatusCanceled})
	if _, ok := acc.LookupTerminated(1); !ok {
		t.Fatal("post-restore Remember then Lookup should hit")
	}
}

func TestRestoreRejectsVersionMismatch(t *testing.T) {
	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	snap := &ShardSnapshot{Version: 99, ShardID: 0}
	if err := Restore(0, state, seq, dt, snap); err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestRestoreRejectsShardMismatch(t *testing.T) {
	state := engine.NewShardState(1)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	snap := &ShardSnapshot{Version: Version, ShardID: 2}
	if err := Restore(1, state, seq, dt, snap); err == nil {
		t.Fatal("expected shard mismatch error")
	}
}

// TestFSBlobStore_GetMissingReturnsErrNotExist is the cold-start contract:
// a fresh store must surface absence as os.ErrNotExist so callers can
// branch on errors.Is.
func TestFSBlobStore_GetMissingReturnsErrNotExist(t *testing.T) {
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
