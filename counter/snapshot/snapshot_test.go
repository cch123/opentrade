package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/engine"
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
	snap := Capture(3, state, seq, dt, offsets, 1234, 0)

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
	if err := Save(ctx, store, "shard-0", Capture(0, state, seq, dt, nil, 0, 0), FormatProto); err != nil {
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

// -----------------------------------------------------------------------------
// ADR-0064 M1d BlobLister tests — FSBlobStore side.
// -----------------------------------------------------------------------------

func TestFSBlobStore_ListAndDelete_HappyPath(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewFSBlobStore(dir)

	// Seed a few objects under different prefixes.
	mustPut(t, store, "vshard-001.pb", []byte("periodic-1"))
	mustPut(t, store, "vshard-002.pb", []byte("periodic-2"))
	mustPut(t, store, "vshard-001-ondemand-1700000000000.pb", []byte("ondemand-1"))
	mustPut(t, store, "vshard-002-ondemand-1700000000001.pb", []byte("ondemand-2"))
	mustPut(t, store, "unrelated.pb", []byte("noise"))

	// Prefix filter should pick up periodic + ondemand for
	// vshard-001 but NOT the unrelated.pb object.
	got, err := store.List(ctx, "vshard-001")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	keys := make(map[string]bool, len(got))
	for _, o := range got {
		keys[o.Key] = true
	}
	wantKeys := []string{"vshard-001.pb", "vshard-001-ondemand-1700000000000.pb"}
	for _, k := range wantKeys {
		if !keys[k] {
			t.Errorf("List missing expected key %q; got=%v", k, got)
		}
	}
	if keys["unrelated.pb"] {
		t.Errorf("List returned unrelated.pb despite prefix filter")
	}
	for _, o := range got {
		if o.Size == 0 {
			t.Errorf("List entry %q has zero Size", o.Key)
		}
		if o.LastModified.IsZero() {
			t.Errorf("List entry %q has zero LastModified", o.Key)
		}
	}

	// Delete one on-demand key; subsequent List no longer shows it.
	if err := store.Delete(ctx, "vshard-001-ondemand-1700000000000.pb"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err = store.List(ctx, "vshard-001-ondemand-")
	if err != nil {
		t.Fatalf("List after Delete: %v", err)
	}
	for _, o := range got {
		if o.Key == "vshard-001-ondemand-1700000000000.pb" {
			t.Errorf("deleted key still listed: %+v", o)
		}
	}
}

// TestFSBlobStore_ListEmptyPrefix enumerates every file under the
// baseDir when prefix is empty.
func TestFSBlobStore_ListEmptyPrefix(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewFSBlobStore(dir)
	mustPut(t, store, "a.pb", []byte("a"))
	mustPut(t, store, "b.pb", []byte("b"))
	got, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List(empty prefix): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d: %+v", len(got), got)
	}
}

// TestFSBlobStore_ListSkipsTmpStagingFiles guards the Put atomicity
// implementation detail (tmp+rename) from leaking into housekeeper
// scans. A .tmp sibling MUST NOT show up in List results even if a
// crashed Put left one behind.
func TestFSBlobStore_ListSkipsTmpStagingFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewFSBlobStore(dir)
	mustPut(t, store, "real.pb", []byte("real"))
	// Simulate a crashed Put: stray .tmp file in the dir.
	if err := os.WriteFile(filepath.Join(dir, "stray.pb.tmp"), []byte("half"), 0o644); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}
	got, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, o := range got {
		if strings.HasSuffix(o.Key, ".tmp") {
			t.Errorf("List returned staging file %q", o.Key)
		}
	}
	// real.pb should still surface.
	found := false
	for _, o := range got {
		if o.Key == "real.pb" {
			found = true
		}
	}
	if !found {
		t.Error("real.pb missing from List output")
	}
}

// TestFSBlobStore_ListMissingDir is the cold-start contract: a
// non-existent baseDir is a valid "empty store" (housekeeper sees
// nothing to clean up), not an error.
func TestFSBlobStore_ListMissingDir(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "does-not-exist")
	store := NewFSBlobStore(dir)
	got, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List against missing dir should not error, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %+v", got)
	}
}

// TestFSBlobStore_DeleteMissingIsNoOp — idempotent cleanup.
func TestFSBlobStore_DeleteMissingIsNoOp(t *testing.T) {
	ctx := context.Background()
	store := NewFSBlobStore(t.TempDir())
	if err := store.Delete(ctx, "never-existed.pb"); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}

func mustPut(t *testing.T, store *FSBlobStore, key string, data []byte) {
	t.Helper()
	if err := store.Put(context.Background(), key, data); err != nil {
		t.Fatalf("Put(%s): %v", key, err)
	}
}
