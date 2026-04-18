package snapshot

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

func TestSaveLoadRoundTrip_Proto(t *testing.T) {
	testSaveLoadRoundTrip(t, FormatProto)
}

func TestSaveLoadRoundTrip_JSON(t *testing.T) {
	testSaveLoadRoundTrip(t, FormatJSON)
}

func testSaveLoadRoundTrip(t *testing.T, format Format) {
	base := filepath.Join(t.TempDir(), "counter-shard-0")
	snap := &ShardSnapshot{
		Version:     Version,
		ShardID:     0,
		ShardSeq:    99,
		TimestampMS: 1,
		Accounts: []AccountSnapshot{{
			UserID: "u1",
			Balances: []BalanceSnapshot{
				{Asset: "USDT", Available: "100", Frozen: "50"},
			},
		}},
	}
	if err := Save(base, snap, format); err != nil {
		t.Fatal(err)
	}
	got, err := Load(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.ShardID != 0 || got.ShardSeq != 99 || len(got.Accounts) != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

// TestSaveLoadRoundTrip_ProtoPreferredOverJSON verifies that when both
// formats live on disk, Load prefers the proto file (ADR-0049 probe order).
func TestSaveLoadRoundTrip_ProtoPreferredOverJSON(t *testing.T) {
	base := filepath.Join(t.TempDir(), "counter-shard-0")
	protoSnap := &ShardSnapshot{Version: Version, ShardID: 0, ShardSeq: 7}
	jsonSnap := &ShardSnapshot{Version: Version, ShardID: 0, ShardSeq: 42}
	if err := Save(base, protoSnap, FormatProto); err != nil {
		t.Fatal(err)
	}
	if err := Save(base, jsonSnap, FormatJSON); err != nil {
		t.Fatal(err)
	}
	got, err := Load(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.ShardSeq != 7 {
		t.Fatalf("expected proto (seq=7) to win over json (seq=42), got %d", got.ShardSeq)
	}
}

// TestLoad_JSONOnlyMigration covers the upgrade path: only .json on disk,
// Load still returns it so the first-start-after-upgrade works.
func TestLoad_JSONOnlyMigration(t *testing.T) {
	base := filepath.Join(t.TempDir(), "counter-shard-0")
	snap := &ShardSnapshot{Version: Version, ShardID: 0, ShardSeq: 42}
	if err := Save(base, snap, FormatJSON); err != nil {
		t.Fatal(err)
	}
	got, err := Load(base)
	if err != nil {
		t.Fatal(err)
	}
	if got.ShardSeq != 42 {
		t.Fatalf("json-only load: got seq=%d", got.ShardSeq)
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
	seq.SetShardSeq(42)
	dt.Set("tx-1", "cached-response")

	// Prime LastMatchSeq on two symbols for u1 (ADR-0048 backlog item 2).
	state.Account("u1").AdvanceMatchSeq("BTC-USDT", 101)
	state.Account("u1").AdvanceMatchSeq("ETH-USDT", 7)

	offsets := map[int32]int64{0: 100, 2: 55}
	snap := Capture(3, state, seq, dt, offsets, 0)

	// Persist and reload (default proto format, ADR-0049).
	base := filepath.Join(t.TempDir(), "shard-3")
	if err := Save(base, snap, FormatProto); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(base)
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

	if seq2.ShardSeq() != 42 {
		t.Fatalf("shard seq = %d, want 42", seq2.ShardSeq())
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
		t.Errorf("u2 account version after restore = %d, want 1", got)
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

	base := filepath.Join(t.TempDir(), "shard-0")
	if err := Save(base, Capture(0, state, seq, dt, nil, 0), FormatProto); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(base)
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
