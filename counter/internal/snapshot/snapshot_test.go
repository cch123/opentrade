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

func TestSaveLoadRoundTrip(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "counter-shard-0.json")
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
	if err := Save(tmp, snap); err != nil {
		t.Fatal(err)
	}
	got, err := Load(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if got.ShardID != 0 || got.ShardSeq != 99 || len(got.Accounts) != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
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

	offsets := map[int32]int64{0: 100, 2: 55}
	snap := Capture(3, state, seq, dt, offsets, 0)

	// Persist and reload.
	tmp := filepath.Join(t.TempDir(), "shard-3.json")
	if err := Save(tmp, snap); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(tmp)
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

	tmp := filepath.Join(t.TempDir(), "shard-0.json")
	if err := Save(tmp, Capture(0, state, seq, dt, nil, 0)); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(tmp)
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
