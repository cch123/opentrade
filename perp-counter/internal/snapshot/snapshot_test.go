package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/perp-counter/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func TestSaveLoad_RoundTrip(t *testing.T) {
	eng := engine.New()
	eng.Deposit("u1", dec.New("1000"))
	eng.Reserve("u1", dec.New("10"))
	eng.ApplyFill("u1", "BTC-USDT-PERP", dec.New("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: dec.New("100"), Qty: dec.New("1")})
	eng.SetMark("BTC-USDT-PERP", dec.New("101"))
	eng.AddInsurance("BTC-USDT-PERP", dec.New("5"))

	in := PerpSnapshot{
		TsUnixMs: 1748505600000,
		Engine:   eng.Snapshot(),
		Service: service.Snapshot{
			PerpSeq: 7, OrderSeq: 4, Offsets: map[int32]int64{0: 100, 1: 250},
		},
	}

	path := filepath.Join(t.TempDir(), "snap.json")
	if err := Save(path, in); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := Load(path)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if got.Version != formatVersion {
		t.Errorf("version = %d, want %d", got.Version, formatVersion)
	}
	if got.TsUnixMs != in.TsUnixMs {
		t.Errorf("ts = %d, want %d", got.TsUnixMs, in.TsUnixMs)
	}
	if got.Service.PerpSeq != 7 || got.Service.OrderSeq != 4 {
		t.Errorf("service seqs = %d/%d", got.Service.PerpSeq, got.Service.OrderSeq)
	}
	if got.Service.Offsets[1] != 250 {
		t.Errorf("offset[1] = %d, want 250", got.Service.Offsets[1])
	}

	// Engine half restores to an equivalent position + insurance.
	eng2 := engine.New()
	eng2.Restore(got.Engine)
	p, okPos := eng2.PositionOf("u1", "BTC-USDT-PERP")
	if !okPos || p.Size.String() != "1" || p.Entry.String() != "100" {
		t.Fatalf("restored position wrong: %+v ok=%v", p, okPos)
	}
	if eng2.InsuranceFund("BTC-USDT-PERP").String() != "5" {
		t.Errorf("restored insurance = %s, want 5", eng2.InsuranceFund("BTC-USDT-PERP"))
	}
}

func TestSave_AtomicOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.json")
	if err := Save(path, PerpSnapshot{Service: service.Snapshot{PerpSeq: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := Save(path, PerpSnapshot{Service: service.Snapshot{PerpSeq: 2}}); err != nil {
		t.Fatal(err)
	}
	got, _, _ := Load(path)
	if got.Service.PerpSeq != 2 {
		t.Fatalf("overwrite failed: PerpSeq = %d, want 2", got.Service.PerpSeq)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp file should not linger after rename")
	}
}

func TestLoad_MissingIsColdStart(t *testing.T) {
	_, ok, err := Load(filepath.Join(t.TempDir(), "absent.json"))
	if err != nil || ok {
		t.Fatalf("missing snapshot should be (false, nil), got ok=%v err=%v", ok, err)
	}
}

func TestLoad_VersionMismatchErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v.json")
	if err := os.WriteFile(path, []byte(`{"version":999}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(path); err == nil {
		t.Fatal("version mismatch must error, not silently mis-restore")
	}
}
