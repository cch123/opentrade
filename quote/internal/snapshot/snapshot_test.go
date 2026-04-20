package snapshot

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSaveLoad_RoundTrip_Proto(t *testing.T) {
	testSaveLoadRoundTrip(t, FormatProto)
}

func TestSaveLoad_RoundTrip_JSON(t *testing.T) {
	testSaveLoadRoundTrip(t, FormatJSON)
}

func testSaveLoadRoundTrip(t *testing.T, format Format) {
	dir := t.TempDir()
	base := filepath.Join(dir, "state")
	src := &Snapshot{
		Version:   Version,
		TakenAtMs: 1700,
		QuoteSeq:  42,
		Offsets:   map[int32]int64{0: 100, 1: 205},
		Symbols: map[string]*SymbolSnapshot{
			"BTC-USDT": {
				Kline: &KlineSnapshot{
					Symbol: "BTC-USDT",
					Bars: map[int32]BarSnap{
						1: {OpenTimeMs: 60_000, CloseTimeMs: 120_000, Open: "100", High: "101", Low: "99", Close: "100", Volume: "1", QuoteVolume: "100", Count: 2},
					},
				},
			},
		},
	}
	if err := Save(base, src, format); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(base)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(src, loaded) {
		t.Errorf("round trip differs:\n src=%+v\n dst=%+v", src, loaded)
	}
}

func TestLoad_MissingFileReturnsNil(t *testing.T) {
	snap, err := Load(filepath.Join(t.TempDir(), "nope"))
	if err != nil {
		t.Fatalf("err = %v; missing file should be nil, nil", err)
	}
	if snap != nil {
		t.Errorf("snap = %+v, want nil", snap)
	}
}

func TestLoad_VersionMismatch(t *testing.T) {
	base := filepath.Join(t.TempDir(), "state")
	if err := Save(base, &Snapshot{Version: Version + 1}, FormatProto); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(base); err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestSave_AtomicRename(t *testing.T) {
	// A tmp file must not linger after a successful write.
	base := filepath.Join(t.TempDir(), "state")
	if err := Save(base, &Snapshot{Version: Version, Offsets: map[int32]int64{}}, FormatProto); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(base + ".pb.tmp"); !os.IsNotExist(err) {
		t.Errorf("tmp file lingered: err=%v", err)
	}
}

// TestLoad_JSONOnlyMigration verifies Load still reads legacy .json files
// when only that format is present (ADR-0049 probe order .pb → .json).
func TestLoad_JSONOnlyMigration(t *testing.T) {
	base := filepath.Join(t.TempDir(), "state")
	src := &Snapshot{
		Version:   Version,
		TakenAtMs: 7,
		Offsets:   map[int32]int64{},
	}
	if err := Save(base, src, FormatJSON); err != nil {
		t.Fatal(err)
	}
	snap, err := Load(base)
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil || snap.TakenAtMs != 7 {
		t.Fatalf("json-only load: got %+v", snap)
	}
}
