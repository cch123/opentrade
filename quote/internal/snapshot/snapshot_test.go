package snapshot

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSaveLoad_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	src := &Snapshot{
		Version:   Version,
		TakenAtMs: 1700,
		Seq:       42,
		Offsets:   map[int32]int64{0: 100, 1: 205},
		Symbols: map[string]*SymbolSnapshot{
			"BTC-USDT": {
				Depth: &DepthSnapshot{
					Symbol: "BTC-USDT",
					Bids:   map[string]string{"99": "3"},
					Asks:   map[string]string{"101": "5"},
					Prices: map[string]string{"99": "99", "101": "101"},
					Orders: []OrderRefSnap{
						{OrderID: 10, Side: 1, PriceKey: "99", Remaining: "3"},
						{OrderID: 11, Side: 2, PriceKey: "101", Remaining: "5"},
					},
				},
				Kline: &KlineSnapshot{
					Symbol: "BTC-USDT",
					Bars: map[int32]BarSnap{
						1: {OpenTimeMs: 60_000, CloseTimeMs: 120_000, Open: "100", High: "101", Low: "99", Close: "100", Volume: "1", QuoteVolume: "100", Count: 2},
					},
				},
			},
		},
	}
	if err := Save(path, src); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(src, loaded) {
		t.Errorf("round trip differs:\n src=%+v\n dst=%+v", src, loaded)
	}
}

func TestLoad_MissingFileReturnsNil(t *testing.T) {
	snap, err := Load(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatalf("err = %v; missing file should be nil, nil", err)
	}
	if snap != nil {
		t.Errorf("snap = %+v, want nil", snap)
	}
}

func TestLoad_VersionMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := Save(path, &Snapshot{Version: Version + 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestSave_AtomicRename(t *testing.T) {
	// A tmp file must not linger after a successful write.
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := Save(path, &Snapshot{Version: Version, Offsets: map[int32]int64{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("tmp file lingered: err=%v", err)
	}
}
