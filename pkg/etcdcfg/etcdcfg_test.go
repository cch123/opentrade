package etcdcfg

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xargin/opentrade/pkg/dec"
)

func TestDecodeValue(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    SymbolConfig
		wantErr bool
	}{
		{"full", `{"shard":"match-0","trading":true,"version":"v1"}`,
			SymbolConfig{Shard: "match-0", Trading: true, Version: "v1"}, false},
		{"trading-false", `{"shard":"match-0","trading":false}`,
			SymbolConfig{Shard: "match-0", Trading: false}, false},
		{"missing-optional", `{"shard":"match-0"}`,
			SymbolConfig{Shard: "match-0"}, false},
		// ADR-0053 M0: old JSON format (pre-precision fields) must
		// round-trip into the expanded struct with zero-value precision
		// fields — this is the "compatibility mode" gate.
		{"pre-adr0053-format", `{"shard":"match-1","trading":true,"version":"v2"}`,
			SymbolConfig{Shard: "match-1", Trading: true, Version: "v2"}, false},
		{"empty", "", SymbolConfig{}, true},
		{"bad-json", "{not-json", SymbolConfig{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := DecodeValue([]byte(c.raw))
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if err == nil && !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %+v want %+v", got, c.want)
			}
		})
	}
}

func TestSymbolFromKey(t *testing.T) {
	if got := SymbolFromKey("/cex/match/symbols/BTC-USDT", DefaultPrefix); got != "BTC-USDT" {
		t.Errorf("got %q", got)
	}
	if got := SymbolFromKey("/different/key", DefaultPrefix); got != "" {
		t.Errorf("expected empty for non-matching prefix: %q", got)
	}
}

func TestOwned(t *testing.T) {
	cases := []struct {
		cfg     SymbolConfig
		shard   string
		want    bool
	}{
		{SymbolConfig{Shard: "match-0", Trading: true}, "match-0", true},
		{SymbolConfig{Shard: "match-0", Trading: false}, "match-0", false},
		{SymbolConfig{Shard: "match-0", Trading: true}, "match-1", false},
		{SymbolConfig{}, "", false},
	}
	for _, c := range cases {
		if got := c.cfg.Owned(c.shard); got != c.want {
			t.Errorf("%+v.Owned(%q)=%v want %v", c.cfg, c.shard, got, c.want)
		}
	}
}

func TestMemorySource_ListAndWatch(t *testing.T) {
	s := NewMemorySource()
	s.Put("BTC-USDT", SymbolConfig{Shard: "match-0", Trading: true})
	s.Put("ETH-USDT", SymbolConfig{Shard: "match-1", Trading: true})

	snap, rev, err := s.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap) != 2 || snap["BTC-USDT"].Shard != "match-0" {
		t.Fatalf("snap: %+v", snap)
	}
	if rev <= 0 {
		t.Errorf("revision: %d", rev)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch, err := s.Watch(ctx, rev+1)
	if err != nil {
		t.Fatal(err)
	}

	// Put after watch starts — expect event.
	s.Put("BTC-USDT", SymbolConfig{Shard: "match-0", Trading: false})

	select {
	case ev := <-watch:
		if ev.Type != EventPut || ev.Symbol != "BTC-USDT" || ev.Config.Trading {
			t.Errorf("event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for put event")
	}

	// Delete.
	s.Delete("ETH-USDT")
	select {
	case ev := <-watch:
		if ev.Type != EventDelete || ev.Symbol != "ETH-USDT" {
			t.Errorf("event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for delete event")
	}
}

func TestMemorySource_WatchFromRevisionSkipsOldEvents(t *testing.T) {
	s := NewMemorySource()
	s.Put("BTC-USDT", SymbolConfig{Shard: "match-0", Trading: true}) // rev 1
	s.Put("ETH-USDT", SymbolConfig{Shard: "match-0", Trading: true}) // rev 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Request events strictly after rev 2.
	watch, err := s.Watch(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	// Mutation at rev 3 should be delivered.
	s.Delete("BTC-USDT")
	select {
	case ev := <-watch:
		if ev.Symbol != "BTC-USDT" || ev.Type != EventDelete {
			t.Errorf("unexpected: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestMemorySource_ClosedOnCancel(t *testing.T) {
	s := NewMemorySource()
	ctx, cancel := context.WithCancel(context.Background())
	watch, err := s.Watch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-watch:
		if ok {
			t.Errorf("expected channel closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestValidateSymbol(t *testing.T) {
	cases := []struct {
		in string
		ok bool
	}{
		{"BTC-USDT", true},
		{"ETH_USDT", true},
		{"abc.def", true},
		{"", false},
		{"a/b", false},
		{"a b", false},
		{"a\nb", false},
		{"a\x00b", false},
	}
	for _, c := range cases {
		err := ValidateSymbol(c.in)
		if (err == nil) != c.ok {
			t.Errorf("%q: ok=%v err=%v", c.in, c.ok, err)
		}
	}
}

func TestMemorySource_PutCtxDeleteCtx(t *testing.T) {
	s := NewMemorySource()
	ctx := context.Background()

	rev1, err := s.PutCtx(ctx, "BTC-USDT", SymbolConfig{Shard: "match-0", Trading: true})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if rev1 <= 0 {
		t.Fatalf("want rev > 0, got %d", rev1)
	}

	cfgs, rev2, err := s.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rev2 != rev1 {
		t.Fatalf("revs: put=%d list=%d", rev1, rev2)
	}
	if cfgs["BTC-USDT"].Shard != "match-0" {
		t.Fatalf("list: %+v", cfgs)
	}

	existed, _, err := s.DeleteCtx(ctx, "BTC-USDT")
	if err != nil || !existed {
		t.Fatalf("delete: existed=%v err=%v", existed, err)
	}
	existed, _, err = s.DeleteCtx(ctx, "BTC-USDT")
	if err != nil || existed {
		t.Fatalf("idempotent delete: existed=%v err=%v", existed, err)
	}

	if _, err := s.PutCtx(ctx, "bad/sym", SymbolConfig{}); err == nil {
		t.Fatal("expected invalid-symbol error")
	}
}

func TestMemorySource_PutCtxCancelled(t *testing.T) {
	s := NewMemorySource()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.PutCtx(ctx, "BTC-USDT", SymbolConfig{}); err == nil {
		t.Fatal("expected cancelled error")
	}
}

// ADR-0053 M0: a SymbolConfig with no precision fields should marshal back
// to the legacy JSON shape (no tiers / base_asset / precision_version keys),
// so live etcd values stay byte-identical on round-trip through admin writes.
func TestSymbolConfig_LegacyMarshalUnchanged(t *testing.T) {
	cfg := SymbolConfig{Shard: "match-0", Trading: true, Version: "v1"}
	buf, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := string(buf)
	for _, banned := range []string{"tiers", "base_asset", "quote_asset", "precision_version", "scheduled_change"} {
		if strings.Contains(got, banned) {
			t.Errorf("legacy cfg marshalled to %q, must not contain %q", got, banned)
		}
	}
}

// Round-trip a full precision-carrying SymbolConfig: marshal → unmarshal
// must recover the exact struct (including Decimal values).
func TestSymbolConfig_PrecisionRoundTrip(t *testing.T) {
	effective, _ := time.Parse(time.RFC3339, "2026-05-01T00:00:00Z")
	original := SymbolConfig{
		Shard:            "match-0",
		Trading:          true,
		Version:          "v3",
		BaseAsset:        "BTC",
		QuoteAsset:       "USDT",
		PrecisionVersion: 2,
		Tiers: []PrecisionTier{
			{
				PriceFrom:                     dec.New("0"),
				PriceTo:                       dec.New("0"),
				TickSize:                      dec.New("0.01"),
				StepSize:                      dec.New("0.00001"),
				QuoteStepSize:                 dec.New("0.01"),
				MinQty:                        dec.New("0.0001"),
				MaxQty:                        dec.New("1000"),
				MinQuoteQty:                   dec.New("1"),
				MinQuoteAmount:                dec.New("5"),
				MarketMinQty:                  dec.New("0.0001"),
				MarketMaxQty:                  dec.New("500"),
				EnforceMinQuoteAmountOnMarket: true,
				AvgPriceMins:                  5,
			},
		},
		ScheduledChange: &PrecisionChange{
			EffectiveAt:         effective,
			NewPrecisionVersion: 3,
			Reason:              "price surge",
			NewTiers: []PrecisionTier{
				{
					PriceFrom: dec.New("0"),
					PriceTo:   dec.New("0"),
					TickSize:  dec.New("0.1"),
					StepSize:  dec.New("0.0001"),
				},
			},
		},
	}
	buf, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored SymbolConfig
	if err := json.Unmarshal(buf, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Use DeepEqual after normalising Decimal — shopspring/decimal's zero
	// state and JSON-roundtripped state can differ internally (value=nil
	// vs value=bigInt(0)), so compare via String() for each decimal.
	if !jsonEqual(t, original, restored) {
		t.Errorf("round-trip mismatch:\n got=%+v\nwant=%+v", restored, original)
	}
	// HasPrecision should survive the trip.
	if !restored.HasPrecision() {
		t.Error("restored cfg lost Tiers → HasPrecision=false")
	}
}

// jsonEqual is a DeepEqual that tolerates Decimal internal representation
// drift by re-marshalling both sides and comparing bytes.
func jsonEqual(t *testing.T, a, b any) bool {
	t.Helper()
	ba, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return string(ba) == string(bb)
}

// Compatibility: a cfg with zero-valued precision fields (omitempty) must
// NOT emit precision keys, so legacy watchers parsing old formats don't
// suddenly see unfamiliar keys.
func TestSymbolConfig_ZeroPrecisionOmitted(t *testing.T) {
	cfg := SymbolConfig{Shard: "match-0", Trading: true}
	buf, _ := json.Marshal(cfg)
	want := `{"shard":"match-0","trading":true}`
	if string(buf) != want {
		t.Errorf("got  %s\nwant %s", string(buf), want)
	}
}
