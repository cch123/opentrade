package etcdcfg

import (
	"context"
	"testing"
	"time"
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
		{"empty", "", SymbolConfig{}, true},
		{"bad-json", "{not-json", SymbolConfig{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := DecodeValue([]byte(c.raw))
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
			if err == nil && got != c.want {
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
