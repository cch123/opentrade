package symregistry

import (
	"context"
	"testing"
	"time"

	"github.com/xargin/opentrade/pkg/etcdcfg"
)

func TestRegistryGetPutDelete(t *testing.T) {
	r := New()
	if _, ok := r.Get("BTC-USDT"); ok {
		t.Fatal("empty registry should miss")
	}
	r.Put("BTC-USDT", etcdcfg.SymbolConfig{Shard: "match-0", Trading: true})
	cfg, ok := r.Get("BTC-USDT")
	if !ok || cfg.Shard != "match-0" {
		t.Fatalf("got %+v ok=%v", cfg, ok)
	}
	if n := r.Len(); n != 1 {
		t.Errorf("Len=%d", n)
	}
	r.Delete("BTC-USDT")
	if _, ok := r.Get("BTC-USDT"); ok {
		t.Error("after delete should miss")
	}
}

func TestRegistryRun_SeedsAndWatches(t *testing.T) {
	mem := etcdcfg.NewMemorySource()
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT",
		etcdcfg.SymbolConfig{Shard: "match-0", Trading: true})
	_, _ = mem.PutCtx(context.Background(), "ETH-USDT",
		etcdcfg.SymbolConfig{Shard: "match-1", Trading: false, Version: "v2"})

	r := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(ctx, mem) }()

	// Spin until seed arrives (Run does seed then subscribes; seed is sync
	// but watch setup has a slice append under mu in MemorySource).
	deadline := time.After(2 * time.Second)
	for r.Len() < 2 {
		select {
		case <-deadline:
			t.Fatal("seed never populated")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if cfg, ok := r.Get("ETH-USDT"); !ok || cfg.Version != "v2" {
		t.Fatalf("seeded ETH-USDT wrong: %+v ok=%v", cfg, ok)
	}

	// Live update.
	_, _ = mem.PutCtx(context.Background(), "SOL-USDT",
		etcdcfg.SymbolConfig{Shard: "match-0", Trading: true})
	for {
		if _, ok := r.Get("SOL-USDT"); ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("live SOL-USDT put not observed")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Live delete.
	mem.Delete("BTC-USDT")
	for {
		if _, ok := r.Get("BTC-USDT"); !ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("live delete not observed")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil && err != context.Canceled {
			t.Errorf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Run did not exit after ctx cancel")
	}
}
