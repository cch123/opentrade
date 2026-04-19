package rollout

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// fixedClock is a deterministic clock for tests.
type fixedClock struct{ t time.Time }

func (c *fixedClock) Now() time.Time { return c.t }

// seed returns a cfg with a scheduled change effective at `when`.
func seed(when time.Time) etcdcfg.SymbolConfig {
	oldTiers := []etcdcfg.PrecisionTier{{
		PriceFrom: dec.New("0"), PriceTo: dec.New("0"),
		TickSize: dec.New("0.01"), StepSize: dec.New("0.00001"),
	}}
	newTiers := []etcdcfg.PrecisionTier{
		{PriceFrom: dec.New("0"), PriceTo: dec.New("1000"),
			TickSize: dec.New("0.01"), StepSize: dec.New("0.00001")},
		{PriceFrom: dec.New("1000"), PriceTo: dec.New("0"),
			TickSize: dec.New("1"), StepSize: dec.New("0.0001")},
	}
	return etcdcfg.SymbolConfig{
		Shard:            "match-0",
		Trading:          true,
		BaseAsset:        "BTC",
		QuoteAsset:       "USDT",
		PrecisionVersion: 1,
		Tiers:            oldTiers,
		ScheduledChange: &etcdcfg.PrecisionChange{
			EffectiveAt:         when,
			NewTiers:            newTiers,
			NewPrecisionVersion: 2,
			Reason:              "tier split",
		},
	}
}

func newRunner(t *testing.T, clock *fixedClock, mem *etcdcfg.MemorySource, audit adminaudit.Logger) *Runner {
	t.Helper()
	r, err := New(Config{
		Etcd:         memoryAdapter{mem},
		Audit:        audit,
		Logger:       zap.NewNop(),
		Clock:        clock,
		ScanInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// memoryAdapter wraps MemorySource to satisfy the rollout.EtcdSource
// interface (List + Put with the expected ctx-only signatures).
type memoryAdapter struct{ s *etcdcfg.MemorySource }

func (m memoryAdapter) List(ctx context.Context) (map[string]etcdcfg.SymbolConfig, int64, error) {
	return m.s.List(ctx)
}
func (m memoryAdapter) Put(ctx context.Context, symbol string, cfg etcdcfg.SymbolConfig) (int64, error) {
	return m.s.PutCtx(ctx, symbol, cfg)
}

func TestScan_DueRolloutExecutes(t *testing.T) {
	mem := etcdcfg.NewMemorySource()
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	// Effective 1 minute in the past → due.
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT", seed(now.Add(-time.Minute)))

	audit := &memoryAuditLogger{}
	r := newRunner(t, &fixedClock{now}, mem, audit)

	if err := r.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}

	cfgs, _, _ := mem.List(context.Background())
	got := cfgs["BTC-USDT"]
	if got.PrecisionVersion != 2 {
		t.Errorf("PrecisionVersion=%d want 2", got.PrecisionVersion)
	}
	if got.ScheduledChange != nil {
		t.Errorf("ScheduledChange not cleared: %+v", got.ScheduledChange)
	}
	if len(got.Tiers) != 2 {
		t.Errorf("Tiers not swapped: %d", len(got.Tiers))
	}
	if len(audit.entries) != 1 || audit.entries[0].Op != "admin.precision.rolled_out" {
		t.Errorf("audit: %+v", audit.entries)
	}
	params := audit.entries[0].Params
	if params["from_precision_version"].(uint64) != 1 {
		t.Errorf("from_version: %v", params["from_precision_version"])
	}
	if params["to_precision_version"].(uint64) != 2 {
		t.Errorf("to_version: %v", params["to_precision_version"])
	}
}

func TestScan_FutureRolloutIgnored(t *testing.T) {
	mem := etcdcfg.NewMemorySource()
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT", seed(now.Add(time.Hour)))

	audit := &memoryAuditLogger{}
	r := newRunner(t, &fixedClock{now}, mem, audit)
	if err := r.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}

	cfgs, _, _ := mem.List(context.Background())
	got := cfgs["BTC-USDT"]
	if got.PrecisionVersion != 1 {
		t.Errorf("PrecisionVersion bumped early: %d", got.PrecisionVersion)
	}
	if got.ScheduledChange == nil {
		t.Error("ScheduledChange cleared early")
	}
	if len(audit.entries) != 0 {
		t.Errorf("no audit expected yet: %+v", audit.entries)
	}
}

func TestScan_IdempotentAfterExecution(t *testing.T) {
	mem := etcdcfg.NewMemorySource()
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT", seed(now.Add(-time.Minute)))

	audit := &memoryAuditLogger{}
	r := newRunner(t, &fixedClock{now}, mem, audit)

	// First scan swaps.
	_ = r.Scan(context.Background())
	// Second scan should be a no-op because ScheduledChange is cleared.
	_ = r.Scan(context.Background())

	if n := len(audit.entries); n != 1 {
		t.Errorf("second scan should not audit; got %d entries", n)
	}
}

func TestScan_NoScheduledChangeSkipped(t *testing.T) {
	mem := etcdcfg.NewMemorySource()
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT", etcdcfg.SymbolConfig{
		Shard: "match-0", Trading: true,
	})
	audit := &memoryAuditLogger{}
	r := newRunner(t, &fixedClock{time.Now()}, mem, audit)
	if err := r.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(audit.entries) != 0 {
		t.Errorf("no work expected; got audit %+v", audit.entries)
	}
}

func TestScan_MultipleSymbols(t *testing.T) {
	mem := etcdcfg.NewMemorySource()
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT", seed(now.Add(-time.Minute)))
	_, _ = mem.PutCtx(context.Background(), "ETH-USDT", seed(now.Add(time.Hour))) // not due
	_, _ = mem.PutCtx(context.Background(), "SOL-USDT", seed(now.Add(-time.Second)))

	audit := &memoryAuditLogger{}
	r := newRunner(t, &fixedClock{now}, mem, audit)
	_ = r.Scan(context.Background())

	cfgs, _, _ := mem.List(context.Background())
	if cfgs["BTC-USDT"].PrecisionVersion != 2 {
		t.Error("BTC-USDT not rolled")
	}
	if cfgs["ETH-USDT"].PrecisionVersion != 1 {
		t.Error("ETH-USDT incorrectly rolled (future)")
	}
	if cfgs["SOL-USDT"].PrecisionVersion != 2 {
		t.Error("SOL-USDT not rolled")
	}
	if len(audit.entries) != 2 {
		t.Errorf("audit count: %d want 2", len(audit.entries))
	}
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	mem := etcdcfg.NewMemorySource()
	audit := &memoryAuditLogger{}
	r := newRunner(t, &fixedClock{time.Now()}, mem, audit)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err == nil || err != context.Canceled {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestNew_RequiresEtcd(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("expected error without Etcd")
	}
}

// ---- test helpers ----------------------------------------------------------

type memoryAuditLogger struct {
	entries []adminaudit.Entry
	calls   atomic.Int64
}

func (m *memoryAuditLogger) Log(e adminaudit.Entry) error {
	m.calls.Add(1)
	m.entries = append(m.entries, e)
	return nil
}
func (m *memoryAuditLogger) Close() error { return nil }
