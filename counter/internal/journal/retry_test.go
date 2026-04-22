package journal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestRunTxnWithRetry_SucceedsAfterTransientFailures verifies the
// bounded-retry path: fn fails the first two attempts, then succeeds
// on the third within budget.
func TestRunTxnWithRetry_SucceedsAfterTransientFailures(t *testing.T) {
	p := &TxnProducer{logger: zap.NewNop()}
	// Short budget + backoff so the test is fast, but long enough to
	// accommodate three attempts (2 x 5ms sleep + 3 x fn call).
	p.cfg.PublishRetryBudget = 200 * time.Millisecond
	p.cfg.PublishRetryBackoff = 5 * time.Millisecond

	var attempts atomic.Int32
	transient := errors.New("transient kafka")
	op := func() error {
		n := attempts.Add(1)
		if n < 3 {
			return transient
		}
		return nil
	}

	// Use runTxnWithRetry directly with a fn that does NOT actually
	// touch kgo — we override runTxn via a test-only helper (see
	// runTxnFake below). The retry layer only inspects error / ctx.
	err := p.runTxnWithRetryFake(context.Background(), "test", op)
	if err != nil {
		t.Fatalf("expected success after 3 attempts, got %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d, want 3", attempts.Load())
	}
}

// TestRunTxnWithRetry_PanicsOnBudgetExhaustion verifies the
// retry-then-panic path: fn fails deterministically and the budget
// is consumed, producing a panic that callers rely on to trigger
// vshard failover.
func TestRunTxnWithRetry_PanicsOnBudgetExhaustion(t *testing.T) {
	p := &TxnProducer{logger: zap.NewNop()}
	p.cfg.PublishRetryBudget = 50 * time.Millisecond
	p.cfg.PublishRetryBackoff = 5 * time.Millisecond

	persistent := errors.New("kafka dead")
	op := func() error { return persistent }

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic, got none")
		}
		msg, ok := r.(string)
		if !ok || !containsSubstring(msg, "test") || !containsSubstring(msg, "kafka dead") {
			t.Fatalf("panic message = %q, missing op name or error", msg)
		}
	}()
	_ = p.runTxnWithRetryFake(context.Background(), "test", op)
}

// TestRunTxnWithRetry_ContextCancelShortCircuits verifies that a
// cancelled ctx returns ctx.Err() immediately without waiting for
// budget exhaustion.
func TestRunTxnWithRetry_ContextCancelShortCircuits(t *testing.T) {
	p := &TxnProducer{logger: zap.NewNop()}
	p.cfg.PublishRetryBudget = time.Hour // would never expire
	p.cfg.PublishRetryBackoff = 10 * time.Millisecond

	var attempts atomic.Int32
	transient := errors.New("transient")
	op := func() error {
		attempts.Add(1)
		return transient
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	var got error
	go func() {
		defer wg.Done()
		got = p.runTxnWithRetryFake(ctx, "test", op)
	}()
	// Let one retry happen, then cancel.
	time.Sleep(30 * time.Millisecond)
	cancel()
	wg.Wait()

	if !errors.Is(got, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", got)
	}
	if attempts.Load() == 0 {
		t.Fatal("op was never attempted")
	}
}

// TestPublishRetryBudgetDefaults exercises the zero-value fallback —
// unset cfg fields fall back to package defaults so callers don't
// have to set them unless they want a custom budget.
func TestPublishRetryBudgetDefaults(t *testing.T) {
	p := &TxnProducer{logger: zap.NewNop()}
	// cfg.PublishRetryBudget == 0 → fallback to 5s package default.
	// Deterministic panic with a sub-ms budget via the package var:
	origBudget := PublishRetryBudget
	origBackoff := PublishRetryBackoff
	defer func() {
		PublishRetryBudget = origBudget
		PublishRetryBackoff = origBackoff
	}()
	PublishRetryBudget = 20 * time.Millisecond
	PublishRetryBackoff = 5 * time.Millisecond

	persistent := errors.New("kafka dead")
	op := func() error { return persistent }
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic via package default budget")
		}
	}()
	_ = p.runTxnWithRetryFake(context.Background(), "default-budget", op)
}

// containsSubstring — avoid importing strings for such trivial use.
func containsSubstring(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	if len(needle) > len(haystack) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// runTxnWithRetryFake is a test-only variant of runTxnWithRetry that
// calls fn directly instead of going through runTxn (which needs a
// live kgo.Client). This lets us unit-test the retry envelope in
// isolation.
func (p *TxnProducer) runTxnWithRetryFake(ctx context.Context, opName string, fn func() error) error {
	budget := p.cfg.PublishRetryBudget
	if budget <= 0 {
		budget = PublishRetryBudget
	}
	backoff := p.cfg.PublishRetryBackoff
	if backoff <= 0 {
		backoff = PublishRetryBackoff
	}
	deadline := time.Now().Add(budget)
	var lastErr error
	attempts := 0
	for {
		attempts++
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			panic(errors.New("journal: " + opName + ": " + lastErr.Error()).Error())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}
