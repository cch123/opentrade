package ws

import (
	"testing"
	"time"
)

func TestTokenBucket_ZeroRateAllowsAll(t *testing.T) {
	tb := newTokenBucket(0, 0)
	for i := 0; i < 10; i++ {
		if !tb.allow() {
			t.Fatalf("rate=0 must allow everything (iter %d)", i)
		}
	}
}

func TestTokenBucket_BurstThenThrottle(t *testing.T) {
	tb := newTokenBucket(1, 3) // 1 token/s, burst 3
	// Consume the full burst up front.
	for i := 0; i < 3; i++ {
		if !tb.allow() {
			t.Fatalf("burst[%d] should be allowed", i)
		}
	}
	// Next call must fail — no time has passed.
	if tb.allow() {
		t.Errorf("bucket should be empty after draining burst")
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	tb := newTokenBucket(100, 1) // 100 tokens/s, burst 1
	// Drain.
	if !tb.allow() {
		t.Fatal("initial token should be allowed")
	}
	if tb.allow() {
		t.Fatal("second call must fail")
	}
	// Wait long enough for ≥1 token at 100/s.
	time.Sleep(25 * time.Millisecond) // 2.5 tokens worth → clamped to burst=1
	if !tb.allow() {
		t.Error("bucket should have refilled after 25ms")
	}
}

func TestTokenBucket_DefaultBurstEqualsRate(t *testing.T) {
	tb := newTokenBucket(5, 0) // burst unspecified
	// Should allow up to 5 without refill.
	for i := 0; i < 5; i++ {
		if !tb.allow() {
			t.Fatalf("iter %d should be allowed (burst=rate=5)", i)
		}
	}
	if tb.allow() {
		t.Error("6th call must fail")
	}
}
