package ratelimit

import (
	"testing"
	"time"
)

func TestAllowWithinLimit(t *testing.T) {
	sw := New(3, time.Second)
	now := time.Unix(0, 0)
	sw.SetNowFunc(func() time.Time { return now })

	for i := 0; i < 3; i++ {
		if !sw.Allow("u1") {
			t.Fatalf("request %d denied unexpectedly", i)
		}
	}
	if sw.Allow("u1") {
		t.Fatal("4th request should be denied")
	}
}

func TestAllowWindowSlide(t *testing.T) {
	sw := New(2, 100*time.Millisecond)
	now := time.Unix(0, 0)
	sw.SetNowFunc(func() time.Time { return now })

	sw.Allow("u1")
	sw.Allow("u1")
	if sw.Allow("u1") {
		t.Fatal("3rd should be denied")
	}
	// advance past window
	now = now.Add(200 * time.Millisecond)
	if !sw.Allow("u1") {
		t.Fatal("after window, should allow")
	}
}

func TestAllowDifferentKeysIndependent(t *testing.T) {
	sw := New(1, time.Second)
	if !sw.Allow("u1") {
		t.Fatal("u1 first allow")
	}
	if !sw.Allow("u2") {
		t.Fatal("u2 should not be blocked by u1")
	}
	if sw.Allow("u1") {
		t.Fatal("u1 over quota should deny")
	}
}

func TestEmptyKeyBypass(t *testing.T) {
	sw := New(1, time.Second)
	for i := 0; i < 10; i++ {
		if !sw.Allow("") {
			t.Fatal("empty key should be unrestricted")
		}
	}
}

func TestSweepEvictsStale(t *testing.T) {
	sw := New(3, 50*time.Millisecond)
	now := time.Unix(0, 0)
	sw.SetNowFunc(func() time.Time { return now })

	sw.Allow("u1")
	sw.Allow("u2")
	now = now.Add(time.Second)
	sw.Sweep()

	// After sweep, internal buckets for idle keys should be gone.
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if len(sw.buckets) != 0 {
		t.Fatalf("sweep left %d buckets", len(sw.buckets))
	}
}
