package dedup

import (
	"testing"
	"time"
)

func TestGetSet(t *testing.T) {
	tbl := New(time.Hour)
	if _, ok := tbl.Get("k"); ok {
		t.Fatal("miss expected")
	}
	tbl.Set("k", 42)
	v, ok := tbl.Get("k")
	if !ok || v.(int) != 42 {
		t.Fatalf("hit = %v / %v", v, ok)
	}
}

func TestExpiry(t *testing.T) {
	tbl := New(50 * time.Millisecond)
	now := time.Unix(0, 0)
	tbl.SetNowFunc(func() time.Time { return now })

	tbl.Set("k", "v")
	// Advance past expiry.
	now = now.Add(time.Second)
	if _, ok := tbl.Get("k"); ok {
		t.Fatal("expired entry still visible")
	}
	if tbl.Len() != 0 {
		t.Fatalf("expected Get to evict; len = %d", tbl.Len())
	}
}

func TestSweep(t *testing.T) {
	tbl := New(10 * time.Millisecond)
	now := time.Unix(0, 0)
	tbl.SetNowFunc(func() time.Time { return now })

	tbl.Set("a", 1)
	tbl.Set("b", 2)
	now = now.Add(time.Minute)
	tbl.Sweep()
	if tbl.Len() != 0 {
		t.Fatalf("sweep left %d entries", tbl.Len())
	}
}

func TestSnapshotRestore(t *testing.T) {
	tbl := New(time.Hour)
	tbl.Set("a", "x")
	tbl.Set("b", "y")
	entries := tbl.Snapshot()
	if len(entries) != 2 {
		t.Fatalf("snapshot len = %d", len(entries))
	}

	tbl2 := New(time.Hour)
	tbl2.Restore(entries)
	if v, ok := tbl2.Get("a"); !ok || v != "x" {
		t.Fatalf("restore: a = %v / %v", v, ok)
	}
	if v, ok := tbl2.Get("b"); !ok || v != "y" {
		t.Fatalf("restore: b = %v / %v", v, ok)
	}
}

func TestEmptyKey(t *testing.T) {
	tbl := New(time.Hour)
	tbl.Set("", 1)
	if _, ok := tbl.Get(""); ok {
		t.Fatal("empty key should not be stored")
	}
}
