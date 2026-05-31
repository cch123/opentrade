package shard

import (
	"testing"
)

func TestIndex_InRange(t *testing.T) {
	const total = 10
	for _, u := range []uint64{0, 1, 1001, 1 << 63} {
		got := Index(u, total)
		if got < 0 || got >= total {
			t.Errorf("Index(%d) = %d out of [0,%d)", u, got, total)
		}
	}
}

func TestIndex_Stable(t *testing.T) {
	// These are frozen expected values — regression trip-wire. If xxhash
	// changes semantics or we switch hash algorithms we need to plan a
	// coordinated re-shard; this test makes that intent explicit.
	cases := []struct {
		userID uint64
		total  int
		want   int
	}{
		// Frozen from xxhash64 over the big-endian uint64 bytes: regression
		// trip-wire. If this fails, the hash algorithm or encoding changed
		// and we need a coordinated re-shard plan before shipping.
		{0, 10, 9},
		{1, 10, 4},
		{1001, 10, 8},
		{1 << 63, 10, 2},
	}
	for _, c := range cases {
		if got := Index(c.userID, c.total); got != c.want {
			t.Errorf("Index(%d,%d)=%d want %d", c.userID, c.total, got, c.want)
		}
	}
}

func TestIndex_Distribution(t *testing.T) {
	const total = 10
	const n = 10_000
	counts := make([]int, total)
	for i := 0; i < n; i++ {
		counts[Index(uint64(i), total)]++
	}
	// Each bucket should land within ±25% of n/total. The 25% bound is
	// generous — xxhash is far better in practice, but we'd rather not
	// have a flaky test.
	expected := n / total
	for i, c := range counts {
		delta := c - expected
		if delta < 0 {
			delta = -delta
		}
		if delta > expected/4 {
			t.Errorf("bucket %d count=%d (expected ~%d, ±%d)", i, c, expected, expected/4)
		}
	}
}

func TestOwnsUser(t *testing.T) {
	const total = 10
	userID := uint64(1001)
	want := Index(userID, total)
	if !OwnsUser(want, total, userID) {
		t.Errorf("expected shard %d to own %d", want, userID)
	}
	for s := 0; s < total; s++ {
		if s == want {
			continue
		}
		if OwnsUser(s, total, userID) {
			t.Errorf("shard %d must not claim %d", s, userID)
		}
	}
}

func TestOwnsUser_OutOfRange(t *testing.T) {
	if OwnsUser(-1, 10, 1) {
		t.Error("negative shard must not own")
	}
	if OwnsUser(10, 10, 1) {
		t.Error("shard == total must not own")
	}
	if OwnsUser(0, 0, 1) {
		t.Error("zero total must not own")
	}
}

func TestIndex_PanicsOnBadTotal(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on totalShards=0")
		}
	}()
	_ = Index(1, 0)
}
