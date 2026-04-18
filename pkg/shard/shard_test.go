package shard

import (
	"testing"
)

func TestIndex_InRange(t *testing.T) {
	const total = 10
	for _, u := range []string{"", "alice", "bob", "00000000-0000-0000-0000-000000000000"} {
		got := Index(u, total)
		if got < 0 || got >= total {
			t.Errorf("Index(%q) = %d out of [0,%d)", u, got, total)
		}
	}
}

func TestIndex_Stable(t *testing.T) {
	// These are frozen expected values — regression trip-wire. If xxhash
	// changes semantics or we switch hash algorithms we need to plan a
	// coordinated re-shard; this test makes that intent explicit.
	cases := []struct {
		userID string
		total  int
		want   int
	}{
		// Frozen from xxhash64: regression trip-wire. If this fails, the
		// hash algorithm or its library changed and we need a coordinated
		// re-shard plan before shipping.
		{"alice", 10, 9},
		{"bob", 10, 7},
		{"", 10, 1},
		{"carol", 10, 8},
	}
	for _, c := range cases {
		if got := Index(c.userID, c.total); got != c.want {
			t.Errorf("Index(%q,%d)=%d want %d", c.userID, c.total, got, c.want)
		}
	}
}

func TestIndex_Distribution(t *testing.T) {
	const total = 10
	const n = 10_000
	counts := make([]int, total)
	for i := 0; i < n; i++ {
		u := "u" + itoa(i)
		counts[Index(u, total)]++
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
	userID := "alice"
	want := Index(userID, total)
	if !OwnsUser(want, total, userID) {
		t.Errorf("expected shard %d to own %q", want, userID)
	}
	for s := 0; s < total; s++ {
		if s == want {
			continue
		}
		if OwnsUser(s, total, userID) {
			t.Errorf("shard %d must not claim %q", s, userID)
		}
	}
}

func TestOwnsUser_OutOfRange(t *testing.T) {
	if OwnsUser(-1, 10, "x") {
		t.Error("negative shard must not own")
	}
	if OwnsUser(10, 10, "x") {
		t.Error("shard == total must not own")
	}
	if OwnsUser(0, 0, "x") {
		t.Error("zero total must not own")
	}
}

func TestIndex_PanicsOnBadTotal(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on totalShards=0")
		}
	}()
	_ = Index("x", 0)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
