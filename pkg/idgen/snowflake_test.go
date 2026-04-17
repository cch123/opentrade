package idgen

import "testing"

func TestGeneratorMonotonic(t *testing.T) {
	g, err := NewGenerator(0)
	if err != nil {
		t.Fatalf("NewGenerator: %v", err)
	}
	const n = 100_000
	prev := g.Next()
	for i := 1; i < n; i++ {
		id := g.Next()
		if id <= prev {
			t.Fatalf("id not strictly increasing at i=%d: prev=%d cur=%d", i, prev, id)
		}
		prev = id
	}
}

func TestGeneratorShardBounds(t *testing.T) {
	if _, err := NewGenerator(-1); err == nil {
		t.Fatal("expected error for shard=-1")
	}
	if _, err := NewGenerator(1024); err == nil {
		t.Fatal("expected error for shard=1024")
	}
	if _, err := NewGenerator(1023); err != nil {
		t.Fatalf("shard=1023 should be valid: %v", err)
	}
}
