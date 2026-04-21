package clustering

import (
	"fmt"
	"testing"
)

func TestAssignVShards_EmptyInputs(t *testing.T) {
	if got := AssignVShards(nil, []string{"a"}); got != nil {
		t.Errorf("nil vshards should yield nil, got %v", got)
	}
	if got := AssignVShards(AllVShards(4), nil); got != nil {
		t.Errorf("no nodes should yield nil, got %v", got)
	}
}

// TestAssignVShards_Deterministic: same inputs → same output, regardless
// of input node order. This is the property BFF / coordinator rely on.
func TestAssignVShards_Deterministic(t *testing.T) {
	vshards := AllVShards(256)
	orderA := []string{"node-a", "node-b", "node-c", "node-d"}
	orderB := []string{"node-d", "node-a", "node-c", "node-b"}
	a := AssignVShards(vshards, orderA)
	b := AssignVShards(vshards, orderB)
	if len(a) != len(b) {
		t.Fatalf("size mismatch: %d vs %d", len(a), len(b))
	}
	for v, owner := range a {
		if b[v] != owner {
			t.Errorf("vshard %d: order A → %s, order B → %s", v, owner, b[v])
		}
	}
}

// TestAssignVShards_MinimalChurnOnNodeJoin: adding one node to a set of
// N should move roughly count/(N+1) of the assignments — and crucially
// no assignment "bounces" between previously-existing nodes.
func TestAssignVShards_MinimalChurnOnNodeJoin(t *testing.T) {
	vshards := AllVShards(256)
	before := AssignVShards(vshards, []string{"n1", "n2", "n3"})
	after := AssignVShards(vshards, []string{"n1", "n2", "n3", "n4"})

	moved := 0
	for v, ownerBefore := range before {
		if after[v] != ownerBefore {
			moved++
			// The only legitimate move is onto the new node. A
			// reassignment between n1/n2/n3 would violate HRW.
			if after[v] != "n4" {
				t.Errorf("vshard %d moved %s → %s without n4", v, ownerBefore, after[v])
			}
		}
	}
	// Expected ~256/4 = 64; allow ±25% drift for FNV variance.
	expected := 256 / 4
	low, high := expected*3/4, expected*5/4
	if moved < low || moved > high {
		t.Errorf("moved %d assignments on node join, want ~%d (±25%%)", moved, expected)
	}
}

// TestAssignVShards_MinimalChurnOnNodeLeave is the dual test.
func TestAssignVShards_MinimalChurnOnNodeLeave(t *testing.T) {
	vshards := AllVShards(256)
	before := AssignVShards(vshards, []string{"n1", "n2", "n3", "n4"})
	after := AssignVShards(vshards, []string{"n1", "n2", "n3"})

	moved := 0
	for v, ownerBefore := range before {
		if after[v] != ownerBefore {
			moved++
			// Only vshards whose previous owner was n4 should
			// move.
			if ownerBefore != "n4" {
				t.Errorf("vshard %d moved %s → %s but previous owner wasn't leaving", v, ownerBefore, after[v])
			}
		}
	}
	// Roughly 256/4 = 64 should move.
	if moved < 48 || moved > 80 {
		t.Errorf("moved %d on node leave, want ~64 (±25%%)", moved)
	}
}

// TestAssignVShards_LoadBalance: with 256 vshards and a handful of nodes,
// the per-node load shouldn't diverge wildly from the average. FNV-1a
// isn't cryptographic so we give generous bounds — this is a
// smoke test for "the algorithm isn't obviously broken", not a
// statistical claim.
func TestAssignVShards_LoadBalance(t *testing.T) {
	cases := []int{2, 4, 8, 16}
	for _, n := range cases {
		nodes := make([]string, n)
		for i := 0; i < n; i++ {
			nodes[i] = fmt.Sprintf("node-%d", i)
		}
		got := AssignVShards(AllVShards(256), nodes)
		counts := make(map[string]int, n)
		for _, owner := range got {
			counts[owner]++
		}
		avg := float64(256) / float64(n)
		// Bound: no node should be >2x average (very lax).
		for node, c := range counts {
			if float64(c) > 2*avg || float64(c) < avg/2 {
				t.Errorf("n=%d %s owns %d vshards, avg %.1f (outside 2x bounds)",
					n, node, c, avg)
			}
		}
		// Sanity: every vshard assigned.
		total := 0
		for _, c := range counts {
			total += c
		}
		if total != 256 {
			t.Errorf("n=%d: total %d != 256", n, total)
		}
	}
}
