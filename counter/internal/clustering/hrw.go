package clustering

import (
	"encoding/binary"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// AssignVShards maps every id in vshards to the node with the highest
// score(vshard, node). This is Rendezvous / Highest-Random-Weight hashing:
// adding or removing one node moves only ~1/N of the assignments, which
// minimises churn during scale-up / scale-down.
//
// Ties (which in practice don't happen with FNV but are handled for
// determinism) are broken by node id lexicographically so every caller —
// coordinator, counter node, BFF — computes the same mapping.
//
// Returns nil when nodes is empty; callers must guard against that
// (there's nowhere to assign to).
func AssignVShards(vshards []VShardID, nodes []string) map[VShardID]string {
	if len(nodes) == 0 || len(vshards) == 0 {
		return nil
	}
	// Make a sorted copy so the caller's slice order doesn't affect
	// tie-breaking.
	sortedNodes := make([]string, len(nodes))
	copy(sortedNodes, nodes)
	sort.Strings(sortedNodes)

	out := make(map[VShardID]string, len(vshards))
	for _, v := range vshards {
		best := sortedNodes[0]
		bestScore := score(v, best)
		for _, n := range sortedNodes[1:] {
			s := score(v, n)
			if s > bestScore || (s == bestScore && n < best) {
				best = n
				bestScore = s
			}
		}
		out[v] = best
	}
	return out
}

// score is the HRW weighting function. xxhash64 over (vshard_id ||
// node_id) — same hash family already used by pkg/shard for
// user→shard routing, so both layers have identical stability
// guarantees across Go versions.
func score(v VShardID, node string) uint64 {
	h := xxhash.New()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte(node))
	return h.Sum64()
}

// AllVShards returns [0, 1, …, count-1] for callers that want the
// canonical full set to pass to AssignVShards.
func AllVShards(count int) []VShardID {
	if count <= 0 {
		return nil
	}
	out := make([]VShardID, count)
	for i := 0; i < count; i++ {
		out[i] = VShardID(i)
	}
	return out
}
