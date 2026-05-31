// Package shard provides stable user_id → shard-id routing used by BFF to
// select a Counter shard and by Counter to guard "this is mine" checks.
//
// See ADR-0010 (Counter sharded by user_id) and ADR-0027 (MVP-8 rollout).
//
// Stability contract: the output of Index and UserShard MUST NOT change
// across Go versions or hardware. We rely on xxhash64 rather than the
// builtin string hash (which is randomized per-process).
package shard

import (
	"encoding/binary"
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// Index maps userID to a shard id in [0, totalShards).
// totalShards must be > 0.
func Index(userID uint64, totalShards int) int {
	if totalShards <= 0 {
		panic(fmt.Sprintf("shard: totalShards must be > 0, got %d", totalShards))
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], userID)
	h := xxhash.Sum64(buf[:])
	return int(h % uint64(totalShards))
}

// OwnsUser reports whether shardID is the owner of userID under
// totalShards shards. Returns false on a nonsense totalShards.
func OwnsUser(shardID, totalShards int, userID uint64) bool {
	if totalShards <= 0 || shardID < 0 || shardID >= totalShards {
		return false
	}
	return Index(userID, totalShards) == shardID
}
