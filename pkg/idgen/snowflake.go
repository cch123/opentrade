// Package idgen provides a Snowflake-style monotonic 63-bit id generator.
//
// Bit layout (63 bits usable, sign bit kept zero):
//
//	sign | timestamp (41 bits, ms since Epoch) | shard (10 bits) | seq (12 bits)
//
// A single Generator can produce up to 4096 ids per ms per shard (= 4M/s).
// Across 1024 shards this is plenty for our scale.
package idgen

import (
	"errors"
	"sync"
	"time"
)

// Epoch is the custom epoch in unix millis — 2026-01-01T00:00:00Z.
const Epoch int64 = 1767225600000

const (
	shardBits = 10
	seqBits   = 12

	maxShard = 1<<shardBits - 1
	maxSeq   = 1<<seqBits - 1

	shardShift = seqBits
	tsShift    = seqBits + shardBits
)

// ErrShardOutOfRange is returned when NewGenerator is given an invalid shard id.
var ErrShardOutOfRange = errors.New("idgen: shard id out of range (0..1023)")

// Generator is a Snowflake id generator for a specific shard.
type Generator struct {
	mu       sync.Mutex
	shard    uint64
	lastMS   int64
	seq      uint64
	nowFunc  func() int64 // override for tests
}

// NewGenerator constructs a Generator for the given shard id (0..1023).
func NewGenerator(shard int) (*Generator, error) {
	if shard < 0 || shard > maxShard {
		return nil, ErrShardOutOfRange
	}
	return &Generator{
		shard:   uint64(shard),
		nowFunc: func() int64 { return time.Now().UnixMilli() },
	}, nil
}

// Next returns the next unique id. Waits (busy-spin) up to 1 ms if the current
// ms bucket is exhausted.
func (g *Generator) Next() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.nowFunc() - Epoch
	if now == g.lastMS {
		g.seq = (g.seq + 1) & maxSeq
		if g.seq == 0 {
			// wait for next ms
			for now == g.lastMS {
				now = g.nowFunc() - Epoch
			}
		}
	} else {
		g.seq = 0
	}
	if now < g.lastMS {
		// clock moved backwards; stay on lastMS to preserve monotonicity
		now = g.lastMS
	}
	g.lastMS = now
	return uint64(now)<<tsShift | g.shard<<shardShift | g.seq
}
