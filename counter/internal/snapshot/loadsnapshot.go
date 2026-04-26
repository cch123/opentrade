// Package snapshot is counter's adapter over the shared shard-snapshot
// wire format. Per ADR-0066 trade-dump is the projection platform that
// produces snapshots and counter consumes them on startup. The wire
// format + Save / Load / Restore helpers live in pkg/snapshot/counter
// (shared between both modules); this package keeps a single Load
// entry point so worker code never reaches across the snapshot
// boundary directly.
package snapshot

import (
	"context"

	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/counterstate"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	countersnap "github.com/xargin/opentrade/pkg/snapshot/counter"
)

// Restored is everything Load extracts from a snapshot, ready for the
// worker to install. Caller still owns dedup.Table (not in snapshot
// scope) and any other side state.
type Restored struct {
	State         *counterstate.ShardState
	Seq           *sequencer.UserSequencer
	Offsets       map[int32]int64
	JournalOffset int64

	// Surface fields callers log:
	Version      int
	CounterSeq   uint64
	AccountCount int
	OrderCount   int
}

// Load fetches the snapshot at key from store and restores it into a
// fresh ShardState/Sequencer for vshardID. If key already has a .pb /
// .json extension only that format is attempted; otherwise probes both
// (ADR-0049). Returns os.ErrNotExist on cold start.
func Load(ctx context.Context, store snapshotpkg.BlobStore, key string, vshardID int) (*Restored, error) {
	snap, err := countersnap.LoadPath(ctx, store, key)
	if err != nil {
		return nil, err
	}
	state := counterstate.NewShardState(vshardID)
	if err := countersnap.RestoreState(vshardID, state, snap); err != nil {
		return nil, err
	}
	seq := sequencer.New()
	seq.SetCounterSeq(snap.CounterSeq)
	return &Restored{
		State:         state,
		Seq:           seq,
		Offsets:       countersnap.OffsetsSliceToMap(snap.Offsets),
		JournalOffset: snap.JournalOffset,
		Version:       snap.Version,
		CounterSeq:    snap.CounterSeq,
		AccountCount:  len(snap.Accounts),
		OrderCount:    len(snap.Orders),
	}, nil
}
