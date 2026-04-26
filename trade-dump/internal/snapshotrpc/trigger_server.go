package snapshotrpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	triggersnap "github.com/xargin/opentrade/pkg/snapshot/trigger"
	"github.com/xargin/opentrade/trade-dump/internal/snapshot/triggershadow"
)

// TriggerBackend bundles the dependencies the TakeTriggerSnapshot
// handler needs (ADR-0067 M4). All non-nil → handler runs the full
// LEO-aligned capture flow; otherwise the handler returns
// Unimplemented and the trigger falls back to the cold-path BlobStore
// read (M5).
type TriggerBackend struct {
	// Shadow is the trigger shadow engine that applies trigger-event
	// records and produces the snapshot.
	Shadow *triggershadow.Engine

	// Admin queries trigger-event LEOs (one ListEndOffset call per
	// partition before the WaitAppliedTo wait).
	Admin KafkaAdmin

	// BlobStore receives the captured snapshot. Same store the
	// pipeline writes its periodic snapshot to.
	BlobStore snapshotpkg.BlobStore

	// Topic is the trigger-event topic name (e.g. "trigger-event").
	Topic string

	// PartitionCount is the number of partitions on Topic. The
	// handler queries LEO for [0, PartitionCount) and waits for the
	// shadow's per-partition cursor to catch up to each.
	PartitionCount int

	// SnapshotFormat is the encoding used when saving on-demand
	// snapshots.
	SnapshotFormat snapshotpkg.Format

	// KeyPrefix lets ops namespace on-demand keys; default "trigger-
	// ondemand-{unix_ms}" lives at the BlobStore root. Production
	// wiring leaves this empty.
	KeyPrefix string
}

// TakeTriggerSnapshot is the ADR-0067 M4 hot-path RPC: produce a
// trigger snapshot aligned to the current LEO of every trigger-event
// partition, return the BlobStore key. Counterpart of TakeSnapshot
// but for trigger; simpler because trigger doesn't shard, has no
// EOS/fence dance, and uses no requester_epoch.
func (s *Server) TakeTriggerSnapshot(
	ctx context.Context,
	req *tradedumprpc.TakeTriggerSnapshotRequest,
) (*tradedumprpc.TakeTriggerSnapshotResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	tb := s.cfg.Trigger
	if tb == nil || tb.Shadow == nil || tb.Admin == nil || tb.BlobStore == nil ||
		tb.Topic == "" || tb.PartitionCount <= 0 {
		s.cfg.Logger.Info("TakeTriggerSnapshot hit without trigger backend",
			zap.String("requester_node_id", req.RequesterNodeId))
		return nil, status.Error(codes.Unimplemented,
			"TakeTriggerSnapshot backend not wired (trigger pipeline disabled)")
	}

	// Singleflight key is constant — single shadow engine, so two
	// concurrent callers (rare, since trigger has at most one
	// primary at a time) share the work.
	ch := s.sf.DoChan("trigger", func() (any, error) {
		workCtx, cancel := context.WithTimeout(context.Background(), s.workTimeout)
		defer cancel()
		return s.takeTriggerSnapshotOnce(workCtx, tb)
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*tradedumprpc.TakeTriggerSnapshotResponse), nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
		}
		return nil, status.Error(codes.Canceled, ctx.Err().Error())
	}
}

func (s *Server) takeTriggerSnapshotOnce(
	ctx context.Context,
	tb *TriggerBackend,
) (*tradedumprpc.TakeTriggerSnapshotResponse, error) {
	// Step 1: query LEO per partition. ListEndOffset is uncommitted
	// — equal to the next offset to be produced; semantically same
	// as what the shadow stores in nextTriggerEventOffsets.
	targets := make(map[int32]int64, tb.PartitionCount)
	for part := int32(0); part < int32(tb.PartitionCount); part++ {
		leo, err := tb.Admin.ListEndOffset(ctx, tb.Topic, part)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable,
				"list end offset partition %d: %v", part, err)
		}
		targets[part] = leo
	}

	// Step 2: wait for shadow apply cursor to reach LEO on every
	// partition. WaitApplyTimeout bounds this — if the shadow
	// pipeline is way behind, the handler returns DeadlineExceeded
	// and trigger falls back to the cold path.
	waitCtx, cancel := context.WithTimeout(ctx, s.cfg.WaitApplyTimeout)
	defer cancel()
	if err := tb.Shadow.WaitAppliedTo(waitCtx, targets, 5*time.Millisecond); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Errorf(codes.DeadlineExceeded,
				"shadow apply did not reach LEO within %s", s.cfg.WaitApplyTimeout)
		}
		return nil, status.Errorf(codes.Internal, "wait applied: %v", err)
	}

	// Step 3: Capture + Save.
	now := s.nowFn()
	snap := tb.Shadow.Capture(now.UnixMilli(), false)
	key := fmt.Sprintf("%strigger-ondemand-%d", tb.KeyPrefix, now.UnixMilli())
	if err := triggersnap.Save(ctx, tb.BlobStore, key, snap, tb.SnapshotFormat); err != nil {
		return nil, status.Errorf(codes.Internal, "save snapshot: %v", err)
	}

	// Build response. Copy the maps so caller mutations don't ripple
	// into the shadow's owned snap.
	resp := &tradedumprpc.TakeTriggerSnapshotResponse{
		SnapshotKey:         key,
		TriggerEventOffsets: copyOffsetMap(snap.TriggerEventOffsets),
		MarketOffsets:       copyOffsetMap(snap.Offsets),
	}
	return resp, nil
}

func copyOffsetMap(in map[int32]int64) map[int32]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int32]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
