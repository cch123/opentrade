// Package snapshotrpc implements trade-dump's on-demand snapshot
// gRPC surface (ADR-0064 §2).
//
// This package starts as the M1b skeleton: the TradeDumpSnapshot
// service is registered on the gRPC server, but every method returns
// codes.Unimplemented with a pointer to the milestone that will wire
// in the actual logic. This shape lets ADR-0064 land in
// reviewable-sized PRs — proto + transport first, then correctness
// (M1c: ShadowStore + LEO + WaitAppliedTo + Capture + upload).
//
// Callers on the Counter side (M2) MUST treat UNIMPLEMENTED as a
// fallback trigger (fall through to the legacy "load last periodic
// snapshot + catchUpJournal" path) so staging environments can run
// trade-dump at any milestone without breaking Counter startup.
package snapshotrpc

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
)

// ShadowStore is the minimal contract this server needs from
// trade-dump's snapshot pipeline. Left empty in M1b — M1c will add
// WaitAppliedTo / CaptureForVshard / epoch tracking. Kept as an
// interface so tests can stub and so the package stays loosely
// coupled to the pipeline implementation.
type ShadowStore interface{}

// Config bundles the server's immutable wiring. In M1b the server
// only needs a logger; the full set (ShadowStore, BlobStore,
// Kafka admin client, semaphore, singleflight, epoch tracker) arrives
// in M1c.
type Config struct {
	Logger *zap.Logger
}

// Server implements tradedumprpc.TradeDumpSnapshotServer. All
// handlers return Unimplemented until M1c lands.
type Server struct {
	tradedumprpc.UnimplementedTradeDumpSnapshotServer

	logger *zap.Logger
}

// New constructs a skeleton Server.
func New(cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Server{logger: logger}
}

// TakeSnapshot is the M1b skeleton implementation. It logs the
// request (for integration-test visibility) and returns Unimplemented
// so Counter's on-demand startup path falls through to the legacy
// fallback (ADR-0064 §4).
//
// Replaced in M1c: LEO query → WaitAppliedTo → CaptureForVshard →
// serialize → blob-store upload → return (key, leo, counter_seq).
func (s *Server) TakeSnapshot(
	ctx context.Context,
	req *tradedumprpc.TakeSnapshotRequest,
) (*tradedumprpc.TakeSnapshotResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	s.logger.Info("TakeSnapshot skeleton hit (M1b: Unimplemented)",
		zap.Uint32("vshard_id", req.VshardId),
		zap.String("requester_node_id", req.RequesterNodeId),
		zap.Uint64("requester_epoch", req.RequesterEpoch))
	return nil, status.Error(codes.Unimplemented,
		"TakeSnapshot not implemented yet (ADR-0064 M1b skeleton; M1c will wire it up)")
}
