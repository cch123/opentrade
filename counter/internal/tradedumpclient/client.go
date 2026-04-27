// Package tradedumpclient wraps trade-dump's TradeDumpSnapshot RPC
// surface (ADR-0064 §2) into the narrow view Counter's startup flow
// consumes:
//
//   - Dial once at process start, Close once at shutdown.
//   - Call TakeSnapshot per vshard startup.
//   - Classify the result as "use this snapshot" vs "fall back to
//     legacy path" via ErrFallback.
//
// Keeping the ADR-0064 §4 fallback decision table inside one
// package means worker.Run stays focused on sequencing Phase 1 /
// Phase 2 and doesn't leak Connect codes into the startup state
// machine.
package tradedumpclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"

	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
	"github.com/xargin/opentrade/api/gen/rpc/tradedump/tradedumprpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// ErrFallback signals that the on-demand path did not produce a
// usable snapshot and the caller should take the legacy startup
// path (load last periodic snapshot + catchUpJournal). Wrapped
// around the underlying transport error so callers can errors.Is
// it AND still unwrap to log the original reason.
//
// Every Connect code listed in ADR-0064 §4 as a "fallback trigger"
// maps to ErrFallback:
//
//   - CodeUnimplemented      — trade-dump running skeleton mode
//                              or Counter built against a newer
//                              proto
//   - CodeUnavailable        — transient Kafka / S3 / network
//   - CodeDeadlineExceeded   — WaitApply timed out or caller
//                              cancelled
//   - CodeCanceled           — caller cancelled
//   - CodeFailedPrecondition — trade-dump doesn't own the vshard
//                              or rejected stale epoch
//   - CodeResourceExhausted  — too many on-demand in flight
//
// Any other non-OK code (Internal, Unknown, InvalidArgument,
// DataLoss, …) surfaces as a naked error — Counter treats those
// as programming or infrastructure bugs that deserve a loud
// failure rather than silent fallback.
var ErrFallback = errors.New("ondemand: fallback required")

// Response is the local-friendly projection of
// tradedumprpc.TakeSnapshotResponse so consumers don't have to
// touch the generated proto types.
type Response struct {
	// SnapshotKey is the blob-store key trade-dump wrote the
	// on-demand snapshot to. Counter reads it through the same
	// BlobStore the periodic snapshot reader uses.
	SnapshotKey string

	// LEO is snapshot.JournalOffset of the Capture (ADR-0064
	// M1c-β P1 fix — this is the cursor the snapshot is actually
	// aligned to, not the queried Kafka LEO).
	LEO int64

	// CounterSeq is the max counter_seq_id observed by trade-dump's
	// shadow engine at Capture time. Counter seeds its UserSequencer
	// with this value (ADR-0064 §3 Phase 2 step ⑥).
	CounterSeq uint64
}

// Client is the thin wrapper over a single long-lived h2c HTTP/2
// pool and the generated Connect stub.
type Client struct {
	httpClient *http.Client
	owns       bool // true => Close() drops idle conns on httpClient
	stub       tradedumprpcconnect.TradeDumpSnapshotClient
	logger     *zap.Logger
}

// Dial wires a Connect client targeting the trade-dump TakeSnapshot
// endpoint over plaintext h2c. mTLS / auth arrive with the broader
// auth work; for now the internal cluster network is the trust
// boundary. logger is the caller's, stamped onto every RPC log.
//
// The returned Client owns its http.Client — callers invoke Close
// on shutdown.
func Dial(_ context.Context, endpoint string, logger *zap.Logger) (*Client, error) {
	if endpoint == "" {
		return nil, errors.New("tradedumpclient: empty endpoint")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	httpClient := connectx.NewH2CClient()
	stub := tradedumprpcconnect.NewTradeDumpSnapshotClient(
		httpClient,
		connectx.BaseURL(endpoint),
		connect.WithGRPC(),
	)
	return &Client{
		httpClient: httpClient,
		owns:       true,
		stub:       stub,
		logger:     logger,
	}, nil
}

// NewFromStub lets tests (and future pre-pooled connection flows)
// supply a hand-built stub without going through Dial. Production
// code should prefer Dial; NewFromStub skips connection ownership
// so Close is a no-op.
func NewFromStub(stub tradedumprpcconnect.TradeDumpSnapshotClient, logger *zap.Logger) *Client {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Client{stub: stub, logger: logger}
}

// Close drops idle connections on the owned *http.Client. Safe to
// call multiple times; safe on Client constructed via NewFromStub
// (no-op).
func (c *Client) Close() error {
	if c.owns && c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
	return nil
}

// TakeSnapshot issues one RPC to trade-dump. The caller supplies
// the request ctx (with whatever per-request deadline Counter is
// enforcing — typically 3s, leaving room for download + parse).
//
// On success returns (*Response, nil).
// On fallback-class errors returns (nil, wrapped-ErrFallback).
// On fatal errors returns (nil, error) — caller should log and
// abort startup rather than silently take the legacy path.
//
// Worker flow:
//
//	resp, err := client.TakeSnapshot(ctx, vshardID, nodeID, epoch)
//	switch {
//	case err == nil:
//	    // hot path
//	case errors.Is(err, tradedumpclient.ErrFallback):
//	    // legacy load + catchUpJournal
//	default:
//	    return fmt.Errorf("ondemand fatal: %w", err)
//	}
func (c *Client) TakeSnapshot(
	ctx context.Context,
	vshardID uint32,
	nodeID string,
	epoch uint64,
) (*Response, error) {
	start := time.Now()
	resp, err := c.stub.TakeSnapshot(ctx, connect.NewRequest(&tradedumprpc.TakeSnapshotRequest{
		VshardId:        vshardID,
		RequesterNodeId: nodeID,
		RequesterEpoch:  epoch,
	}))
	elapsed := time.Since(start)

	if err != nil {
		// Raw context errors can surface when the Connect client
		// short-circuits on ctx.Done before the transport turns
		// them into a Connect error. Treat them as fallback
		// explicitly so callers don't need to probe codes
		// separately.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			c.logger.Info("ondemand falling back (ctx)",
				zap.Uint32("vshard_id", vshardID),
				zap.Duration("elapsed", elapsed),
				zap.Error(err))
			return nil, fmt.Errorf("%w: %v", ErrFallback, err)
		}
		code := connect.CodeOf(err)
		if IsFallbackCode(code) {
			c.logger.Info("ondemand falling back",
				zap.Uint32("vshard_id", vshardID),
				zap.String("code", code.String()),
				zap.Duration("elapsed", elapsed),
				zap.Error(err))
			return nil, fmt.Errorf("%w: %v", ErrFallback, err)
		}
		c.logger.Error("ondemand fatal",
			zap.Uint32("vshard_id", vshardID),
			zap.String("code", code.String()),
			zap.Duration("elapsed", elapsed),
			zap.Error(err))
		return nil, err
	}

	c.logger.Info("ondemand succeeded",
		zap.Uint32("vshard_id", vshardID),
		zap.Int64("leo", resp.Msg.Leo),
		zap.Uint64("counter_seq", resp.Msg.CounterSeq),
		zap.String("snapshot_key", resp.Msg.SnapshotKey),
		zap.Duration("elapsed", elapsed))

	return &Response{
		SnapshotKey: resp.Msg.SnapshotKey,
		LEO:         resp.Msg.Leo,
		CounterSeq:  resp.Msg.CounterSeq,
	}, nil
}

// IsFallbackCode reports whether a Connect status code is one Counter
// should treat as "fall back to legacy path" per ADR-0064 §4.
// Exported so tests and other callers (future health probes) can
// share the classification without re-deriving it.
func IsFallbackCode(code connect.Code) bool {
	switch code {
	case connect.CodeUnimplemented,
		connect.CodeUnavailable,
		connect.CodeDeadlineExceeded,
		connect.CodeCanceled,
		connect.CodeFailedPrecondition,
		connect.CodeResourceExhausted:
		return true
	}
	return false
}
