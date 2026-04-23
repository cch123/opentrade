package tradedumpclient

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
)

// fakeStub implements tradedumprpc.TradeDumpSnapshotClient so unit
// tests can drive Client without a live trade-dump server.
type fakeStub struct {
	tradedumprpc.TradeDumpSnapshotClient // embed to fail closed on
	// unintended method calls

	calls    atomic.Int64
	lastReq  atomic.Pointer[tradedumprpc.TakeSnapshotRequest]
	response *tradedumprpc.TakeSnapshotResponse
	err      error
	delay    time.Duration
}

func (f *fakeStub) TakeSnapshot(
	ctx context.Context,
	req *tradedumprpc.TakeSnapshotRequest,
	_ ...grpc.CallOption,
) (*tradedumprpc.TakeSnapshotResponse, error) {
	f.calls.Add(1)
	reqCopy := proto.Clone(req).(*tradedumprpc.TakeSnapshotRequest)
	f.lastReq.Store(reqCopy)
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}

// -----------------------------------------------------------------------------
// IsFallbackCode — shared classifier
// -----------------------------------------------------------------------------

func TestIsFallbackCode(t *testing.T) {
	cases := []struct {
		code codes.Code
		want bool
	}{
		// Fallback triggers per ADR-0064 §4.
		{codes.Unimplemented, true},
		{codes.Unavailable, true},
		{codes.DeadlineExceeded, true},
		{codes.Canceled, true},
		{codes.FailedPrecondition, true},
		{codes.ResourceExhausted, true},
		// Fatal — really wrong.
		{codes.Internal, false},
		{codes.Unknown, false},
		{codes.InvalidArgument, false},
		{codes.DataLoss, false},
		{codes.OK, false}, // not an error, shouldn't be queried but documented
	}
	for _, tc := range cases {
		if got := IsFallbackCode(tc.code); got != tc.want {
			t.Errorf("IsFallbackCode(%s) = %v, want %v", tc.code, got, tc.want)
		}
	}
}

// -----------------------------------------------------------------------------
// TakeSnapshot — happy path and error mapping
// -----------------------------------------------------------------------------

func TestClient_TakeSnapshot_HappyPath(t *testing.T) {
	stub := &fakeStub{
		response: &tradedumprpc.TakeSnapshotResponse{
			SnapshotKey: "vshard-005-ondemand-1700000000000",
			Leo:         100,
			CounterSeq:  42,
		},
	}
	c := NewFromStub(stub, zap.NewNop())

	resp, err := c.TakeSnapshot(context.Background(), 5, "node-A", 7)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.SnapshotKey != "vshard-005-ondemand-1700000000000" {
		t.Errorf("SnapshotKey = %q", resp.SnapshotKey)
	}
	if resp.LEO != 100 {
		t.Errorf("LEO = %d, want 100", resp.LEO)
	}
	if resp.CounterSeq != 42 {
		t.Errorf("CounterSeq = %d, want 42", resp.CounterSeq)
	}
	// Request fields must be passed through verbatim.
	last := stub.lastReq.Load()
	if last == nil || last.VshardId != 5 || last.RequesterNodeId != "node-A" || last.RequesterEpoch != 7 {
		t.Fatalf("lastReq = %+v", last)
	}
}

// TestClient_TakeSnapshot_FallbackCodesMap verifies every
// ADR-0064 §4 fallback-trigger code round-trips through
// TakeSnapshot into an ErrFallback wrap. Callers rely on errors.Is
// to branch — if this mapping breaks, worker.Run would crash
// instead of falling back.
func TestClient_TakeSnapshot_FallbackCodesMap(t *testing.T) {
	fallbackCodes := []codes.Code{
		codes.Unimplemented,
		codes.Unavailable,
		codes.DeadlineExceeded,
		codes.Canceled,
		codes.FailedPrecondition,
		codes.ResourceExhausted,
	}
	for _, code := range fallbackCodes {
		t.Run(code.String(), func(t *testing.T) {
			stub := &fakeStub{err: status.Error(code, "simulated")}
			c := NewFromStub(stub, zap.NewNop())
			_, err := c.TakeSnapshot(context.Background(), 0, "", 0)
			if err == nil {
				t.Fatalf("want err, got nil")
			}
			if !errors.Is(err, ErrFallback) {
				t.Fatalf("errors.Is(err, ErrFallback) = false for code %s; err=%v", code, err)
			}
			// Underlying error message (the gRPC status) must be preserved
			// so operators can tell WHY fallback was taken.
			if !strings.Contains(err.Error(), "simulated") {
				t.Fatalf("err does not preserve underlying msg: %v", err)
			}
		})
	}
}

// TestClient_TakeSnapshot_FatalCodesSurface pins the other half:
// Internal / Unknown / InvalidArgument — non-fallback codes must
// NOT be wrapped in ErrFallback. Worker.Run treats those as startup
// fatal.
func TestClient_TakeSnapshot_FatalCodesSurface(t *testing.T) {
	fatalCodes := []codes.Code{
		codes.Internal,
		codes.Unknown,
		codes.InvalidArgument,
		codes.DataLoss,
	}
	for _, code := range fatalCodes {
		t.Run(code.String(), func(t *testing.T) {
			stub := &fakeStub{err: status.Error(code, "bad")}
			c := NewFromStub(stub, zap.NewNop())
			_, err := c.TakeSnapshot(context.Background(), 0, "", 0)
			if err == nil {
				t.Fatalf("want err, got nil")
			}
			if errors.Is(err, ErrFallback) {
				t.Fatalf("fatal code %s should NOT map to ErrFallback; got %v", code, err)
			}
			// Status code must survive round-trip so worker can log it.
			if got := status.Code(err); got != code {
				t.Fatalf("status code = %s, want %s", got, code)
			}
		})
	}
}

// TestClient_TakeSnapshot_CtxCancelMapsToFallback locks the ctx
// propagation path: Counter's worker.Run supplies a bounded ctx,
// and if the caller cancels before the RPC returns, the resulting
// Canceled should classify as fallback (ADR-0064 §4 — caller gave
// up, take legacy path).
func TestClient_TakeSnapshot_CtxCancelMapsToFallback(t *testing.T) {
	stub := &fakeStub{delay: 200 * time.Millisecond} // blocks past ctx cancel
	c := NewFromStub(stub, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := c.TakeSnapshot(ctx, 0, "node-A", 1)
	if err == nil {
		t.Fatal("want ctx err, got nil")
	}
	if !errors.Is(err, ErrFallback) {
		t.Fatalf("ctx cancel should map to ErrFallback, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Dial / Close — transport integration via a real loopback gRPC
// server with no registered service so we hit "Unimplemented".
// -----------------------------------------------------------------------------

func TestClient_Dial_AndClose_ErrFallbackOnUnimplemented(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer() // empty — TakeSnapshot → codes.Unimplemented
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.GracefulStop()
		<-serveErr
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, err := Dial(ctx, lis.Addr().String(), zap.NewNop())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Override default grpc.Dial's lazy resolver: force a connect
	// by setting a short call deadline.
	callCtx, callCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer callCancel()
	_, err = c.TakeSnapshot(callCtx, 0, "node-A", 1)
	if err == nil {
		t.Fatal("expected Unimplemented-wrapped ErrFallback over transport")
	}
	if !errors.Is(err, ErrFallback) {
		t.Fatalf("transport Unimplemented should map to ErrFallback, got %v", err)
	}
}

func TestDial_EmptyEndpointErrors(t *testing.T) {
	_, err := Dial(context.Background(), "", zap.NewNop())
	if err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestDial_InsecureCredentialsImport(t *testing.T) {
	// Defensive: verify we can construct insecure credentials in
	// this environment (import cycle sanity). Trivial but catches
	// build / dep regressions.
	_ = insecure.NewCredentials()
}
