package tradedumpclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/proto"

	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
	"github.com/xargin/opentrade/api/gen/rpc/tradedump/tradedumprpcconnect"
)

// fakeStub implements tradedumprpcconnect.TradeDumpSnapshotClient so
// unit tests can drive Client without a live trade-dump server.
type fakeStub struct {
	tradedumprpcconnect.TradeDumpSnapshotClient // embed to fail closed on
	// unintended method calls

	calls    atomic.Int64
	lastReq  atomic.Pointer[tradedumprpc.TakeSnapshotRequest]
	response *tradedumprpc.TakeSnapshotResponse
	err      error
	delay    time.Duration
}

func (f *fakeStub) TakeSnapshot(
	ctx context.Context,
	req *connect.Request[tradedumprpc.TakeSnapshotRequest],
) (*connect.Response[tradedumprpc.TakeSnapshotResponse], error) {
	f.calls.Add(1)
	reqCopy := proto.Clone(req.Msg).(*tradedumprpc.TakeSnapshotRequest)
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
	return connect.NewResponse(f.response), nil
}

// -----------------------------------------------------------------------------
// IsFallbackCode — shared classifier
// -----------------------------------------------------------------------------

func TestIsFallbackCode(t *testing.T) {
	cases := []struct {
		code connect.Code
		want bool
	}{
		// Fallback triggers per ADR-0064 §4.
		{connect.CodeUnimplemented, true},
		{connect.CodeUnavailable, true},
		{connect.CodeDeadlineExceeded, true},
		{connect.CodeCanceled, true},
		{connect.CodeFailedPrecondition, true},
		{connect.CodeResourceExhausted, true},
		// Fatal — really wrong.
		{connect.CodeInternal, false},
		{connect.CodeUnknown, false},
		{connect.CodeInvalidArgument, false},
		{connect.CodeDataLoss, false},
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
	fallbackCodes := []connect.Code{
		connect.CodeUnimplemented,
		connect.CodeUnavailable,
		connect.CodeDeadlineExceeded,
		connect.CodeCanceled,
		connect.CodeFailedPrecondition,
		connect.CodeResourceExhausted,
	}
	for _, code := range fallbackCodes {
		t.Run(code.String(), func(t *testing.T) {
			stub := &fakeStub{err: connect.NewError(code, errors.New("simulated"))}
			c := NewFromStub(stub, zap.NewNop())
			_, err := c.TakeSnapshot(context.Background(), 0, "", 0)
			if err == nil {
				t.Fatalf("want err, got nil")
			}
			if !errors.Is(err, ErrFallback) {
				t.Fatalf("errors.Is(err, ErrFallback) = false for code %s; err=%v", code, err)
			}
			// Underlying error message must be preserved so operators
			// can tell WHY fallback was taken.
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
	fatalCodes := []connect.Code{
		connect.CodeInternal,
		connect.CodeUnknown,
		connect.CodeInvalidArgument,
		connect.CodeDataLoss,
	}
	for _, code := range fatalCodes {
		t.Run(code.String(), func(t *testing.T) {
			stub := &fakeStub{err: connect.NewError(code, errors.New("bad"))}
			c := NewFromStub(stub, zap.NewNop())
			_, err := c.TakeSnapshot(context.Background(), 0, "", 0)
			if err == nil {
				t.Fatalf("want err, got nil")
			}
			if errors.Is(err, ErrFallback) {
				t.Fatalf("fatal code %s should NOT map to ErrFallback; got %v", code, err)
			}
			// Connect code must survive round-trip so worker can log it.
			if got := connect.CodeOf(err); got != code {
				t.Fatalf("connect code = %s, want %s", got, code)
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
// Dial / Close — transport integration via a real loopback h2c
// server with no registered service so we hit "Unimplemented".
// -----------------------------------------------------------------------------

func TestClient_Dial_AndClose_ErrFallbackOnUnimplemented(t *testing.T) {
	// Empty mux — any /opentrade.rpc.tradedump.TradeDumpSnapshot/* path
	// returns 404, which Connect maps to CodeUnimplemented.
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	srv.EnableHTTP2 = true
	srv.Start()
	t.Cleanup(srv.Close)

	endpoint := strings.TrimPrefix(srv.URL, "http://")

	c, err := Dial(context.Background(), endpoint, zap.NewNop())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

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
