package snapshotrpc

import (
	"context"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
)

// TestServer_TakeSnapshot_ReturnsUnimplemented locks the M1b
// skeleton contract: a registered TradeDumpSnapshot handler that
// accepts the RPC transport-wise but reports Unimplemented so
// Counter's on-demand path falls through to the fallback
// (ADR-0064 §4). When M1c replaces the handler body, this test
// SHOULD fail — the replacement is expected to return a concrete
// response. Delete or adapt at that point.
func TestServer_TakeSnapshot_ReturnsUnimplemented(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	tradedumprpc.RegisterTradeDumpSnapshotServer(grpcSrv, New(Config{Logger: zap.NewNop()}))

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcSrv.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcSrv.GracefulStop()
		<-serveErr
	})

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDial()
	conn, err := grpc.DialContext(dialCtx, lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := tradedumprpc.NewTradeDumpSnapshotClient(conn)
	callCtx, cancelCall := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCall()

	resp, err := client.TakeSnapshot(callCtx, &tradedumprpc.TakeSnapshotRequest{
		VshardId:        5,
		RequesterNodeId: "counter-node-test",
		RequesterEpoch:  7,
	})
	if err == nil {
		t.Fatalf("expected Unimplemented error, got response: %+v", resp)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Unimplemented {
		t.Fatalf("expected code=Unimplemented, got %v (msg=%q)", st.Code(), st.Message())
	}
}

// TestServer_TakeSnapshot_NilRequestInvalidArgument pins the input
// validation so a client sending a malformed proto doesn't trigger
// a nil-deref in the skeleton. When M1c lands a full handler, the
// same guard should survive — an Unimplemented handler upgrading
// to a real one must not lose input validation.
func TestServer_TakeSnapshot_NilRequestInvalidArgument(t *testing.T) {
	srv := New(Config{Logger: zap.NewNop()})
	_, err := srv.TakeSnapshot(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error on nil request")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected code=InvalidArgument, got %v", st.Code())
	}
}
