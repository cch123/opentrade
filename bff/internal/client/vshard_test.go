package client

import (
	"context"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/bff/internal/clusterview"
)

// newTestWatcher builds a Watcher backed by an empty *clientv3.Client
// so Lookup can run without etcd. Resync() will error silently when
// the internal list RPC fails — that's fine for the no-owner path
// because dispatch falls back to FailedPrecondition after its single
// retry.
func newTestWatcher(t *testing.T) *clusterview.Watcher {
	t.Helper()
	w, err := clusterview.New(clusterview.Config{
		Client:      &clientv3.Client{},
		VShardCount: 4,
	})
	if err != nil {
		t.Fatalf("clusterview.New: %v", err)
	}
	return w
}

// TestVShardCounter_RequiresUserID: an empty user_id is an
// InvalidArgument up-front — no point walking the assignment table.
func TestVShardCounter_RequiresUserID(t *testing.T) {
	v, err := NewVShardCounter(newTestWatcher(t))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	_, err = v.PlaceOrder(context.Background(), &counterrpc.PlaceOrderRequest{UserId: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty user_id = %v, want InvalidArgument", err)
	}
}

// TestVShardCounter_NoActiveOwnerIsFailedPrecondition: with an empty
// watcher (no assignments loaded), dispatch should surface
// FailedPrecondition so the REST layer can translate it to a 412 and
// the client retries after refreshing its own view.
func TestVShardCounter_NoActiveOwnerIsFailedPrecondition(t *testing.T) {
	v, err := NewVShardCounter(newTestWatcher(t))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	_, err = v.PlaceOrder(context.Background(), &counterrpc.PlaceOrderRequest{UserId: "alice"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("no owner = %v, want FailedPrecondition", err)
	}
}

// TestVShardCounter_AdminCancelRequiresUserID: ADR-0058 removed the
// symbol-only fan-out path; handler must reject it up-front.
func TestVShardCounter_AdminCancelRequiresUserID(t *testing.T) {
	v, err := NewVShardCounter(newTestWatcher(t))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	_, err = v.AdminCancelOrders(context.Background(), &counterrpc.AdminCancelOrdersRequest{
		Symbol: "BTC-USDT",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("symbol-only = %v, want InvalidArgument", err)
	}
}
