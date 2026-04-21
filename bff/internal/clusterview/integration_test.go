//go:build integration

// Integration test for the BFF cluster watcher against a live etcd.
// Gated behind the `integration` build tag.
//
// Usage:
//
//   docker compose up -d etcd
//   ETCD_ENDPOINTS=localhost:2379 go test -tags=integration ./internal/clusterview/...
//
// Each test scopes itself under a unique root prefix so parallel /
// repeated runs stay isolated.

package clusterview

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/shard"
)

func requireEtcd(t *testing.T) (*clientv3.Client, string) {
	t.Helper()
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("ETCD_ENDPOINTS unset; skipping clusterview integration test")
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("etcd dial: %v", err)
	}
	root := fmt.Sprintf("/cex/counter-bff-test-%d-%s", time.Now().UnixNano(), t.Name())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = cli.Delete(ctx, root, clientv3.WithPrefix())
		_ = cli.Close()
	})
	return cli, root
}

func putAssignment(t *testing.T, cli *clientv3.Client, root string, a Assignment) {
	t.Helper()
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("%s/assignment/vshard-%03d", root, a.VShardID)
	if _, err := cli.Put(context.Background(), key, string(data)); err != nil {
		t.Fatal(err)
	}
}

func putNode(t *testing.T, cli *clientv3.Client, root string, n Node) {
	t.Helper()
	data, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	key := root + "/nodes/" + n.ID
	if _, err := cli.Put(context.Background(), key, string(data)); err != nil {
		t.Fatal(err)
	}
}

// TestWatcher_RunSeesInitialAndChange starts with one assignment + one
// node, confirms Lookup picks it up, then rewrites ownership to a
// second node and waits for the watcher to observe the change.
func TestWatcher_RunSeesInitialAndChange(t *testing.T) {
	cli, root := requireEtcd(t)
	const vshardCount = 8

	putNode(t, cli, root, Node{ID: "node-A", Endpoint: "10.0.0.1:8081"})
	putAssignment(t, cli, root, Assignment{
		VShardID: 0, Owner: "node-A", Epoch: 1, State: StateActive,
	})

	w, err := New(Config{
		Client: cli, RootPrefix: root, VShardCount: vshardCount,
		Logger: zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	u0 := firstUserHashingTo(t, 0, vshardCount)

	// Wait for the initial resync to have loaded our seeded state.
	if !waitFor(5*time.Second, func() bool {
		ep, ok := w.Lookup(u0)
		return ok && ep == "10.0.0.1:8081"
	}) {
		t.Fatalf("initial Lookup did not converge on node-A")
	}

	// Change ownership.
	putNode(t, cli, root, Node{ID: "node-B", Endpoint: "10.0.0.2:8081"})
	putAssignment(t, cli, root, Assignment{
		VShardID: 0, Owner: "node-B", Epoch: 2, State: StateActive,
	})

	if !waitFor(5*time.Second, func() bool {
		ep, ok := w.Lookup(u0)
		return ok && ep == "10.0.0.2:8081"
	}) {
		t.Fatalf("watcher did not pick up ownership change to node-B")
	}

	cancel()
	<-done
}

// TestWatcher_ResyncForces: writing directly to etcd usually triggers
// a watch event, but Resync() must also succeed as an on-demand
// refresh (used by dispatcher on FailedPrecondition retries).
func TestWatcher_ResyncForces(t *testing.T) {
	cli, root := requireEtcd(t)
	putNode(t, cli, root, Node{ID: "node-A", Endpoint: "10.0.0.1:8081"})

	w, err := New(Config{
		Client: cli, RootPrefix: root, VShardCount: 4,
		Logger: zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait for initial empty-assignment-table resync to settle.
	time.Sleep(200 * time.Millisecond)

	putAssignment(t, cli, root, Assignment{
		VShardID: 0, Owner: "node-A", Epoch: 1, State: StateActive,
	})
	u0 := firstUserHashingTo(t, 0, 4)

	// Trigger Resync in case the Watch event is slower than the put.
	w.Resync()

	if !waitFor(5*time.Second, func() bool {
		_, ok := w.Lookup(u0)
		return ok
	}) {
		t.Fatalf("Lookup never saw the new assignment")
	}
	cancel()
	<-done
}

// helpers -------------------------------------------------------------

func waitFor(d time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if fn() {
			return true
		}
		if time.Now().After(deadline) {
			return fn()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

var _ = shard.Index // keep pkg/shard referenced when the _test pool happens to skip shard calls
