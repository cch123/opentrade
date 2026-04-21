//go:build integration

// Integration tests for the clustering package. Gated behind the
// `integration` build tag — regular `go test ./...` stays hermetic.
//
// Usage:
//
//   docker compose up -d etcd
//   ETCD_ENDPOINTS=localhost:2379 go test -tags=integration ./internal/clustering/...
//
// Each test scopes itself under a unique root prefix so tests are
// independent even when run in parallel or repeated back-to-back.

package clustering

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// requireEtcd returns a client + a unique root prefix, or skips the test
// if ETCD_ENDPOINTS is unset. Cleanup wipes everything under the root.
func requireEtcd(t *testing.T) (*clientv3.Client, string) {
	t.Helper()
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("ETCD_ENDPOINTS unset; skipping clustering integration test")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("etcd dial: %v", err)
	}
	root := fmt.Sprintf("/cex/counter-test-%d-%s", time.Now().UnixNano(), t.Name())
	// Keep the root free of characters etcd treats specially.
	root = strings.ReplaceAll(root, "/", "/")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.Delete(ctx, root, clientv3.WithPrefix())
		_ = client.Close()
	})
	return client, root
}

// waitFor polls fn every 50ms until it returns true or ctx expires.
func waitFor(ctx context.Context, fn func() bool) bool {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		if fn() {
			return true
		}
		select {
		case <-ctx.Done():
			return fn()
		case <-t.C:
		}
	}
}

// TestCluster_NodeRegistrationAndTeardown: starting the cluster writes
// /nodes/{id}; cancelling the context removes it.
func TestCluster_NodeRegistrationAndTeardown(t *testing.T) {
	client, root := requireEtcd(t)

	cl, err := New(Config{
		Client: client,
		Node: Node{
			ID:          "node-A",
			Endpoint:    "10.0.0.1:8081",
			Capacity:    32,
			StartedAtMS: time.Now().UnixMilli(),
		},
		VShardCount: 16, // small so tests are fast
		LeaseTTL:    5,
		RootPrefix:  root,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cl.Run(ctx) }()

	// Give the registry a moment to write the key.
	deadline, deadlineCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deadlineCancel()
	ok := waitFor(deadline, func() bool {
		resp, err := client.Get(context.Background(), cl.Keys().Node("node-A"))
		return err == nil && len(resp.Kvs) == 1
	})
	if !ok {
		t.Fatal("node key never appeared")
	}

	// Cancel and confirm key disappears (lease revoked).
	cancel()
	if err := <-done; err != nil && err != context.Canceled {
		t.Fatalf("Run returned %v", err)
	}

	// Wait for etcd to propagate the delete.
	deadline2, deadline2Cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deadline2Cancel()
	gone := waitFor(deadline2, func() bool {
		resp, err := client.Get(context.Background(), cl.Keys().Node("node-A"))
		return err == nil && len(resp.Kvs) == 0
	})
	if !gone {
		t.Fatal("node key lingered after shutdown")
	}
}

// TestCluster_InitialAssignment: a single node starts; the coordinator
// wins leadership and fills the assignment table with 256 entries, all
// owned by that node.
func TestCluster_InitialAssignment(t *testing.T) {
	client, root := requireEtcd(t)
	vshardCount := 32 // smaller for speed; 256 is tested in prod config
	keys := NewKeys(root)

	cl, err := New(Config{
		Client: client,
		Node: Node{
			ID:          "node-A",
			Endpoint:    "10.0.0.1:8081",
			Capacity:    vshardCount,
			StartedAtMS: time.Now().UnixMilli(),
		},
		VShardCount: vshardCount,
		LeaseTTL:    5,
		RootPrefix:  root,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cl.Run(ctx) }()

	// Wait for all assignments to land.
	deadline, deadlineCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer deadlineCancel()
	ok := waitFor(deadline, func() bool {
		resp, err := client.Get(context.Background(),
			keys.AssignmentsPrefix(), clientv3.WithPrefix())
		return err == nil && len(resp.Kvs) == vshardCount
	})
	if !ok {
		resp, _ := client.Get(context.Background(),
			keys.AssignmentsPrefix(), clientv3.WithPrefix())
		t.Fatalf("expected %d assignments, got %d", vshardCount, len(resp.Kvs))
	}

	// Every assignment should own to node-A, state Active, epoch 1.
	resp, err := client.Get(context.Background(),
		keys.AssignmentsPrefix(), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range resp.Kvs {
		var a Assignment
		if err := json.Unmarshal(kv.Value, &a); err != nil {
			t.Fatalf("unmarshal %s: %v", kv.Key, err)
		}
		if a.Owner != "node-A" {
			t.Errorf("%s owner = %q, want node-A", kv.Key, a.Owner)
		}
		if a.Epoch != 1 {
			t.Errorf("%s epoch = %d, want 1", kv.Key, a.Epoch)
		}
		if a.State != StateActive {
			t.Errorf("%s state = %q, want %q", kv.Key, a.State, StateActive)
		}
	}

	cancel()
	<-done
}

// TestCluster_ReconcileIsIdempotent: once the assignment table is full,
// re-running reconcile (by restarting the coordinator) must not change
// any entries.
func TestCluster_ReconcileIsIdempotent(t *testing.T) {
	client, root := requireEtcd(t)
	vshardCount := 16
	keys := NewKeys(root)

	startCluster := func(id string) (context.CancelFunc, <-chan error) {
		cl, err := New(Config{
			Client: client,
			Node: Node{
				ID:          id,
				Endpoint:    "10.0.0.1:8081",
				StartedAtMS: time.Now().UnixMilli(),
			},
			VShardCount: vshardCount,
			LeaseTTL:    5,
			RootPrefix:  root,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- cl.Run(ctx) }()
		return cancel, done
	}

	cancel1, done1 := startCluster("node-A")
	// Wait for initial fill.
	deadline, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	waitFor(deadline, func() bool {
		resp, _ := client.Get(context.Background(), keys.AssignmentsPrefix(), clientv3.WithPrefix())
		return len(resp.Kvs) == vshardCount
	})

	// Snapshot the assignment contents.
	beforeResp, err := client.Get(context.Background(),
		keys.AssignmentsPrefix(), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeResp.Kvs) != vshardCount {
		t.Fatalf("got %d assignments before restart, want %d", len(beforeResp.Kvs), vshardCount)
	}
	before := make(map[string]string, vshardCount)
	for _, kv := range beforeResp.Kvs {
		before[string(kv.Key)] = string(kv.Value)
	}

	// Tear down, then start a second cluster using the same root but a
	// different node id. Its reconcile should skip every vshard.
	cancel1()
	<-done1

	cancel2, done2 := startCluster("node-B")
	// Even if node-B becomes coordinator, it won't change anything,
	// but we need to let it come up before checking.
	time.Sleep(2 * time.Second)

	afterResp, err := client.Get(context.Background(),
		keys.AssignmentsPrefix(), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range afterResp.Kvs {
		if before[string(kv.Key)] != string(kv.Value) {
			t.Errorf("assignment %s changed across restart: %q → %q",
				kv.Key, before[string(kv.Key)], string(kv.Value))
		}
	}

	cancel2()
	<-done2
}

// TestCluster_TwoCandidatesOneLeader: two clusters share the same root,
// one becomes coordinator, both node records exist. The non-leader must
// never write assignments.
func TestCluster_TwoCandidatesOneLeader(t *testing.T) {
	client, root := requireEtcd(t)
	vshardCount := 16
	keys := NewKeys(root)

	startCluster := func(id string) (*Cluster, context.CancelFunc, <-chan error) {
		cl, err := New(Config{
			Client: client,
			Node: Node{
				ID:          id,
				Endpoint:    "10.0.0.1:8081",
				StartedAtMS: time.Now().UnixMilli(),
			},
			VShardCount: vshardCount,
			LeaseTTL:    5,
			RootPrefix:  root,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- cl.Run(ctx) }()
		return cl, cancel, done
	}

	_, cancelA, doneA := startCluster("node-A")
	_, cancelB, doneB := startCluster("node-B")

	// Wait for both node records + full assignment table.
	deadline, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	ready := waitFor(deadline, func() bool {
		nodes, err := client.Get(context.Background(), keys.NodesPrefix(), clientv3.WithPrefix())
		if err != nil || len(nodes.Kvs) != 2 {
			return false
		}
		assigns, err := client.Get(context.Background(), keys.AssignmentsPrefix(), clientv3.WithPrefix())
		return err == nil && len(assigns.Kvs) == vshardCount
	})
	if !ready {
		t.Fatal("cluster did not converge in time")
	}

	// Multiple candidates register under CoordinatorLeader prefix
	// (concurrency.Election gives each a unique suffix); the leader is
	// the one with the smallest CreateRevision. There must be exactly
	// one such winner, and its value must be a node id we started.
	leaderResp, err := client.Get(context.Background(),
		keys.CoordinatorLeader(), clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend),
		clientv3.WithLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(leaderResp.Kvs) != 1 {
		t.Fatalf("no leader found under %s (count=%d)", keys.CoordinatorLeader(), len(leaderResp.Kvs))
	}
	leader := string(leaderResp.Kvs[0].Value)
	if leader != "node-A" && leader != "node-B" {
		t.Fatalf("unexpected leader value %q", leader)
	}

	// Clean shutdown — order shouldn't matter.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); cancelA(); <-doneA }()
	go func() { defer wg.Done(); cancelB(); <-doneB }()
	wg.Wait()
}

// expectNext reads the next list from ch or fails the test on timeout /
// closed channel. Helper for the WatchAssignedVShards tests.
func expectNext(t *testing.T, ch <-chan []VShardID, timeout time.Duration) []VShardID {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return v
	case <-time.After(timeout):
		t.Fatal("timeout waiting for watch emit")
		return nil
	}
}

// writeAssignment overwrites one vshard's record. Used by the watch
// tests to simulate a migration / takeover from outside the cluster.
func writeAssignment(t *testing.T, client *clientv3.Client, keys *Keys, a Assignment) {
	t.Helper()
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(context.Background(), keys.Assignment(a.VShardID), string(data)); err != nil {
		t.Fatal(err)
	}
}

// TestCluster_WatchAssignedVShards_InitialAndOnChange covers the phase
// 3b-1 contract: WatchAssignedVShards emits a snapshot on subscribe,
// then a fresh list on every assignment change that affects what this
// node owns. Consecutive identical emissions are suppressed.
func TestCluster_WatchAssignedVShards_InitialAndOnChange(t *testing.T) {
	client, root := requireEtcd(t)
	vshardCount := 8
	keys := NewKeys(root)

	cl, err := New(Config{
		Client: client,
		Node: Node{
			ID:          "node-A",
			Endpoint:    "10.0.0.1:8081",
			StartedAtMS: time.Now().UnixMilli(),
		},
		VShardCount: vshardCount,
		LeaseTTL:    5,
		RootPrefix:  root,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cl.Run(ctx) }()

	// Wait until the coordinator has filled the table — otherwise
	// listMyVShards may see a partial view.
	deadline, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	waitFor(deadline, func() bool {
		resp, _ := client.Get(context.Background(), keys.AssignmentsPrefix(), clientv3.WithPrefix())
		return len(resp.Kvs) == vshardCount
	})

	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	ch, err := cl.WatchAssignedVShards(watchCtx)
	if err != nil {
		t.Fatalf("WatchAssignedVShards: %v", err)
	}

	// Initial emit: all 8 owned by node-A.
	first := expectNext(t, ch, 5*time.Second)
	if len(first) != vshardCount {
		t.Fatalf("initial set = %d, want %d: %v", len(first), vshardCount, first)
	}

	// Simulate an external takeover of vshard-003.
	writeAssignment(t, client, keys, Assignment{
		VShardID: 3,
		Owner:    "someone-else",
		Epoch:    2,
		State:    StateActive,
	})
	second := expectNext(t, ch, 5*time.Second)
	if len(second) != vshardCount-1 {
		t.Fatalf("after takeover = %d, want %d: %v", len(second), vshardCount-1, second)
	}
	for _, v := range second {
		if v == 3 {
			t.Errorf("vshard-003 should have dropped out, still in %v", second)
		}
	}

	// Restore ownership.
	writeAssignment(t, client, keys, Assignment{
		VShardID: 3,
		Owner:    "node-A",
		Epoch:    3,
		State:    StateActive,
	})
	third := expectNext(t, ch, 5*time.Second)
	if len(third) != vshardCount {
		t.Fatalf("after restore = %d, want %d: %v", len(third), vshardCount, third)
	}

	watchCancel()
	cancel()
	<-done
}

// TestCluster_WatchAssignedVShards_DropsOnStateTransition: a vshard
// whose owner stays self but whose state moves to MIGRATING must drop
// off the assigned set — the worker should step down during the
// handoff window (ADR-0058 §Cold Handoff).
func TestCluster_WatchAssignedVShards_DropsOnStateTransition(t *testing.T) {
	client, root := requireEtcd(t)
	vshardCount := 4
	keys := NewKeys(root)

	cl, err := New(Config{
		Client:      client,
		Node:        Node{ID: "node-A", StartedAtMS: time.Now().UnixMilli()},
		VShardCount: vshardCount,
		LeaseTTL:    5,
		RootPrefix:  root,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cl.Run(ctx) }()

	deadline, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	waitFor(deadline, func() bool {
		resp, _ := client.Get(context.Background(), keys.AssignmentsPrefix(), clientv3.WithPrefix())
		return len(resp.Kvs) == vshardCount
	})

	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	ch, err := cl.WatchAssignedVShards(watchCtx)
	if err != nil {
		t.Fatal(err)
	}

	// Drain initial.
	first := expectNext(t, ch, 5*time.Second)
	if len(first) != vshardCount {
		t.Fatalf("initial = %d", len(first))
	}

	// Flip vshard-001 to MIGRATING (owner unchanged). The watcher must
	// emit a shorter set.
	writeAssignment(t, client, keys, Assignment{
		VShardID: 1,
		Owner:    "node-A",
		Epoch:    2,
		State:    StateMigrating,
		Target:   "node-B",
	})
	second := expectNext(t, ch, 5*time.Second)
	if len(second) != vshardCount-1 {
		t.Fatalf("after state flip = %d, want %d: %v", len(second), vshardCount-1, second)
	}
	for _, v := range second {
		if v == 1 {
			t.Errorf("migrating vshard-001 should have dropped, set %v", second)
		}
	}

	watchCancel()
	cancel()
	<-done
}

// TestCluster_PreexistingAssignmentNotClobbered: pre-write an Assignment
// with custom Owner + Epoch, then run the cluster. Reconcile must leave
// that vshard alone while filling in the rest.
func TestCluster_PreexistingAssignmentNotClobbered(t *testing.T) {
	client, root := requireEtcd(t)
	vshardCount := 8
	keys := NewKeys(root)

	// Seed vshard-000 with an intentional off-design assignment.
	seed := Assignment{
		VShardID: 0,
		Owner:    "someone-else",
		Epoch:    99,
		State:    StateActive,
	}
	data, err := json.Marshal(seed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(context.Background(), keys.Assignment(0), string(data)); err != nil {
		t.Fatal(err)
	}

	cl, err := New(Config{
		Client: client,
		Node: Node{
			ID:          "node-A",
			Endpoint:    "10.0.0.1:8081",
			StartedAtMS: time.Now().UnixMilli(),
		},
		VShardCount: vshardCount,
		LeaseTTL:    5,
		RootPrefix:  root,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cl.Run(ctx) }()

	// Wait for full fill.
	deadline, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	waitFor(deadline, func() bool {
		resp, _ := client.Get(context.Background(), keys.AssignmentsPrefix(), clientv3.WithPrefix())
		return len(resp.Kvs) == vshardCount
	})

	// vshard-000 should still belong to someone-else at epoch 99.
	resp, err := client.Get(context.Background(), keys.Assignment(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("vshard-000 missing")
	}
	var got Assignment
	if err := json.Unmarshal(resp.Kvs[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got.Owner != "someone-else" || got.Epoch != 99 {
		t.Errorf("vshard-000 overwritten: %+v (want someone-else @ 99)", got)
	}

	cancel()
	<-done
}
