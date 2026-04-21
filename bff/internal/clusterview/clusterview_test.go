package clusterview

import (
	"strconv"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/xargin/opentrade/pkg/shard"
)

// TestLookup_ActiveOwner seeds two assignments + two nodes directly
// into the Watcher (no etcd) so we can assert the deterministic
// user_id → endpoint mapping. The Run loop and resync logic are
// covered by the integration suite.
func TestLookup_ActiveOwner(t *testing.T) {
	w, err := New(Config{Client: fakeClient(), VShardCount: 256})
	if err != nil {
		t.Fatal(err)
	}
	w.assignments = map[int]Assignment{
		0:  {VShardID: 0, Owner: "node-A", Epoch: 1, State: StateActive},
		42: {VShardID: 42, Owner: "node-B", Epoch: 3, State: StateActive},
		99: {VShardID: 99, Owner: "node-A", Epoch: 1, State: "MIGRATING"}, // non-active → not routable
	}
	w.nodes = map[string]Node{
		"node-A": {ID: "node-A", Endpoint: "10.0.0.1:8081"},
		"node-B": {ID: "node-B", Endpoint: "10.0.0.2:8081"},
	}

	u0 := firstUserHashingTo(t, 0, 256)
	u42 := firstUserHashingTo(t, 42, 256)

	if ep, ok := w.Lookup(u0); !ok || ep != "10.0.0.1:8081" {
		t.Errorf("Lookup(%q) = (%q, %v), want node-A endpoint", u0, ep, ok)
	}
	if ep, ok := w.Lookup(u42); !ok || ep != "10.0.0.2:8081" {
		t.Errorf("Lookup(%q) = (%q, %v), want node-B endpoint", u42, ep, ok)
	}

	if u99 := firstUserHashingToOrEmpty(99, 256); u99 != "" {
		if ep, ok := w.Lookup(u99); ok {
			t.Errorf("Lookup(%q) on MIGRATING vshard = (%q, true), want false", u99, ep)
		}
	}
}

// TestLookup_MissingNode: an assignment pointing at a node that no
// longer exists in /nodes/ is treated as not routable. This is the
// guard against dialling into nothing during a node death window.
func TestLookup_MissingNode(t *testing.T) {
	w, err := New(Config{Client: fakeClient(), VShardCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	w.assignments = map[int]Assignment{
		0: {VShardID: 0, Owner: "ghost", Epoch: 1, State: StateActive},
	}
	w.nodes = map[string]Node{}
	u0 := firstUserHashingTo(t, 0, 4)
	if ep, ok := w.Lookup(u0); ok {
		t.Errorf("Lookup on ghost owner = (%q, true), want false", ep)
	}
}

// TestResync_Coalesces: rapid Resync() calls collapse to at most one
// queued event. The Run loop reads one resync per trip around the
// select, so coalescing prevents pile-up during migration storms.
func TestResync_Coalesces(t *testing.T) {
	w, err := New(Config{Client: fakeClient(), VShardCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		w.Resync()
	}
	select {
	case <-w.resyncCh:
	default:
		t.Fatal("Resync never landed on channel")
	}
	select {
	case <-w.resyncCh:
		t.Fatal("Resync queued more than one event")
	default:
	}
}

// fakeClient satisfies the non-nil pointer check in New without
// reaching etcd. Tests that need real etcd use the integration suite.
func fakeClient() *clientv3.Client {
	return &clientv3.Client{}
}

func firstUserHashingTo(t *testing.T, target, n int) string {
	t.Helper()
	if u := firstUserHashingToOrEmpty(target, n); u != "" {
		return u
	}
	t.Fatalf("no user in test pool hashes to vshard %d (n=%d)", target, n)
	return ""
}

func firstUserHashingToOrEmpty(target, n int) string {
	for i := 0; i < 100_000; i++ {
		u := "u-" + strconv.Itoa(i)
		if shard.Index(u, n) == target {
			return u
		}
	}
	return ""
}
