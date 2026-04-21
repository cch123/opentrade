package clustering

import "fmt"

// Keys centralises the etcd key layout so tests and operators agree on
// the same strings. Root is a trimmed prefix (no trailing slash); sub-key
// helpers re-add the slashes they need.
//
// Layout (ADR-0058):
//
//	{Root}/coordinator/leader            ← etcd Election key
//	{Root}/nodes/{node_id}               ← Node JSON, lease-bound
//	{Root}/assignment/vshard-{NNN}       ← Assignment JSON, CAS on Epoch
type Keys struct {
	Root string
}

// DefaultRoot is the canonical prefix for Counter's clustering data.
const DefaultRoot = "/cex/counter"

// NewKeys sanitises root (strips trailing slash) and returns a helper.
// Empty root falls back to DefaultRoot.
func NewKeys(root string) *Keys {
	if root == "" {
		root = DefaultRoot
	}
	// Strip any trailing slash so sub-keys don't produce "//".
	for len(root) > 1 && root[len(root)-1] == '/' {
		root = root[:len(root)-1]
	}
	return &Keys{Root: root}
}

// CoordinatorLeader is the election key candidates compete for.
func (k *Keys) CoordinatorLeader() string {
	return k.Root + "/coordinator/leader"
}

// NodesPrefix is the prefix for per-node keys; useful for range reads
// and watchers.
func (k *Keys) NodesPrefix() string {
	return k.Root + "/nodes/"
}

// Node returns the key for one node's registration record.
func (k *Keys) Node(id string) string {
	return k.NodesPrefix() + id
}

// AssignmentsPrefix is the prefix for per-vshard assignment keys.
func (k *Keys) AssignmentsPrefix() string {
	return k.Root + "/assignment/"
}

// Assignment returns the key for one vshard's assignment record. The
// three-digit zero-padded suffix keeps lexicographic order aligned with
// numeric order so `etcdctl get --prefix` reads naturally.
func (k *Keys) Assignment(id VShardID) string {
	return fmt.Sprintf("%svshard-%03d", k.AssignmentsPrefix(), id)
}
