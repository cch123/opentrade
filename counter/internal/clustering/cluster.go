package clustering

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Cluster is the facade main.go talks to. It composes two long-running
// goroutines:
//
//   - a node registry loop that keeps /nodes/{id} live for as long as
//     Run is active, restarting on session loss;
//   - a coordinator loop that campaigns for /coordinator/leader and, when
//     elected, maintains the assignment table.
//
// Both loops are restart-tolerant internally; Cluster.Run returns only
// when its context is cancelled.
type Cluster struct {
	client      *clientv3.Client
	nodeID      string
	registry    *NodeRegistry
	coordinator *Coordinator
	keys        *Keys
	logger      *zap.Logger

	registryBackoff time.Duration
}

// Config is the all-in-one constructor argument.
type Config struct {
	// Client is the etcd client shared with the rest of the process.
	// Cluster does not own it.
	Client *clientv3.Client

	// Node is this process's self-description (id, endpoint, …).
	Node Node

	// VShardCount is the total number of virtual shards. 256 for
	// production (ADR-0058); smaller for tests.
	VShardCount int

	// LeaseTTL is the etcd session TTL, in seconds. Default 10.
	// Used for BOTH the node lease and the coordinator election.
	LeaseTTL int

	// RootPrefix overrides the default "/cex/counter" — useful for
	// running multiple isolated test clusters in one etcd.
	RootPrefix string

	// RegistryBackoff is the wait between node-registry restart
	// attempts after a session loss. Default 2s.
	RegistryBackoff time.Duration

	Logger *zap.Logger
}

// New validates cfg and wires up the registry + coordinator.
func New(cfg Config) (*Cluster, error) {
	if cfg.Client == nil {
		return nil, errors.New("clustering: Config.Client required")
	}
	if cfg.VShardCount <= 0 {
		return nil, errors.New("clustering: Config.VShardCount must be > 0")
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 10
	}
	if cfg.RegistryBackoff <= 0 {
		cfg.RegistryBackoff = 2 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	keys := NewKeys(cfg.RootPrefix)

	reg, err := NewNodeRegistry(RegistryConfig{
		Client:   cfg.Client,
		Keys:     keys,
		Node:     cfg.Node,
		LeaseTTL: cfg.LeaseTTL,
		Logger:   cfg.Logger,
	})
	if err != nil {
		return nil, err
	}

	coord, err := NewCoordinator(CoordinatorConfig{
		Client:      cfg.Client,
		Keys:        keys,
		NodeID:      cfg.Node.ID,
		VShardCount: cfg.VShardCount,
		LeaseTTL:    cfg.LeaseTTL,
		Logger:      cfg.Logger,
	})
	if err != nil {
		return nil, err
	}

	return &Cluster{
		client:          cfg.Client,
		nodeID:          cfg.Node.ID,
		registry:        reg,
		coordinator:     coord,
		keys:            keys,
		logger:          cfg.Logger,
		registryBackoff: cfg.RegistryBackoff,
	}, nil
}

// WatchAssignedVShards emits the set of vshards this node currently owns
// (Owner == self && State == StateActive). It publishes an initial
// snapshot, then a fresh list every time the assignment table changes.
// Consecutive identical lists are suppressed so consumers only react to
// real ownership changes.
//
// The returned channel closes when ctx is cancelled or the watcher dies.
// Callers (the phase-3b worker manager) should start/stop per-vshard
// workers on every list they receive.
func (c *Cluster) WatchAssignedVShards(ctx context.Context) (<-chan []VShardID, error) {
	initial, err := c.listMyVShards(ctx)
	if err != nil {
		return nil, err
	}

	out := make(chan []VShardID, 1)
	out <- initial

	go func() {
		defer close(out)
		wc := c.client.Watch(ctx,
			c.keys.AssignmentsPrefix(),
			clientv3.WithPrefix())
		last := sortedCopy(initial)
		for wresp := range wc {
			if err := wresp.Err(); err != nil {
				c.logger.Warn("assignment watcher error", zap.Error(err))
				return
			}
			current, err := c.listMyVShards(ctx)
			if err != nil {
				c.logger.Warn("re-list after assignment event", zap.Error(err))
				continue
			}
			if sameSet(current, last) {
				continue
			}
			last = sortedCopy(current)
			select {
			case out <- current:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// listMyVShards does one full read of the assignment table and returns
// the vshards this node currently owns (active state only). Sorted
// ascending so comparisons are deterministic.
func (c *Cluster) listMyVShards(ctx context.Context) ([]VShardID, error) {
	resp, err := c.client.Get(ctx,
		c.keys.AssignmentsPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]VShardID, 0)
	for _, kv := range resp.Kvs {
		var a Assignment
		if err := json.Unmarshal(kv.Value, &a); err != nil {
			c.logger.Warn("skip malformed assignment",
				zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		if a.Owner != c.nodeID || a.State != StateActive {
			continue
		}
		out = append(out, a.VShardID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// sortedCopy returns a shallow copy in ascending order. Used as the
// watcher's "last emitted" snapshot for de-dup comparisons.
func sortedCopy(in []VShardID) []VShardID {
	out := make([]VShardID, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// sameSet returns true iff a and b contain the same vshards. Both are
// expected to be sorted ascending (listMyVShards / sortedCopy enforce
// this).
func sameSet(a, b []VShardID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Keys exposes the etcd schema for readers that want to observe the
// cluster (BFF, operator tools) without re-deriving prefixes.
func (c *Cluster) Keys() *Keys { return c.keys }

// Run blocks until ctx is cancelled. It starts the registry + coordinator
// goroutines concurrently; when ctx fires (or one of them returns a
// non-restart error), the other is cancelled and drained before Run
// returns.
func (c *Cluster) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		errCh <- c.runRegistryLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		errCh <- c.coordinator.Run(ctx)
	}()

	firstErr := <-errCh
	cancel()
	wg.Wait()
	return firstErr
}

// runRegistryLoop keeps the node record alive even if the underlying
// etcd session expires (network blip, etcd restart). On a session loss
// it waits RegistryBackoff then re-registers. Only ctx cancellation
// ever causes it to return.
func (c *Cluster) runRegistryLoop(ctx context.Context) error {
	for {
		err := c.registry.Run(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.logger.Warn("node registry restarting after loss", zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.registryBackoff):
		}
	}
}
