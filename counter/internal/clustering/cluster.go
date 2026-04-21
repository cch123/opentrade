package clustering

import (
	"context"
	"errors"
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
		registry:        reg,
		coordinator:     coord,
		keys:            keys,
		logger:          cfg.Logger,
		registryBackoff: cfg.RegistryBackoff,
	}, nil
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
