package clustering

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

// NodeRegistry keeps this process visible in etcd for as long as Run is
// blocked. It writes the Node record under a leased key and relies on
// concurrency.Session to refresh the lease automatically; when the
// session dies (network partition, etcd outage, ctx cancel) the key
// disappears and the coordinator sees the node as gone.
//
// Lifecycle:
//
//	reg, _ := NewNodeRegistry(RegistryConfig{Client, Keys, Node, LeaseTTL})
//	go reg.Run(ctx)      // blocks until ctx done or session lost
//
// Run returns a non-nil error if the session died before ctx was
// cancelled; callers (typically Cluster.Run) restart the goroutine.
type NodeRegistry struct {
	client   *clientv3.Client
	keys     *Keys
	node     Node
	leaseTTL int
	logger   *zap.Logger
}

// RegistryConfig is the constructor argument for NewNodeRegistry.
type RegistryConfig struct {
	Client   *clientv3.Client
	Keys     *Keys
	Node     Node
	LeaseTTL int // seconds; default 10
	Logger   *zap.Logger
}

// NewNodeRegistry validates cfg. The client is reused, not owned —
// NodeRegistry never closes it.
func NewNodeRegistry(cfg RegistryConfig) (*NodeRegistry, error) {
	if cfg.Client == nil {
		return nil, errors.New("clustering: RegistryConfig.Client required")
	}
	if cfg.Keys == nil {
		return nil, errors.New("clustering: RegistryConfig.Keys required")
	}
	if cfg.Node.ID == "" {
		return nil, errors.New("clustering: RegistryConfig.Node.ID required")
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 10
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &NodeRegistry{
		client:   cfg.Client,
		keys:     cfg.Keys,
		node:     cfg.Node,
		leaseTTL: cfg.LeaseTTL,
		logger:   cfg.Logger,
	}, nil
}

// Run creates a session, writes the leased Node record, and blocks
// until ctx is cancelled (clean exit, returns ctx.Err()) or the session
// expires (error return so the caller can campaign for a new session).
func (r *NodeRegistry) Run(ctx context.Context) error {
	session, err := concurrency.NewSession(r.client, concurrency.WithTTL(r.leaseTTL))
	if err != nil {
		return fmt.Errorf("clustering: new session: %w", err)
	}
	defer session.Close()

	data, err := json.Marshal(r.node)
	if err != nil {
		return fmt.Errorf("clustering: marshal node: %w", err)
	}
	key := r.keys.Node(r.node.ID)
	if _, err := r.client.Put(ctx, key, string(data), clientv3.WithLease(session.Lease())); err != nil {
		return fmt.Errorf("clustering: put node %s: %w", key, err)
	}
	r.logger.Info("node registered",
		zap.String("id", r.node.ID),
		zap.String("endpoint", r.node.Endpoint),
		zap.String("key", key),
		zap.Int("lease_ttl_sec", r.leaseTTL))

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.Done():
		return errors.New("clustering: node registry session expired")
	}
}
