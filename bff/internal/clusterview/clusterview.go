// Package clusterview is the BFF-side read-only view of the Counter
// cluster table (ADR-0058). It watches /cex/counter/assignment and
// /cex/counter/nodes, maintains an in-memory map, and answers
// Lookup(userID) → {node_endpoint, ok} so RPC handlers can send each
// request directly to the vshard's current owner.
//
// Design notes:
//
//   - The watcher never writes back to etcd; it is a pure consumer
//     of the schema Counter publishes.
//   - The JSON types are declared locally (not imported from
//     counter/internal/clustering) because that is an internal package;
//     the compatibility contract is the JSON schema + etcd key layout.
//   - On any watch event we full-list both prefixes and rebuild the
//     maps. Keeps the event handling trivial; the trade-off is one
//     extra range read per change, which is cheap at 256 keys + N
//     nodes and matches how rarely membership changes in practice.
//   - Resync() lets RPC handlers force a refresh when they get a
//     FailedPrecondition back from a node (ADR-0058 §5): the cached
//     owner is stale; trigger a resync, then retry.

package clusterview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/shard"
)

// StateActive matches counter/internal/clustering.StateActive. Only
// ACTIVE assignments are eligible for routing; MIGRATING /
// HANDOFF_READY mean the handoff is in flight and the owner cannot
// serve traffic cleanly.
const StateActive = "ACTIVE"

// Assignment mirrors counter/internal/clustering.Assignment's JSON
// layout. Keep the field names / types in sync when that side
// changes; they are the cross-module contract.
type Assignment struct {
	VShardID int    `json:"vshard_id"`
	Owner    string `json:"owner"`
	Epoch    uint64 `json:"epoch"`
	State    string `json:"state"`
	Target   string `json:"target,omitempty"`
}

// Node mirrors counter/internal/clustering.Node's JSON layout.
type Node struct {
	ID          string `json:"id"`
	Endpoint    string `json:"endpoint"`
	Capacity    int    `json:"capacity"`
	StartedAtMS int64  `json:"started_at_ms"`
}

// Config wires a Watcher. All fields are required.
type Config struct {
	Client      *clientv3.Client
	RootPrefix  string // e.g. "/cex/counter"; trailing slash trimmed
	VShardCount int    // must match counter --vshard-count
	Logger      *zap.Logger
}

// Watcher is the BFF's in-memory cluster view.
type Watcher struct {
	client      *clientv3.Client
	root        string
	vshardCount int
	logger      *zap.Logger

	mu          sync.RWMutex
	assignments map[int]Assignment
	nodes       map[string]Node

	resyncCh chan struct{}
}

// New validates cfg; borrowed client is not closed by Watcher.
func New(cfg Config) (*Watcher, error) {
	if cfg.Client == nil {
		return nil, errors.New("clusterview: Config.Client required")
	}
	if cfg.VShardCount <= 0 {
		return nil, errors.New("clusterview: Config.VShardCount must be > 0")
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	root := cfg.RootPrefix
	for len(root) > 1 && root[len(root)-1] == '/' {
		root = root[:len(root)-1]
	}
	if root == "" {
		root = "/cex/counter"
	}
	return &Watcher{
		client:      cfg.Client,
		root:        root,
		vshardCount: cfg.VShardCount,
		logger:      cfg.Logger,
		assignments: make(map[int]Assignment),
		nodes:       make(map[string]Node),
		resyncCh:    make(chan struct{}, 1),
	}, nil
}

// Run blocks until ctx is cancelled. It does an initial full list,
// spawns two etcd watchers, and rebuilds the in-memory maps on every
// event. Resync() pokes the loop to re-list even when no etcd change
// has landed (for post-FailedPrecondition retries).
func (w *Watcher) Run(ctx context.Context) error {
	if err := w.resync(ctx); err != nil {
		return fmt.Errorf("initial cluster resync: %w", err)
	}

	assignWC := w.client.Watch(ctx, w.assignPrefix(), clientv3.WithPrefix())
	nodeWC := w.client.Watch(ctx, w.nodePrefix(), clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp, ok := <-assignWC:
			if !ok {
				return errors.New("clusterview: assignment watcher closed")
			}
			if err := resp.Err(); err != nil {
				w.logger.Warn("assignment watch error", zap.Error(err))
			}
			if err := w.resync(ctx); err != nil {
				w.logger.Warn("resync after assignment event", zap.Error(err))
			}
		case resp, ok := <-nodeWC:
			if !ok {
				return errors.New("clusterview: node watcher closed")
			}
			if err := resp.Err(); err != nil {
				w.logger.Warn("node watch error", zap.Error(err))
			}
			if err := w.resync(ctx); err != nil {
				w.logger.Warn("resync after node event", zap.Error(err))
			}
		case <-w.resyncCh:
			if err := w.resync(ctx); err != nil {
				w.logger.Warn("on-demand resync", zap.Error(err))
			}
		}
	}
}

// Lookup returns the node endpoint currently owning the user's vshard,
// or ok=false when either the vshard has no ACTIVE assignment or the
// owner node isn't registered. Callers treat ok=false as
// "FailedPrecondition" — the BFF has stale routing info and should
// Resync + retry (ADR-0058 §5).
func (w *Watcher) Lookup(userID string) (endpoint string, ok bool) {
	vid := shard.Index(userID, w.vshardCount)
	w.mu.RLock()
	defer w.mu.RUnlock()
	a, ok := w.assignments[vid]
	if !ok || a.State != StateActive {
		return "", false
	}
	n, ok := w.nodes[a.Owner]
	if !ok || n.Endpoint == "" {
		return "", false
	}
	return n.Endpoint, true
}

// Resync signals the Run loop to re-list etcd. Safe to call
// concurrently; drops duplicates instead of queuing.
func (w *Watcher) Resync() {
	select {
	case w.resyncCh <- struct{}{}:
	default:
	}
}

// --- internal ---

func (w *Watcher) assignPrefix() string { return w.root + "/assignment/" }
func (w *Watcher) nodePrefix() string   { return w.root + "/nodes/" }

// resync reads both prefixes with a bounded timeout and atomically
// swaps the two cached maps.
func (w *Watcher) resync(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	aResp, err := w.client.Get(ctx, w.assignPrefix(), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("list assignments: %w", err)
	}
	nResp, err := w.client.Get(ctx, w.nodePrefix(), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	newAssign := make(map[int]Assignment, len(aResp.Kvs))
	for _, kv := range aResp.Kvs {
		var a Assignment
		if err := json.Unmarshal(kv.Value, &a); err != nil {
			w.logger.Warn("skip malformed assignment",
				zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		newAssign[a.VShardID] = a
	}
	newNodes := make(map[string]Node, len(nResp.Kvs))
	for _, kv := range nResp.Kvs {
		var n Node
		if err := json.Unmarshal(kv.Value, &n); err != nil {
			w.logger.Warn("skip malformed node",
				zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		newNodes[n.ID] = n
	}

	w.mu.Lock()
	w.assignments = newAssign
	w.nodes = newNodes
	w.mu.Unlock()
	w.logger.Debug("cluster view resynced",
		zap.Int("assignments", len(newAssign)),
		zap.Int("nodes", len(newNodes)))
	return nil
}
