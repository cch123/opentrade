package clustering

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/election"
)

// Coordinator competes for the /coordinator/leader key and, once
// elected, reconciles the vshard → owner assignment table against the
// live node set: every vshard that has no assignment gets one written
// (CAS create-only) using HRW hashing.
//
// Phase 2 scope: *initial* assignment only. The leader does a single
// reconcile pass on election, then idles until it loses leadership or
// ctx is cancelled. Automatic failover on node churn (phase 7) and
// active migration (phase 6) will add more behaviour to lead() later.
//
// Safety properties:
//
//   - Only one Coordinator at a time actively writes, because only the
//     election leader reaches lead(). Followers block in Campaign.
//   - Writes use a transactional create-only CAS so two leaders — if a
//     split-brain ever happened — cannot clobber each other's work; the
//     loser just sees a skipped write.
type Coordinator struct {
	client      *clientv3.Client
	keys        *Keys
	nodeID      string
	vshardCount int
	leaseTTL    int
	logger      *zap.Logger

	campaignBackoff time.Duration
}

// CoordinatorConfig is the constructor argument.
type CoordinatorConfig struct {
	Client      *clientv3.Client
	Keys        *Keys
	NodeID      string // this process's id; written as the leader value
	VShardCount int    // typically 256 (ADR-0058)
	LeaseTTL    int    // seconds; default 10

	// CampaignBackoff is the wait between failed Campaigns. Default 2s.
	CampaignBackoff time.Duration

	Logger *zap.Logger
}

// NewCoordinator validates cfg. The etcd client is borrowed; the
// coordinator never closes it.
func NewCoordinator(cfg CoordinatorConfig) (*Coordinator, error) {
	if cfg.Client == nil {
		return nil, errors.New("clustering: CoordinatorConfig.Client required")
	}
	if cfg.Keys == nil {
		return nil, errors.New("clustering: CoordinatorConfig.Keys required")
	}
	if cfg.NodeID == "" {
		return nil, errors.New("clustering: CoordinatorConfig.NodeID required")
	}
	if cfg.VShardCount <= 0 {
		return nil, errors.New("clustering: CoordinatorConfig.VShardCount must be > 0")
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 10
	}
	if cfg.CampaignBackoff <= 0 {
		cfg.CampaignBackoff = 2 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &Coordinator{
		client:          cfg.Client,
		keys:            cfg.Keys,
		nodeID:          cfg.NodeID,
		vshardCount:     cfg.VShardCount,
		leaseTTL:        cfg.LeaseTTL,
		campaignBackoff: cfg.CampaignBackoff,
		logger:          cfg.Logger,
	}, nil
}

// Run loops forever: campaign → lead until lost → re-campaign. Exits
// only when ctx is cancelled (returns ctx.Err()) or the election
// cannot be constructed at all.
func (c *Coordinator) Run(ctx context.Context) error {
	elec, err := election.New(election.Config{
		Client:   c.client,
		Path:     c.keys.CoordinatorLeader(),
		Value:    c.nodeID,
		LeaseTTL: c.leaseTTL,
	})
	if err != nil {
		return fmt.Errorf("clustering: election init: %w", err)
	}
	defer func() { _ = elec.Close() }()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		c.logger.Info("coordinator campaigning", zap.String("node", c.nodeID))
		if err := elec.Campaign(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Error("coordinator campaign failed", zap.Error(err))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.campaignBackoff):
			}
			continue
		}
		c.logger.Info("coordinator elected", zap.String("node", c.nodeID))

		if err := c.lead(ctx, elec.LostCh()); err != nil &&
			!errors.Is(err, context.Canceled) {
			c.logger.Error("coordinator lead exited", zap.Error(err))
		}

		if ctx.Err() != nil {
			resignCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = elec.Resign(resignCtx)
			cancel()
			return ctx.Err()
		}
		c.logger.Warn("coordinator lost leadership; re-campaigning")
	}
}

// lead runs the leader-only control loop. Phase 2 does one reconcile
// pass then idles until ctx / lost fires.
func (c *Coordinator) lead(ctx context.Context, lost <-chan struct{}) error {
	leadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-lost:
			cancel()
		case <-leadCtx.Done():
		}
	}()

	if err := c.reconcile(leadCtx); err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}

	<-leadCtx.Done()
	return leadCtx.Err()
}

// reconcile reads the current nodes + assignments from etcd and writes
// assignments for any vshard that lacks one. Writes are CAS create-only
// so the pass is idempotent: running it twice, or racing against another
// leader, never corrupts an existing assignment.
func (c *Coordinator) reconcile(ctx context.Context) error {
	nodes, err := c.listNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	if len(nodes) == 0 {
		c.logger.Warn("reconcile: no live nodes, skipping initial assignment")
		return nil
	}

	nodeIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		nodeIDs = append(nodeIDs, n.ID)
	}

	assignments, err := c.listAssignments(ctx)
	if err != nil {
		return fmt.Errorf("list assignments: %w", err)
	}

	vshards := AllVShards(c.vshardCount)
	desired := AssignVShards(vshards, nodeIDs)

	created, skipped := 0, 0
	for _, v := range vshards {
		if _, ok := assignments[v]; ok {
			skipped++
			continue
		}
		a := Assignment{
			VShardID: v,
			Owner:    desired[v],
			Epoch:    1,
			State:    StateActive,
		}
		wrote, err := c.createAssignment(ctx, a)
		if err != nil {
			return fmt.Errorf("create assignment vshard-%03d: %w", v, err)
		}
		if wrote {
			created++
		} else {
			// Lost the CAS race — someone else (racing leader)
			// beat us. That's fine; move on.
			skipped++
		}
	}
	c.logger.Info("reconcile complete",
		zap.Int("vshards", c.vshardCount),
		zap.Int("live_nodes", len(nodeIDs)),
		zap.Int("created", created),
		zap.Int("skipped", skipped))
	return nil
}

// listNodes returns every node record under /nodes/. Malformed entries
// are skipped with a warning rather than aborting the whole pass.
func (c *Coordinator) listNodes(ctx context.Context) ([]Node, error) {
	resp, err := c.client.Get(ctx, c.keys.NodesPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]Node, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var n Node
		if err := json.Unmarshal(kv.Value, &n); err != nil {
			c.logger.Warn("skip malformed node record",
				zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

// listAssignments returns the current assignment table keyed by
// VShardID. Malformed entries are logged + dropped.
func (c *Coordinator) listAssignments(ctx context.Context) (map[VShardID]Assignment, error) {
	resp, err := c.client.Get(ctx, c.keys.AssignmentsPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make(map[VShardID]Assignment, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var a Assignment
		if err := json.Unmarshal(kv.Value, &a); err != nil {
			c.logger.Warn("skip malformed assignment record",
				zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		out[a.VShardID] = a
	}
	return out, nil
}

// createAssignment does a CAS create-only write. Returns (wrote, err):
// wrote == false with nil err means the key already existed (another
// writer got there first); that is not an error case.
func (c *Coordinator) createAssignment(ctx context.Context, a Assignment) (bool, error) {
	data, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	key := c.keys.Assignment(a.VShardID)
	resp, err := c.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}
