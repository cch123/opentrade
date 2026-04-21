// Command counter-migrate is the operator tool for ADR-0058 vshard
// migration. It exposes three subcommands:
//
//   counter-migrate move       --vshard N --target X
//       Fire a single ACTIVE → MIGRATING CAS and exit. The state
//       machine progresses asynchronously (Manager flushes →
//       HANDOFF_READY → Coordinator → ACTIVE@target,ep+1).
//
//   counter-migrate plan
//       Compute the HRW rebalance plan vs the current assignment
//       table and print it as a table. Does not mutate anything.
//
//   counter-migrate rebalance
//       Execute the plan: for each vshard whose current owner !=
//       HRW desired owner, run StartMigration and poll until
//       completion before moving to the next. Sequential on purpose —
//       the operator sees progress and any failure stops the batch
//       instead of leaking half-done migrations.
//
// All subcommands share --etcd and --cluster-root for locating the
// control plane.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/xargin/opentrade/counter/internal/clustering"
)

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "move":
		doMove(args)
	case "plan":
		doRebalance(args, false /* execute */)
	case "rebalance":
		doRebalance(args, true)
	case "-h", "--help", "help":
		usage(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage(os.Stderr)
		os.Exit(2)
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `usage: counter-migrate <command> [flags]

Commands:
  move       move one vshard to a target node (single CAS; async completion)
  plan       compute and print the HRW rebalance plan (no mutation)
  rebalance  compute plan + execute it one vshard at a time

Shared flags:
  --etcd=addr[,addr]     (required) etcd endpoints
  --cluster-root=/path   (default /cex/counter) etcd prefix

Run "counter-migrate <command> -h" for per-command flags.`)
}

// --------------------------------------------------------------------
// move
// --------------------------------------------------------------------

func doMove(args []string) {
	var etcd, root, target string
	var vshardID int
	var timeout time.Duration
	fs := flag.NewFlagSet("move", flag.ExitOnError)
	fs.StringVar(&etcd, "etcd", "", "comma-separated etcd endpoints (required)")
	fs.StringVar(&root, "cluster-root", clustering.DefaultRoot, "etcd root prefix for counter cluster data")
	fs.IntVar(&vshardID, "vshard", -1, "vshard id to move (required)")
	fs.StringVar(&target, "target", "", "target node id (required)")
	fs.DurationVar(&timeout, "timeout", 10*time.Second, "overall timeout for the CAS + verify")
	_ = fs.Parse(args)

	if etcd == "" || vshardID < 0 || target == "" {
		fmt.Fprintln(os.Stderr, "move requires --etcd, --vshard, --target")
		os.Exit(2)
	}
	client, cancel := dialEtcd(etcd)
	defer cancel()

	ctx, cancelCtx := context.WithTimeout(context.Background(), timeout)
	defer cancelCtx()

	keys := clustering.NewKeys(root)
	before, err := clustering.ReadAssignment(ctx, client, keys, clustering.VShardID(vshardID))
	if err != nil {
		die(fmt.Errorf("read assignment: %w", err))
	}
	fmt.Fprintf(os.Stderr,
		"current: vshard-%03d owner=%s epoch=%d state=%s target=%q\n",
		before.VShardID, before.Owner, before.Epoch, before.State, before.Target)

	if err := clustering.StartMigration(ctx, client, keys,
		clustering.VShardID(vshardID), target); err != nil {
		if errors.Is(err, clustering.ErrPreconditionMismatch) {
			die(fmt.Errorf("migration rejected: %w", err))
		}
		die(fmt.Errorf("start migration: %w", err))
	}

	after, err := clustering.ReadAssignment(ctx, client, keys, clustering.VShardID(vshardID))
	if err != nil {
		die(fmt.Errorf("read after CAS: %w", err))
	}
	fmt.Fprintf(os.Stdout,
		"MIGRATING: vshard-%03d owner=%s epoch=%d target=%s\n",
		after.VShardID, after.Owner, after.Epoch, after.Target)
	fmt.Fprintln(os.Stdout,
		"state machine runs asynchronously: owner flush -> HANDOFF_READY -> coordinator -> ACTIVE@target,epoch+1")
}

// --------------------------------------------------------------------
// plan / rebalance (shared implementation)
// --------------------------------------------------------------------

type planEntry struct {
	VShardID clustering.VShardID
	From     string
	To       string
	Epoch    uint64 // current (printed as "N → N+1")
}

func doRebalance(args []string, execute bool) {
	name := "plan"
	if execute {
		name = "rebalance"
	}
	var etcd, root string
	var overallTimeout, stepTimeout time.Duration
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.StringVar(&etcd, "etcd", "", "comma-separated etcd endpoints (required)")
	fs.StringVar(&root, "cluster-root", clustering.DefaultRoot, "etcd root prefix for counter cluster data")
	fs.DurationVar(&overallTimeout, "timeout", 10*time.Minute, "total budget for the whole batch")
	fs.DurationVar(&stepTimeout, "step-timeout", 60*time.Second, "timeout for any single vshard migration")
	_ = fs.Parse(args)

	if etcd == "" {
		fmt.Fprintln(os.Stderr, name+" requires --etcd")
		os.Exit(2)
	}
	client, cancel := dialEtcd(etcd)
	defer cancel()

	ctx, cancelCtx := context.WithTimeout(context.Background(), overallTimeout)
	defer cancelCtx()

	keys := clustering.NewKeys(root)
	assigns, liveNodes, nonActive, err := loadCluster(ctx, client, keys)
	if err != nil {
		die(err)
	}
	if len(liveNodes) == 0 {
		die(errors.New("no live nodes registered under /cex/counter/nodes/"))
	}

	plan := computePlan(assigns, liveNodes)
	printPlan(os.Stdout, plan, liveNodes, nonActive)

	if !execute {
		return
	}
	if len(plan) == 0 {
		return
	}

	for i, p := range plan {
		stepCtx, stepCancel := context.WithTimeout(ctx, stepTimeout)
		err := executeOne(stepCtx, client, keys, p)
		stepCancel()
		if err != nil {
			die(fmt.Errorf("step %d/%d vshard-%03d: %w", i+1, len(plan), p.VShardID, err))
		}
		fmt.Fprintf(os.Stdout, "ok %d/%d: vshard-%03d %s → %s (epoch %d → %d)\n",
			i+1, len(plan), p.VShardID, p.From, p.To, p.Epoch, p.Epoch+1)
	}
	fmt.Fprintf(os.Stdout, "rebalance complete: %d migrations applied\n", len(plan))
}

// loadCluster reads both /nodes/ and /assignment/ once. Returns ACTIVE
// assignments separately from non-ACTIVE ones so the plan can print a
// warning block for anything already mid-migration.
func loadCluster(ctx context.Context, client *clientv3.Client, keys *clustering.Keys) (
	active []clustering.Assignment,
	liveNodes []string,
	nonActive []clustering.Assignment,
	err error,
) {
	aResp, err := client.Get(ctx, keys.AssignmentsPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list assignments: %w", err)
	}
	for _, kv := range aResp.Kvs {
		var a clustering.Assignment
		if err := json.Unmarshal(kv.Value, &a); err != nil {
			continue
		}
		if a.State == clustering.StateActive {
			active = append(active, a)
		} else {
			nonActive = append(nonActive, a)
		}
	}

	nResp, err := client.Get(ctx, keys.NodesPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list nodes: %w", err)
	}
	seen := map[string]bool{}
	for _, kv := range nResp.Kvs {
		var n clustering.Node
		if err := json.Unmarshal(kv.Value, &n); err != nil {
			continue
		}
		if !seen[n.ID] {
			seen[n.ID] = true
			liveNodes = append(liveNodes, n.ID)
		}
	}
	sort.Strings(liveNodes)
	return
}

// computePlan compares the current ACTIVE assignments against the
// HRW-optimal distribution over liveNodes and returns the vshards
// whose owner should change. Non-ACTIVE assignments are assumed to
// be under the state machine's care already and are skipped.
func computePlan(active []clustering.Assignment, liveNodes []string) []planEntry {
	if len(active) == 0 || len(liveNodes) == 0 {
		return nil
	}
	vids := make([]clustering.VShardID, 0, len(active))
	for _, a := range active {
		vids = append(vids, a.VShardID)
	}
	desired := clustering.AssignVShards(vids, liveNodes)

	plan := make([]planEntry, 0)
	for _, a := range active {
		want, ok := desired[a.VShardID]
		if !ok || want == "" || want == a.Owner {
			continue
		}
		plan = append(plan, planEntry{
			VShardID: a.VShardID, From: a.Owner, To: want, Epoch: a.Epoch,
		})
	}
	sort.Slice(plan, func(i, j int) bool { return plan[i].VShardID < plan[j].VShardID })
	return plan
}

func printPlan(w io.Writer, plan []planEntry, liveNodes []string, nonActive []clustering.Assignment) {
	fmt.Fprintf(w, "live nodes: %s\n", strings.Join(liveNodes, ", "))
	if len(nonActive) > 0 {
		fmt.Fprintf(w, "warning: %d vshard(s) are mid-migration (not ACTIVE); they will be skipped:\n",
			len(nonActive))
		for _, a := range nonActive {
			fmt.Fprintf(w, "  vshard-%03d state=%s owner=%s target=%s epoch=%d\n",
				a.VShardID, a.State, a.Owner, a.Target, a.Epoch)
		}
	}

	if len(plan) == 0 {
		fmt.Fprintln(w, "no rebalance needed; cluster is already at HRW equilibrium")
		return
	}

	tw := tabwriter.NewWriter(w, 2, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "VSHARD\tFROM\tTO\tEPOCH")
	for _, p := range plan {
		fmt.Fprintf(tw, "vshard-%03d\t%s\t%s\t%d → %d\n",
			p.VShardID, p.From, p.To, p.Epoch, p.Epoch+1)
	}
	_ = tw.Flush()

	// Per-target count is useful for seeing at-a-glance load shifts.
	perTarget := map[string]int{}
	for _, p := range plan {
		perTarget[p.To]++
	}
	targets := make([]string, 0, len(perTarget))
	for k := range perTarget {
		targets = append(targets, k)
	}
	sort.Strings(targets)
	fmt.Fprintf(w, "\ntotal: %d migrations\n", len(plan))
	for _, t := range targets {
		fmt.Fprintf(w, "  %s ← %d\n", t, perTarget[t])
	}
}

// executeOne runs one vshard migration and blocks until it reaches
// ACTIVE@target,new_epoch or ctx expires. Relies on the coordinator
// running to flip HANDOFF_READY → ACTIVE — a rebalance without a
// coordinator present will time out on its first step.
func executeOne(ctx context.Context, client *clientv3.Client, keys *clustering.Keys, p planEntry) error {
	if err := clustering.StartMigration(ctx, client, keys, p.VShardID, p.To); err != nil {
		return fmt.Errorf("start migration: %w", err)
	}
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			a, err := clustering.ReadAssignment(ctx, client, keys, p.VShardID)
			if err != nil {
				return fmt.Errorf("poll: %w", err)
			}
			if a.State == clustering.StateActive && a.Owner == p.To {
				return nil
			}
		}
	}
}

// --------------------------------------------------------------------
// shared helpers
// --------------------------------------------------------------------

func dialEtcd(csv string) (*clientv3.Client, func()) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   splitCSV(csv),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		die(fmt.Errorf("etcd dial: %w", err))
	}
	return client, func() { _ = client.Close() }
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
