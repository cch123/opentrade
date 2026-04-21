// Command counter-migrate is the operator tool that kicks off a cold
// handoff for one vshard (ADR-0058 phase 6). It does a single CAS from
// ACTIVE(owner=A, ep=N) to MIGRATING(owner=A, ep=N, target=B). From
// there the state machine runs itself:
//
//   - The current owner's Manager sees WatchAssignedAssignments emit
//     a list missing this vshard (non-ACTIVE states drop out), stops
//     the worker, flushes its producer + snapshot + uploads, then
//     writes HANDOFF_READY via the shared helper.
//   - The elected coordinator's migration loop picks up HANDOFF_READY
//     and completes the swap to ACTIVE(owner=B, ep=N+1).
//   - The new owner's Manager sees the vshard appear in its ACTIVE
//     set and starts a fresh worker with the bumped epoch.
//
// This CLI does not block on completion — it's a fire-and-observe
// trigger. Use `etcdctl get /cex/counter/assignment/vshard-NNN` (or
// the forthcoming admin view) to watch progress.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/xargin/opentrade/counter/internal/clustering"
)

func main() {
	var (
		etcdCSV     string
		clusterRoot string
		vshardID    int
		target      string
		timeout     time.Duration
	)
	flag.StringVar(&etcdCSV, "etcd", "", "comma-separated etcd endpoints (required)")
	flag.StringVar(&clusterRoot, "cluster-root", clustering.DefaultRoot,
		"etcd root prefix for counter cluster data")
	flag.IntVar(&vshardID, "vshard", -1, "vshard id to migrate (0..vshard_count-1, required)")
	flag.StringVar(&target, "target", "", "target node id (required)")
	flag.DurationVar(&timeout, "timeout", 10*time.Second, "overall timeout for the CAS + verify")
	flag.Parse()

	if etcdCSV == "" || vshardID < 0 || target == "" {
		fmt.Fprintln(os.Stderr, "usage: counter-migrate --etcd=... --vshard=N --target=node-id [--cluster-root=/cex/counter]")
		os.Exit(2)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   splitCSV(etcdCSV),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		die(fmt.Errorf("etcd dial: %w", err))
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	keys := clustering.NewKeys(clusterRoot)
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
		die(fmt.Errorf("read assignment after CAS: %w", err))
	}
	fmt.Fprintf(os.Stdout,
		"MIGRATING: vshard-%03d owner=%s epoch=%d target=%s\n",
		after.VShardID, after.Owner, after.Epoch, after.Target)
	fmt.Fprintln(os.Stdout,
		"state machine will progress asynchronously: owner flushes -> HANDOFF_READY -> coordinator -> ACTIVE@target,epoch+1")
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
