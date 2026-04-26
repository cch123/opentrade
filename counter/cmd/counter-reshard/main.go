// Command counter-reshard is the one-shot migration tool used when the
// Counter shard count changes (e.g. 10 → 20). It reads N input snapshots
// from an old topology and writes M output snapshots routed by the new
// topology's hash function (ADR-0010 / ADR-0027).
//
// Workflow (operator):
//
//  1. Stop every Counter shard cleanly. Each writes its final snapshot.
//  2. Copy the N snapshot files under one directory.
//  3. Run `counter-reshard -in old -out new -from N -to M`.
//  4. Start M shards with --snapshot-dir=new --total-shards=M.
//
// Scope:
//
//   - Accounts are routed by the new hash (pkg/shard.Index).
//   - Orders are routed by their UserID, same rule; order IDs are preserved
//     (snowflake global uniqueness, ADR-0015).
//   - Dedup entries are dropped — the table has no UserID field, so we can't
//     attribute them to the correct new shard. Operator is expected to run
//     this during a maintenance window; any in-flight Transfer must be
//     tolerant of "dedup miss → re-apply" (TransferRequest idempotency is
//     already enforced by ComputeTransfer + the journal counter_seq_id guard).
//   - CounterSeq on every output = max(input CounterSeqs). Safe: trade-dump's
//     accounts projection keys by (shard_id, user, asset) so a fresh new
//     shard_id starts its own seq space anyway; taking the max just avoids
//     collisions with any tooling that cared about "max counter_seq ever seen".
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/xargin/opentrade/pkg/shard"
	"github.com/xargin/opentrade/pkg/snapshot"
	countersnap "github.com/xargin/opentrade/trade-dump/snapshot/counter"
)

type config struct {
	inDir        string
	outDir       string
	fromN        int
	toM          int
	dryRun       bool
	outputFormat snapshot.Format
	timeUnix     int64
}

func parseFlags() config {
	cfg := config{
		timeUnix:     time.Now().UnixMilli(),
		outputFormat: snapshot.FormatProto, // ADR-0049 default
	}
	var outputFormatStr string
	flag.StringVar(&cfg.inDir, "in", "", "directory holding shard-* snapshots from the OLD topology (.pb or .json)")
	flag.StringVar(&cfg.outDir, "out", "", "directory to write NEW snapshots into (created if missing)")
	flag.IntVar(&cfg.fromN, "from", 0, "OLD total-shards count (number of input files)")
	flag.IntVar(&cfg.toM, "to", 0, "NEW total-shards count (number of output files)")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "compute + report routing without writing output")
	flag.StringVar(&outputFormatStr, "output-format", cfg.outputFormat.String(), "output encoding: proto (default) | json (ADR-0049)")
	flag.Parse()
	if outputFormatStr != "" {
		f, err := snapshot.ParseFormat(outputFormatStr)
		if err != nil {
			die(err)
		}
		cfg.outputFormat = f
	}
	return cfg
}

func (c *config) validate() error {
	if c.inDir == "" {
		return fmt.Errorf("-in is required")
	}
	if c.outDir == "" && !c.dryRun {
		return fmt.Errorf("-out is required unless --dry-run")
	}
	if c.fromN <= 0 {
		return fmt.Errorf("-from must be > 0")
	}
	if c.toM <= 0 {
		return fmt.Errorf("-to must be > 0")
	}
	return nil
}

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		die(err)
	}

	ctx := context.Background()
	inStore := snapshot.NewFSBlobStore(cfg.inDir)
	inputs, err := loadInputs(ctx, inStore, cfg.fromN)
	if err != nil {
		die(fmt.Errorf("load inputs: %w", err))
	}

	outputs, report := reshard(inputs, cfg.toM, cfg.timeUnix)

	fmt.Fprintf(os.Stderr, "reshard: %d users, %d orders, dropped %d dedup entries\n",
		report.Users, report.Orders, report.DroppedDedup)
	fmt.Fprintf(os.Stderr, "        input counter_seq max=%d → applied to all %d outputs\n",
		report.MaxCounterSeq, cfg.toM)
	for _, o := range outputs {
		fmt.Fprintf(os.Stderr, "  new shard %d: %d accounts, %d orders\n",
			o.ShardID, len(o.Accounts), len(o.Orders))
	}

	if cfg.dryRun {
		fmt.Fprintln(os.Stderr, "dry-run: no files written")
		return
	}
	outStore := snapshot.NewFSBlobStore(cfg.outDir)
	for _, o := range outputs {
		key := fmt.Sprintf("shard-%d", o.ShardID)
		if err := countersnap.Save(ctx, outStore, key, o, cfg.outputFormat); err != nil {
			die(fmt.Errorf("save %s: %w", key, err))
		}
	}
}

// -----------------------------------------------------------------------------
// Pure logic (unit-tested)
// -----------------------------------------------------------------------------

// Report summarizes the migration for logging.
type Report struct {
	Users         int
	Orders        int
	DroppedDedup  int
	MaxCounterSeq uint64
}

// reshard takes inputs (one per OLD shard) and produces `toM` outputs routed
// by the new topology's hash function. Dedup entries are dropped. Deterministic.
func reshard(inputs []*countersnap.ShardSnapshot, toM int, nowMs int64) ([]*countersnap.ShardSnapshot, Report) {
	outputs := make([]*countersnap.ShardSnapshot, toM)
	for i := 0; i < toM; i++ {
		outputs[i] = &countersnap.ShardSnapshot{
			Version:     countersnap.Version,
			ShardID:     i,
			TimestampMS: nowMs,
		}
	}
	var rep Report
	for _, in := range inputs {
		if in == nil {
			continue
		}
		if in.CounterSeq > rep.MaxCounterSeq {
			rep.MaxCounterSeq = in.CounterSeq
		}
		for _, acc := range in.Accounts {
			dst := shard.Index(acc.UserID, toM)
			outputs[dst].Accounts = append(outputs[dst].Accounts, acc)
			rep.Users++
		}
		for _, o := range in.Orders {
			dst := shard.Index(o.UserID, toM)
			outputs[dst].Orders = append(outputs[dst].Orders, o)
			rep.Orders++
		}
		rep.DroppedDedup += len(in.Dedup)
	}
	for _, o := range outputs {
		o.CounterSeq = rep.MaxCounterSeq
	}
	return outputs, rep
}

// -----------------------------------------------------------------------------
// I/O
// -----------------------------------------------------------------------------

// loadInputs reads every `shard-<i>` snapshot under store for i in [0, n).
// Probes the .pb form first, then .json (ADR-0049 migration window).
// Missing files are treated as empty shards — we still produce outputs,
// just without contributions from the missing input.
func loadInputs(ctx context.Context, store snapshot.BlobStore, n int) ([]*countersnap.ShardSnapshot, error) {
	out := make([]*countersnap.ShardSnapshot, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("shard-%d", i)
		snap, err := countersnap.Load(ctx, store, key)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				fmt.Fprintf(os.Stderr, "warning: %s(.pb|.json) missing, treating as empty\n", key)
				continue
			}
			return nil, fmt.Errorf("load %s: %w", key, err)
		}
		if snap.ShardID != i {
			return nil, fmt.Errorf("%s: shard_id=%d but file name expects %d", key, snap.ShardID, i)
		}
		out[i] = snap
	}
	return out, nil
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "counter-reshard:", err)
	os.Exit(1)
}
