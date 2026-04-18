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
//     already enforced by ComputeTransfer + the journal seq_id guard).
//   - ShardSeq on every output = max(input ShardSeqs). Safe: trade-dump's
//     accounts projection keys by (shard_id, user, asset) so a fresh new
//     shard_id starts its own seq space anyway; taking the max just avoids
//     collisions with any tooling that cared about "max seq ever seen".
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xargin/opentrade/counter/internal/snapshot"
	"github.com/xargin/opentrade/pkg/shard"
)

type config struct {
	inDir    string
	outDir   string
	fromN    int
	toM      int
	dryRun   bool
	timeUnix int64
}

func parseFlags() config {
	cfg := config{
		timeUnix: time.Now().UnixMilli(),
	}
	flag.StringVar(&cfg.inDir, "in", "", "directory holding shard-*.json snapshots from the OLD topology")
	flag.StringVar(&cfg.outDir, "out", "", "directory to write NEW snapshots into (created if missing)")
	flag.IntVar(&cfg.fromN, "from", 0, "OLD total-shards count (number of input files)")
	flag.IntVar(&cfg.toM, "to", 0, "NEW total-shards count (number of output files)")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "compute + report routing without writing output")
	flag.Parse()
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

	inputs, err := loadInputs(cfg.inDir, cfg.fromN)
	if err != nil {
		die(fmt.Errorf("load inputs: %w", err))
	}

	outputs, report := reshard(inputs, cfg.toM, cfg.timeUnix)

	fmt.Fprintf(os.Stderr, "reshard: %d users, %d orders, dropped %d dedup entries\n",
		report.Users, report.Orders, report.DroppedDedup)
	fmt.Fprintf(os.Stderr, "        input shard_seq max=%d → applied to all %d outputs\n",
		report.MaxShardSeq, cfg.toM)
	for _, o := range outputs {
		fmt.Fprintf(os.Stderr, "  new shard %d: %d accounts, %d orders\n",
			o.ShardID, len(o.Accounts), len(o.Orders))
	}

	if cfg.dryRun {
		fmt.Fprintln(os.Stderr, "dry-run: no files written")
		return
	}
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		die(err)
	}
	for _, o := range outputs {
		path := filepath.Join(cfg.outDir, fmt.Sprintf("shard-%d.json", o.ShardID))
		if err := snapshot.Save(path, o); err != nil {
			die(fmt.Errorf("save %s: %w", path, err))
		}
	}
}

// -----------------------------------------------------------------------------
// Pure logic (unit-tested)
// -----------------------------------------------------------------------------

// Report summarizes the migration for logging.
type Report struct {
	Users        int
	Orders       int
	DroppedDedup int
	MaxShardSeq  uint64
}

// reshard takes inputs (one per OLD shard) and produces `toM` outputs routed
// by the new topology's hash function. Dedup entries are dropped. Deterministic.
func reshard(inputs []*snapshot.ShardSnapshot, toM int, nowMs int64) ([]*snapshot.ShardSnapshot, Report) {
	outputs := make([]*snapshot.ShardSnapshot, toM)
	for i := 0; i < toM; i++ {
		outputs[i] = &snapshot.ShardSnapshot{
			Version:     snapshot.Version,
			ShardID:     i,
			TimestampMS: nowMs,
		}
	}
	var rep Report
	for _, in := range inputs {
		if in == nil {
			continue
		}
		if in.ShardSeq > rep.MaxShardSeq {
			rep.MaxShardSeq = in.ShardSeq
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
		o.ShardSeq = rep.MaxShardSeq
	}
	return outputs, rep
}

// -----------------------------------------------------------------------------
// I/O
// -----------------------------------------------------------------------------

// loadInputs reads every `shard-<i>.json` under dir for i in [0, n). Returns
// one snapshot per shard in shard-id order; missing files are treated as
// empty shards (we still produce outputs, just without contributions from
// the missing input — matches "that shard was empty").
func loadInputs(dir string, n int) ([]*snapshot.ShardSnapshot, error) {
	out := make([]*snapshot.ShardSnapshot, n)
	for i := 0; i < n; i++ {
		path := filepath.Join(dir, fmt.Sprintf("shard-%d.json", i))
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "warning: %s missing, treating as empty\n", path)
				continue
			}
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		var snap snapshot.ShardSnapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return nil, fmt.Errorf("decode %s: %w", path, err)
		}
		if snap.ShardID != i {
			return nil, fmt.Errorf("%s: shard_id=%d but file name expects %d", path, snap.ShardID, i)
		}
		out[i] = &snap
	}
	return out, nil
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "counter-reshard:", err)
	os.Exit(1)
}
