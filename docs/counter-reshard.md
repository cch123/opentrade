# Counter re-shard runbook

`counter-reshard` is the one-shot migration tool used when we change the
Counter shard count (e.g. 10 → 20). It reads a full set of snapshot files
from the **old** topology and writes a new set routed by the **new**
topology's hash function (ADR-0010 / ADR-0027).

## When

- Expanding the shard count (typical: doubling)
- Consolidating shards (rare)
- Any time `--total-shards` on `counter` needs to change

This is strictly a **maintenance-window tool**. It doesn't migrate live
state; all Counter processes must be stopped first.

## Prerequisites

- Every Counter instance has landed its final snapshot (clean shutdown, or
  `--ha-mode=auto` relinquished leadership on SIGTERM)
- A single directory collecting the `shard-0.json ... shard-(N-1).json`
  files from the old cluster

## Steps

```sh
# 1. Stop all Counter shards (SIGTERM, wait for "primary stopped" log lines)

# 2. Collect snapshots into one directory
mkdir -p reshard-in reshard-out
for i in $(seq 0 9); do
  cp /var/lib/counter/shard-$i/shard-$i.json reshard-in/
done

# 3. Dry run — prints a routing report without writing
go run ./counter/cmd/counter-reshard -in reshard-in -from 10 -to 20 -dry-run

# 4. Actually write new snapshots
go run ./counter/cmd/counter-reshard -in reshard-in -out reshard-out -from 10 -to 20

# 5. Ship reshard-out/shard-<i>.json to each new Counter instance's
#    --snapshot-dir, start them with --total-shards=20 --shard-id=<i>
```

## What gets migrated

| Item | Behaviour |
|---|---|
| Account balances | Routed by `pkg/shard.Index(user_id, newN)` — one user's row lands on exactly one new shard |
| Orders | Same routing by `user_id`; order IDs kept (snowflake uniqueness, ADR-0015) |
| `ShardSeq` | All outputs get `max(input ShardSeqs)` so downstream tooling never sees a seq "go backwards" |
| Dedup | **Dropped** — the table has no user_id field, so we can't re-attribute entries. See below |

### Why dedup is dropped

`DedupEntry.Key` is the `transfer_id`; nothing in the value points to the
user that issued it. Walking the counter-journal to rebuild would be
possible but adds a hard dependency on Kafka retention covering the whole
dedup TTL (default 24h). Since we already require a maintenance window,
the simpler rule is: dedup starts empty on the new cluster.

**Operational impact**: a client that resubmitted the *same* `transfer_id`
during the brief window before we turned the new cluster back on could
land a duplicate. Mitigation: keep the migration window short
(seconds), and / or pause client traffic via a 503 upstream until the
new cluster is serving.

## Validation

After starting the new cluster, run the reconciler (ADR-0008 §对账):

```sh
counter --mysql-dsn=... --recon-interval=1m
```

The first reconcile pass should report `mismatches=0` (aside from fresh
deltas). If a user is on the wrong shard, the query for that user returns
nothing from MySQL → logs `only_in_memory`. Fix by re-running
`counter-reshard` with the right `--to` value and redeploying.

## Gotchas

- Input files **must** be named `shard-<i>.json` for `i` in `[0, fromN)`.
  Missing files are treated as empty shards — the tool warns but continues.
- Output dir is **overwritten**, not merged. Always use an empty dir.
- The tool does not restart Counter for you. Shipping the files + starting
  binaries is your job.

## Testing the tool

Unit tests cover the pure routing logic in
[counter/cmd/counter-reshard/main_test.go](../counter/cmd/counter-reshard/main_test.go).
For an end-to-end dry run against real data, `--dry-run` prints a per-new-shard
summary (accounts / orders placed) without writing anything.
