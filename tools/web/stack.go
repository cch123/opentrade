package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// stack.go drives the whole dev environment: docker-compose deps, then the Go
// services in dependency order. It is the orchestration the "Start All" button
// triggers. Long-running work happens in a background goroutine; the UI polls
// status while it proceeds.

// overall orchestration phases (string for direct JSON exposure).
const (
	phaseIdle     = "idle"
	phaseDeps     = "deps"     // bringing up + health-gating docker compose
	phaseBuilding = "building" // go build services
	phaseStarting = "starting" // launching + health-gating services
	phaseUp       = "up"       // ready to trade
	phaseStopping = "stopping"
	phaseError    = "error"
)

// per-service start health budgets. Counter does the most at boot (etcd node
// registration + coordinator election + per-vshard worker spawn), so it gets
// the longest leash.
const (
	counterStartTimeout = 75 * time.Second
	defaultStartTimeout = 40 * time.Second
	depsTimeout         = 180 * time.Second // kafka first boot is slow
	buildConcurrency    = 3
)

type depStatus struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Health string `json:"health"`
}

// StackStatus is the full snapshot the UI renders.
type StackStatus struct {
	Phase    string       `json:"phase"`
	Message  string       `json:"message"`
	Busy     bool         `json:"busy"`
	Deps     []depStatus  `json:"deps"`
	Services []procStatus `json:"services"`
}

// Stack coordinates the supervisor + docker compose. mu guards the
// orchestration fields; the supervisor has its own per-proc locking.
type Stack struct {
	sup      *Supervisor
	repoRoot string
	composer []string // docker compose invocation prefix, e.g. ["docker","compose","-f",file]

	mu        sync.Mutex
	phase     string
	message   string
	busy      bool
	enabled   map[string]bool // last-requested optional-service toggles
	runCancel context.CancelFunc
	depsLog   *procLog
}

func newStack(sup *Supervisor, repoRoot string) *Stack {
	return &Stack{
		sup:      sup,
		repoRoot: repoRoot,
		composer: []string{"docker", "compose", "-f", composeFile},
		phase:    phaseIdle,
		enabled:  map[string]bool{},
		depsLog:  newProcLog(sup.logDir + "/docker.log"),
	}
}

func (st *Stack) setPhase(phase, msg string) {
	st.mu.Lock()
	st.phase = phase
	st.message = msg
	st.mu.Unlock()
}

// Up kicks off orchestration in the background. optIn names the optional
// services (asset, trigger) to include. Returns an error only if a run is
// already in flight.
func (st *Stack) Up(optIn map[string]bool) error {
	st.mu.Lock()
	if st.busy {
		st.mu.Unlock()
		return fmt.Errorf("stack operation already in progress (%s)", st.phase)
	}
	st.busy = true
	st.enabled = optIn
	ctx, cancel := context.WithCancel(context.Background())
	st.runCancel = cancel
	st.mu.Unlock()

	go func() {
		defer func() {
			st.mu.Lock()
			st.busy = false
			st.mu.Unlock()
		}()
		st.doUp(ctx, optIn)
	}()
	return nil
}

// enabledSpecs returns the services to run: all always-on plus opted-in
// optionals, preserving spec (tier/UI) order.
func (st *Stack) enabledSpecs(optIn map[string]bool) []serviceSpec {
	var out []serviceSpec
	for _, spec := range stackServices() {
		if spec.Optional && !optIn[spec.Name] {
			continue
		}
		out = append(out, spec)
	}
	return out
}

func (st *Stack) doUp(ctx context.Context, optIn map[string]bool) {
	// 1. docker compose deps.
	st.setPhase(phaseDeps, "starting docker dependencies")
	if err := st.composeUp(ctx); err != nil {
		st.setPhase(phaseError, "docker compose up failed: "+err.Error())
		return
	}
	st.setPhase(phaseDeps, "waiting for kafka / etcd / mysql to be healthy")
	if err := st.waitDeps(ctx); err != nil {
		st.setPhase(phaseError, err.Error())
		return
	}

	// The compose volume only grants the opentrade user its MYSQL_DATABASE
	// (opentrade); the asset service's opentrade_asset DB + grants + schema
	// aren't provisioned unless the mysql-init scripts happened to run on a
	// fresh volume. Ensure them idempotently so "Start All" works on any
	// pre-existing volume.
	st.setPhase(phaseDeps, "ensuring mysql databases, grants and schema")
	if err := st.ensureMySQL(ctx); err != nil {
		st.setPhase(phaseError, err.Error())
		return
	}

	// counter-journal + trade-event must exist at the vshard partition width
	// before counter/match produce to them (counter produces journal to
	// partition = vshard, and a missing partition panics the txn producer).
	// The compose kafka-init one-shot is meant to do this but is flaky on a
	// fresh volume, so the launcher guarantees it directly.
	st.setPhase(phaseDeps, "ensuring kafka topics")
	if err := st.ensureKafkaTopics(ctx); err != nil {
		st.setPhase(phaseError, err.Error())
		return
	}

	specs := st.enabledSpecs(optIn)

	// 2. build everything (bounded parallelism).
	st.setPhase(phaseBuilding, "compiling services")
	if err := st.buildAll(ctx, specs); err != nil {
		st.setPhase(phaseError, err.Error())
		return
	}

	// 3. start tier by tier; gate health before moving to the next tier
	//    because higher tiers dial lower ones at boot.
	st.setPhase(phaseStarting, "launching services")
	for _, tier := range tiersOf(specs) {
		if err := st.startTier(ctx, tier); err != nil {
			st.setPhase(phaseError, err.Error())
			return
		}
	}
	st.setPhase(phaseUp, "stack ready — fund an account and trade")
}

// composeUp runs `docker compose up -d`, teeing output to the docker log.
func (st *Stack) composeUp(ctx context.Context) error {
	args := append(append([]string{}, st.composer[1:]...), "up", "-d", "--remove-orphans")
	cmd := exec.CommandContext(ctx, st.composer[0], args...)
	cmd.Dir = st.repoRoot
	cmd.Stdout = st.depsLog
	cmd.Stderr = st.depsLog
	fmt.Fprintf(st.depsLog, "==> %s %s\n", st.composer[0], strings.Join(args, " "))
	return cmd.Run()
}

// ensureMySQL idempotently provisions both logical databases, grants the
// opentrade user access to them, and (re)applies both schema files. Every
// statement is idempotent (CREATE ... IF NOT EXISTS / GRANT), so it's safe to
// run on every "Start All".
func (st *Stack) ensureMySQL(ctx context.Context) error {
	bootstrap := strings.Join([]string{
		"CREATE DATABASE IF NOT EXISTS opentrade CHARACTER SET utf8mb4;",
		"CREATE DATABASE IF NOT EXISTS opentrade_asset CHARACTER SET utf8mb4;",
		"GRANT ALL PRIVILEGES ON opentrade.* TO 'opentrade'@'%';",
		"GRANT ALL PRIVILEGES ON opentrade_asset.* TO 'opentrade'@'%';",
		"FLUSH PRIVILEGES;",
	}, " ")
	if err := st.mysqlExec(ctx, "", bootstrap, ""); err != nil {
		return fmt.Errorf("mysql bootstrap (db + grants): %w", err)
	}
	// 01-schema.sql has no USE/CREATE DATABASE — apply it against opentrade.
	if err := st.mysqlExec(ctx, "opentrade", "", "deploy/docker/mysql-init/01-schema.sql"); err != nil {
		return fmt.Errorf("apply 01-schema.sql: %w", err)
	}
	// 02-asset-schema.sql carries its own USE opentrade_asset.
	if err := st.mysqlExec(ctx, "", "", "deploy/docker/mysql-init/02-asset-schema.sql"); err != nil {
		return fmt.Errorf("apply 02-asset-schema.sql: %w", err)
	}
	return nil
}

// mysqlExec runs SQL inside the opentrade-mysql container as root, either
// inline (sql) or from a repo-relative file piped to stdin (file). db sets the
// default database when non-empty.
func (st *Stack) mysqlExec(ctx context.Context, db, sql, file string) error {
	args := []string{"exec", "-i", "opentrade-mysql", "mysql", "-uroot", "-proot"}
	if db != "" {
		args = append(args, db)
	}
	if sql != "" {
		args = append(args, "-e", sql)
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = st.repoRoot
	cmd.Stdout = st.depsLog
	cmd.Stderr = st.depsLog
	if file != "" {
		f, err := os.Open(filepath.Join(st.repoRoot, file))
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		cmd.Stdin = f
	}
	fmt.Fprintf(st.depsLog, "==> mysql exec (db=%q) %s%s\n", db, sql, file)
	return cmd.Run()
}

// kafkaTopicPartitions is the width counter-journal + trade-event are created
// at — matches deploy/docker/scripts/init-kafka-topics.sh and the production
// vshard count. Must be >= --vshard-count so partition = vshard always exists.
const kafkaTopicPartitions = 256

// ensureKafkaTopics provisions every topic counter/match produce to before
// they start, idempotently — the transactional producer does NOT auto-create
// topics, so a missing topic/partition panics it. This removes the dependency
// on the flaky kafka-init compose one-shot.
//
// Two shapes:
//   - counter-journal + trade-event are vshard-partitioned (counter produces
//     journal to partition=vshard; match produces trade-event to
//     partition=vshard), so they need >= --vshard-count partitions.
//   - order-event-<symbol> (ADR-0050) is sticky-partitioned by symbol, so one
//     partition carries each symbol; it just has to exist.
func (st *Stack) ensureKafkaTopics(ctx context.Context) error {
	for _, t := range []string{"counter-journal", "trade-event"} {
		n := st.kafkaPartitionCount(ctx, t)
		if n >= vshardCount {
			continue // already wide enough (kafka-init may have won the race)
		}
		var err error
		if n == 0 {
			err = st.kafkaExec(ctx, "--create", "--topic", t,
				"--partitions", itoa(kafkaTopicPartitions), "--replication-factor", "1")
		} else {
			err = st.kafkaExec(ctx, "--alter", "--topic", t,
				"--partitions", itoa(kafkaTopicPartitions))
		}
		// Tolerate a lost race with kafka-init (topic already created): only
		// fail if it's still too narrow after the attempt.
		if err != nil && st.kafkaPartitionCount(ctx, t) < vshardCount {
			return fmt.Errorf("ensure kafka topic %s: %w", t, err)
		}
	}
	for _, sym := range []string{defaultSymbol} {
		t := "order-event-" + sym
		if st.kafkaPartitionCount(ctx, t) >= 1 {
			continue
		}
		err := st.kafkaExec(ctx, "--create", "--topic", t,
			"--partitions", "4", "--replication-factor", "1")
		if err != nil && st.kafkaPartitionCount(ctx, t) < 1 {
			return fmt.Errorf("ensure order-event topic %s: %w", t, err)
		}
	}
	return nil
}

// kafkaPartitionCount returns topic's partition count, or 0 if it's missing.
func (st *Stack) kafkaPartitionCount(ctx context.Context, topic string) int {
	args := []string{"exec", "opentrade-kafka", "kafka-topics.sh",
		"--bootstrap-server", "localhost:9092", "--describe", "--topic", topic}
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return 0 // topic missing → describe exits non-zero
	}
	// Each per-partition detail line contains "Partition: "; the summary line
	// has "PartitionCount: " (no "Partition: " substring), so this counts only
	// the detail lines = the partition count.
	return strings.Count(string(out), "Partition: ")
}

// kafkaExec runs kafka-topics.sh inside the broker container.
func (st *Stack) kafkaExec(ctx context.Context, topicArgs ...string) error {
	args := append([]string{"exec", "opentrade-kafka", "kafka-topics.sh",
		"--bootstrap-server", "localhost:9092"}, topicArgs...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = st.repoRoot
	cmd.Stdout = st.depsLog
	cmd.Stderr = st.depsLog
	fmt.Fprintf(st.depsLog, "==> kafka-topics.sh %s\n", strings.Join(topicArgs, " "))
	return cmd.Run()
}

// waitDeps blocks until kafka/etcd/mysql/minio report healthy AND kafka-init
// has finished provisioning topic partitions (it exits 0). Counter/match rely
// on the 256-partition trade-event + counter-journal topics kafka-init
// creates, so starting them before kafka-init completes would bind to
// auto-created 4-partition topics and break vshard→partition routing.
func (st *Stack) waitDeps(ctx context.Context) error {
	deadline := time.Now().Add(depsTimeout)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		ps, err := st.composePS(ctx)
		if err == nil {
			healthy := map[string]bool{}
			for _, c := range ps {
				if c.Health == "healthy" || (c.Health == "" && c.State == "running") {
					healthy[c.Service] = true
				}
			}
			initDone := true
			for _, c := range ps {
				if c.Service == "kafka-init" && c.State != "exited" {
					initDone = false
				}
			}
			allDeps := true
			for _, d := range dockerDeps {
				if !healthy[d] {
					allDeps = false
				}
			}
			if allDeps && initDone {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("docker dependencies not healthy within %s (see logs/docker.log)", depsTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

type composePS struct {
	Name     string `json:"Name"`
	Service  string `json:"Service"`
	State    string `json:"State"`
	Health   string `json:"Health"`
	ExitCode int    `json:"ExitCode"`
}

// composePS parses `docker compose ps --format json`, tolerating both the
// NDJSON (one object per line, compose v2) and JSON-array encodings.
func (st *Stack) composePS(ctx context.Context) ([]composePS, error) {
	args := append(append([]string{}, st.composer[1:]...), "ps", "-a", "--format", "json")
	out, err := exec.CommandContext(ctx, st.composer[0], args...).Output()
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(string(out))
	if trimmed == "" {
		return nil, nil
	}
	if strings.HasPrefix(trimmed, "[") {
		var arr []composePS
		if err := json.Unmarshal([]byte(trimmed), &arr); err != nil {
			return nil, err
		}
		return arr, nil
	}
	var res []composePS
	for _, line := range strings.Split(trimmed, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var c composePS
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, nil
}

// buildAll compiles every enabled service, bounded to buildConcurrency. The
// first failure cancels the rest.
func (st *Stack) buildAll(ctx context.Context, specs []serviceSpec) error {
	sem := make(chan struct{}, buildConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	buildCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, spec := range specs {
		spec := spec
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-buildCtx.Done():
				return
			}
			defer func() { <-sem }()
			if err := st.sup.build(buildCtx, spec.Name); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// startTier launches every service in one tier, then waits for each to pass
// its health gate (concurrently). Any failure aborts the whole start.
func (st *Stack) startTier(ctx context.Context, tier []serviceSpec) error {
	for _, spec := range tier {
		if err := st.sup.start(ctx, spec.Name); err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, spec := range tier {
		spec := spec
		timeout := defaultStartTimeout
		if spec.Name == "counter" {
			timeout = counterStartTimeout
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := st.sup.waitHealthy(ctx, spec.Name, timeout); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// tiersOf groups specs into ascending tier batches, preserving order within a
// tier.
func tiersOf(specs []serviceSpec) [][]serviceSpec {
	byTier := map[int][]serviceSpec{}
	var tiers []int
	for _, spec := range specs {
		if _, ok := byTier[spec.Tier]; !ok {
			tiers = append(tiers, spec.Tier)
		}
		byTier[spec.Tier] = append(byTier[spec.Tier], spec)
	}
	sort.Ints(tiers)
	out := make([][]serviceSpec, 0, len(tiers))
	for _, t := range tiers {
		out = append(out, byTier[t])
	}
	return out
}

// Down stops all services (and optionally the docker deps). Runs synchronously
// — teardown is fast — but guards against overlapping with an in-flight Up.
func (st *Stack) Down(alsoDeps bool) {
	st.mu.Lock()
	if st.runCancel != nil {
		st.runCancel() // cancel any in-flight Up
	}
	st.busy = true
	st.mu.Unlock()

	st.setPhase(phaseStopping, "stopping services")
	st.sup.stopAll()

	if alsoDeps {
		st.setPhase(phaseStopping, "stopping docker dependencies")
		args := append(append([]string{}, st.composer[1:]...), "stop")
		cmd := exec.Command(st.composer[0], args...)
		cmd.Dir = st.repoRoot
		cmd.Stdout = st.depsLog
		cmd.Stderr = st.depsLog
		_ = cmd.Run()
	}

	st.mu.Lock()
	st.busy = false
	st.phase = phaseIdle
	st.message = "stopped"
	st.mu.Unlock()
}

// status assembles the full UI snapshot. Deps are queried live from compose.
func (st *Stack) status(ctx context.Context) StackStatus {
	st.mu.Lock()
	phase, msg, busy := st.phase, st.message, st.busy
	st.mu.Unlock()

	deps := st.depSnapshot(ctx)
	return StackStatus{
		Phase:    phase,
		Message:  msg,
		Busy:     busy,
		Deps:     deps,
		Services: st.sup.status(),
	}
}

// depSnapshot maps the docker dep containers into UI rows. Best-effort: if
// compose isn't reachable yet, every dep shows "down".
func (st *Stack) depSnapshot(ctx context.Context) []depStatus {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	live := map[string]composePS{}
	if ps, err := st.composePS(ctx); err == nil {
		for _, c := range ps {
			live[c.Service] = c
		}
	}
	out := make([]depStatus, 0, len(dockerDeps))
	for _, d := range dockerDeps {
		ds := depStatus{Name: d, State: "down"}
		if c, ok := live[d]; ok {
			ds.State = c.State
			ds.Health = c.Health
		}
		out = append(out, ds)
	}
	return out
}

// depsLogTail exposes the docker compose output for the UI log viewer.
func (st *Stack) depsLogTail(n int) []string { return st.depsLog.tail(n) }
