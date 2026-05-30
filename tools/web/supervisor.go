package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// supervisor.go is a tiny local process manager: it builds and runs each Go
// service as a child process, captures its output, kills it (and its whole
// process group) on demand, and reports liveness/health. A browser can't
// spawn OS processes, so this Go binary is the control plane the web page
// drives.

// process lifecycle states (string-typed for direct JSON exposure).
const (
	stStopped  = "stopped"  // never started, or fully torn down
	stBuilding = "building" // go build in flight
	stStarting = "starting" // exec'd, not yet health-gated
	stRunning  = "running"  // up; health gate passed (or alive-settled)
	stExited   = "exited"   // process ended on its own (unexpected)
	stFailed   = "failed"   // build failed, or exec/health error
)

const (
	logRingLines = 400             // recent log lines kept in memory per service
	aliveSettle  = 1500 * time.Millisecond // healthAlive: up-this-long ⇒ ready
)

// procLog is a concurrency-safe, line-oriented ring buffer that also tees to a
// log file. exec wires stdout AND stderr to the same procLog, so Write can be
// called from two goroutines at once — every method holds mu.
type procLog struct {
	mu      sync.Mutex
	lines   []string
	partial []byte // bytes of an unterminated trailing line
	file    *os.File
}

func newProcLog(path string) *procLog {
	pl := &procLog{}
	if f, err := os.Create(path); err == nil {
		pl.file = f
	}
	return pl
}

func (pl *procLog) Write(b []byte) (int, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if pl.file != nil {
		_, _ = pl.file.Write(b)
	}
	pl.partial = append(pl.partial, b...)
	for {
		i := indexByte(pl.partial, '\n')
		if i < 0 {
			break
		}
		line := string(pl.partial[:i])
		pl.partial = pl.partial[i+1:]
		pl.lines = append(pl.lines, line)
		if len(pl.lines) > logRingLines {
			pl.lines = pl.lines[len(pl.lines)-logRingLines:]
		}
	}
	return len(b), nil
}

// tail returns up to n most-recent lines (including the current partial line).
func (pl *procLog) tail(n int) []string {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	out := make([]string, 0, len(pl.lines)+1)
	out = append(out, pl.lines...)
	if len(pl.partial) > 0 {
		out = append(out, string(pl.partial))
	}
	if n > 0 && len(out) > n {
		out = out[len(out)-n:]
	}
	return out
}

func (pl *procLog) close() {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if pl.file != nil {
		_ = pl.file.Close()
		pl.file = nil
	}
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// managedProc is one supervised service. All mutable fields are guarded by mu.
type managedProc struct {
	spec serviceSpec

	mu        sync.Mutex
	state     string
	cmd       *exec.Cmd
	pid       int
	startedAt time.Time
	exitNote  string // last error / exit detail surfaced to the UI
	stopping  bool   // set before an intentional kill so the waiter stays quiet
	log       *procLog
}

func (p *managedProc) setState(s, note string) {
	p.mu.Lock()
	p.state = s
	if note != "" {
		p.exitNote = note
	}
	p.mu.Unlock()
}

// procStatus is the JSON-friendly snapshot the API and UI consume.
type procStatus struct {
	Name     string `json:"name"`
	Desc     string `json:"desc"`
	Optional bool   `json:"optional"`
	State    string `json:"state"`
	PID      int    `json:"pid,omitempty"`
	UptimeMS int64  `json:"uptime_ms,omitempty"`
	Note     string `json:"note,omitempty"`
}

func (p *managedProc) snapshot() procStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := procStatus{
		Name:     p.spec.Name,
		Desc:     p.spec.Desc,
		Optional: p.spec.Optional,
		State:    p.state,
		PID:      p.pid,
		Note:     p.exitNote,
	}
	if p.state == stRunning && !p.startedAt.IsZero() {
		st.UptimeMS = time.Since(p.startedAt).Milliseconds()
	}
	return st
}

// Supervisor owns every managed process and the shared run context.
type Supervisor struct {
	repoRoot string
	binDir   string
	logDir   string
	goBin    string

	procs map[string]*managedProc // keyed by spec.Name; built once in newSupervisor
}

func newSupervisor(repoRoot string) (*Supervisor, error) {
	binDir := filepath.Join(repoRoot, "bin")
	logDir := filepath.Join(repoRoot, "logs")
	for _, d := range []string{binDir, logDir,
		filepath.Join(repoRoot, "data", "counter"),
		filepath.Join(repoRoot, "data", "match"),
		filepath.Join(repoRoot, "data", "trade-dump"),
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", d, err)
		}
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		return nil, fmt.Errorf("`go` not found on PATH: %w", err)
	}
	s := &Supervisor{
		repoRoot: repoRoot,
		binDir:   binDir,
		logDir:   logDir,
		goBin:    goBin,
		procs:    map[string]*managedProc{},
	}
	for _, spec := range stackServices() {
		s.procs[spec.Name] = &managedProc{spec: spec, state: stStopped}
	}
	return s, nil
}

func (s *Supervisor) proc(name string) *managedProc { return s.procs[name] }

// build compiles one service into ./bin/<name>. Build output is captured into
// the service's own log ring so failures are visible in the UI.
func (s *Supervisor) build(ctx context.Context, name string) error {
	p := s.procs[name]
	if p == nil {
		return fmt.Errorf("unknown service %q", name)
	}
	p.mu.Lock()
	p.log = newProcLog(filepath.Join(s.logDir, name+".log"))
	p.state = stBuilding
	p.exitNote = ""
	logw := p.log
	p.mu.Unlock()

	out := filepath.Join(s.binDir, name)
	cmd := exec.CommandContext(ctx, s.goBin, "build", "-o", out, p.spec.BuildPath)
	cmd.Dir = s.repoRoot
	cmd.Stdout = logw
	cmd.Stderr = logw
	fmt.Fprintf(logw, "==> go build -o bin/%s %s\n", name, p.spec.BuildPath)
	if err := cmd.Run(); err != nil {
		p.setState(stFailed, "build failed: "+err.Error())
		return fmt.Errorf("build %s: %w", name, err)
	}
	// Built, not yet started. Back to "stopped" so a service whose tier never
	// runs (because an earlier tier failed) doesn't read as forever-building.
	p.setState(stStopped, "")
	return nil
}

// start execs the prebuilt binary in its own process group and spawns a waiter
// that records the exit. Returns once the child is launched (not yet healthy).
func (s *Supervisor) start(ctx context.Context, name string) error {
	p := s.procs[name]
	if p == nil {
		return fmt.Errorf("unknown service %q", name)
	}
	p.mu.Lock()
	if p.state == stRunning || p.state == stStarting {
		p.mu.Unlock()
		return nil // already up — idempotent
	}
	if p.log == nil {
		p.log = newProcLog(filepath.Join(s.logDir, name+".log"))
	}
	logw := p.log
	p.mu.Unlock()

	bin := filepath.Join(s.binDir, name)
	cmd := exec.Command(bin, p.spec.Args...)
	cmd.Dir = s.repoRoot
	cmd.Stdout = logw
	cmd.Stderr = logw
	// Own process group so we can SIGTERM the child and any grandchildren in
	// one syscall, and so Ctrl-C on the launcher doesn't race-signal children.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	fmt.Fprintf(logw, "==> bin/%s %s\n", name, strings.Join(p.spec.Args, " "))
	if err := cmd.Start(); err != nil {
		p.setState(stFailed, "exec failed: "+err.Error())
		return fmt.Errorf("start %s: %w", name, err)
	}

	p.mu.Lock()
	p.cmd = cmd
	p.pid = cmd.Process.Pid
	p.state = stStarting
	p.startedAt = time.Now()
	p.stopping = false
	p.exitNote = ""
	p.mu.Unlock()

	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		defer p.mu.Unlock()
		p.pid = 0
		if p.stopping {
			p.state = stStopped
			return
		}
		// Unexpected exit (crash or fatal config error). Surface the tail so
		// the UI can show why without opening the log file.
		note := "exited"
		if err != nil {
			note = "exited: " + err.Error()
		}
		p.state = stExited
		p.exitNote = note
	}()
	return nil
}

// waitHealthy blocks until the service passes its health gate, the process
// dies, or the deadline elapses.
func (s *Supervisor) waitHealthy(ctx context.Context, name string, timeout time.Duration) error {
	p := s.procs[name]
	if p == nil {
		return fmt.Errorf("unknown service %q", name)
	}
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		p.mu.Lock()
		state := p.state
		startedAt := p.startedAt
		note := p.exitNote
		p.mu.Unlock()

		if state == stExited || state == stFailed {
			return fmt.Errorf("%s %s (%s)", name, state, note)
		}
		if s.probe(p.spec, state, startedAt) {
			p.setState(stRunning, "")
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not healthy within %s", name, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// probe runs the spec's readiness check. Pure (no state mutation) so
// waitHealthy owns the state transition.
func (s *Supervisor) probe(spec serviceSpec, state string, startedAt time.Time) bool {
	if state != stStarting && state != stRunning {
		return false
	}
	switch spec.Health {
	case healthAlive:
		return !startedAt.IsZero() && time.Since(startedAt) >= aliveSettle
	case healthTCP:
		conn, err := net.DialTimeout("tcp", "localhost:"+strconv.Itoa(spec.HealthPort), 600*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	case healthHTTP:
		cl := &http.Client{Timeout: 800 * time.Millisecond}
		resp, err := cl.Get(fmt.Sprintf("http://localhost:%d%s", spec.HealthPort, spec.HealthPath))
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode >= 200 && resp.StatusCode < 300
	}
	return false
}

// stop signals the process group and waits briefly for it to exit, escalating
// to SIGKILL. Safe to call on an already-stopped service.
func (s *Supervisor) stop(name string) {
	p := s.procs[name]
	if p == nil {
		return
	}
	p.mu.Lock()
	cmd := p.cmd
	pid := p.pid
	if cmd == nil || pid == 0 {
		p.state = stStopped
		p.mu.Unlock()
		return
	}
	p.stopping = true
	p.mu.Unlock()

	// Negative pid → signal the whole process group (Setpgid above).
	_ = syscall.Kill(-pid, syscall.SIGTERM)

	done := make(chan struct{})
	go func() { _, _ = cmd.Process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
	p.setState(stStopped, "")
}

// stopAll tears down every running service in reverse tier order (bff first,
// counter last) so dependents drop before their dependencies.
func (s *Supervisor) stopAll() {
	specs := stackServices()
	for i := len(specs) - 1; i >= 0; i-- {
		s.stop(specs[i].Name)
	}
}

// status returns a snapshot of every managed process, in spec (UI) order.
func (s *Supervisor) status() []procStatus {
	specs := stackServices()
	out := make([]procStatus, 0, len(specs))
	for _, spec := range specs {
		if p := s.procs[spec.Name]; p != nil {
			out = append(out, p.snapshot())
		}
	}
	return out
}

// logs returns the recent log tail for one service.
func (s *Supervisor) logs(name string, tail int) ([]string, error) {
	p := s.procs[name]
	if p == nil {
		return nil, fmt.Errorf("unknown service %q", name)
	}
	p.mu.Lock()
	pl := p.log
	p.mu.Unlock()
	if pl == nil {
		return []string{}, nil
	}
	return pl.tail(tail), nil
}

// closeLogs flushes all open log files (called on launcher shutdown).
func (s *Supervisor) closeLogs() {
	for _, p := range s.procs {
		p.mu.Lock()
		pl := p.log
		p.mu.Unlock()
		if pl != nil {
			pl.close()
		}
	}
}

// repoRootFromCWD resolves the repository root via `git rev-parse`, falling
// back to the current directory.
func repoRootFromCWD() string {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err == nil {
		if root := strings.TrimSpace(string(out)); root != "" {
			return root
		}
	}
	wd, _ := os.Getwd()
	return wd
}

// itoa is a tiny strconv.Itoa alias used by services.go for flag building.
func itoa(i int) string { return strconv.Itoa(i) }
