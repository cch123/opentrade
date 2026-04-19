// Package adminaudit writes append-only JSON-Lines (JSONL) audit records
// for admin-plane operations (ADR-0052).
//
// One file, one BFF process: concurrent callers serialise through a
// mutex + `O_APPEND` so rotated files never see interleaved lines. Each
// log call fsync's before returning so a crash after Log means "the
// admin operation did not happen" (the audit is always a strict
// prefix of what's been committed).
//
// Format (one JSON object per line):
//
//	{
//	  "ts_unix_ms": 1713500000000,
//	  "admin_id":   "ops-bot-1",
//	  "op":         "admin.symbol.put",
//	  "target":     "BTC-USDT",
//	  "params":     {...},
//	  "status":     "ok" | "failed",
//	  "error":      "...",
//	  "remote_ip":  "10.0.0.1",
//	  "request_id": "..."
//	}
package adminaudit

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Entry is a single audit record. Zero values for optional fields omit them
// from the encoded JSON.
type Entry struct {
	TsUnixMS  int64          `json:"ts_unix_ms"`
	AdminID   string         `json:"admin_id,omitempty"`
	Op        string         `json:"op"`
	Target    string         `json:"target,omitempty"`
	Params    map[string]any `json:"params,omitempty"`
	Status    string         `json:"status"` // "ok" / "failed"
	Error     string         `json:"error,omitempty"`
	RemoteIP  string         `json:"remote_ip,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
}

// StatusOK / StatusFailed are the canonical Entry.Status values.
const (
	StatusOK     = "ok"
	StatusFailed = "failed"
)

// Logger is the minimal interface handlers depend on. Production uses
// FileLogger; tests can supply a capture-in-memory impl.
type Logger interface {
	Log(Entry) error
	Close() error
}

// FileLogger appends entries to a JSONL file.
type FileLogger struct {
	mu    sync.Mutex
	f     *os.File
	clock func() time.Time
}

// Open appends to path (created with 0o600 if missing). Fails if the
// directory does not exist — callers should not silently create audit
// directories; that is an ops decision, not a BFF decision.
func Open(path string) (*FileLogger, error) {
	if path == "" {
		return nil, fmt.Errorf("adminaudit: path is required")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("adminaudit: open %s: %w", path, err)
	}
	return &FileLogger{f: f, clock: time.Now}, nil
}

// SetClock overrides time.Now (tests only).
func (l *FileLogger) SetClock(c func() time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clock = c
}

// Log appends e. TsUnixMS is filled from the clock when zero. Returns an
// error on partial write / fsync failure — callers should treat an audit
// failure as a blocking error (ADR-0052 §3).
func (l *FileLogger) Log(e Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return fmt.Errorf("adminaudit: logger already closed")
	}
	if e.TsUnixMS == 0 {
		e.TsUnixMS = l.clock().UnixMilli()
	}
	if e.Op == "" {
		return fmt.Errorf("adminaudit: entry.op is required")
	}
	if e.Status == "" {
		return fmt.Errorf("adminaudit: entry.status is required")
	}
	buf, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("adminaudit: marshal: %w", err)
	}
	buf = append(buf, '\n')
	if _, err := l.f.Write(buf); err != nil {
		return fmt.Errorf("adminaudit: write: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("adminaudit: fsync: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying file.
func (l *FileLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

// -----------------------------------------------------------------------------
// NopLogger / test helpers
// -----------------------------------------------------------------------------

// NopLogger discards every entry. Used by call sites that want the audit
// layer to be optional without nil checks.
type NopLogger struct{}

// Log implements Logger.
func (NopLogger) Log(Entry) error { return nil }

// Close implements Logger.
func (NopLogger) Close() error { return nil }

// ReadAll reads every JSONL entry from path. Used by tests.
func ReadAll(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var out []Entry
	for {
		var e Entry
		if err := dec.Decode(&e); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
