package adminaudit

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestFileLogger_AppendAndReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")

	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	l.SetClock(func() time.Time { return time.Unix(1_700_000_000, 0) })
	if err := l.Log(Entry{Op: "admin.symbol.put", Target: "BTC-USDT", Status: StatusOK, AdminID: "ops-1"}); err != nil {
		t.Fatalf("log: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and append a second entry — new file should start where the
	// previous one left off (no truncation).
	l2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	l2.SetClock(func() time.Time { return time.Unix(1_700_000_001, 0) })
	if err := l2.Log(Entry{Op: "admin.cancel-orders", Params: map[string]any{"symbol": "ETH-USDT"}, Status: StatusOK}); err != nil {
		t.Fatalf("log2: %v", err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d", len(got))
	}
	if got[0].Op != "admin.symbol.put" || got[0].Target != "BTC-USDT" || got[0].AdminID != "ops-1" {
		t.Fatalf("entry 0: %+v", got[0])
	}
	if got[0].TsUnixMS != 1_700_000_000_000 {
		t.Fatalf("entry 0 ts: %d", got[0].TsUnixMS)
	}
	if got[1].Op != "admin.cancel-orders" || got[1].Params["symbol"] != "ETH-USDT" {
		t.Fatalf("entry 1: %+v", got[1])
	}
}

func TestFileLogger_ConcurrentWritesAreLineAligned(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = l.Log(Entry{Op: "admin.test", Target: "t", Status: StatusOK, Params: map[string]any{"i": i}})
		}()
	}
	wg.Wait()

	entries, err := ReadAll(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != N {
		t.Fatalf("want %d entries, got %d", N, len(entries))
	}
}

func TestFileLogger_RequiresOpAndStatus(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Log(Entry{Status: StatusOK}); err == nil {
		t.Error("expected error for missing op")
	}
	if err := l.Log(Entry{Op: "x"}); err == nil {
		t.Error("expected error for missing status")
	}
}

func TestFileLogger_EmptyPathRejected(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestFileLogger_MissingDirectoryRejected(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "no-such-dir", "audit.jsonl"))
	if err == nil {
		t.Fatal("expected error for missing directory")
	}
}

func TestFileLogger_ClosedRejectsLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	if err := l.Log(Entry{Op: "x", Status: StatusOK}); err == nil {
		t.Fatal("expected error after close")
	}
}

func TestNopLogger(t *testing.T) {
	var l Logger = NopLogger{}
	if err := l.Log(Entry{Op: "x", Status: StatusOK}); err != nil {
		t.Fatalf("nop log: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("nop close: %v", err)
	}
}

// Ensure mkdir + Open regression: an existing directory + write permission
// must produce a file with 0o600 mode.
func TestFileLogger_FileMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, want 0o600", st.Mode().Perm())
	}
}
