package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// -----------------------------------------------------------------------------
// ADR-0064 M1d BlobLister tests — FSBlobStore side.
// -----------------------------------------------------------------------------

func TestFSBlobStore_ListAndDelete_HappyPath(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewFSBlobStore(dir)

	// Seed a few objects under different prefixes.
	mustPut(t, store, "vshard-001.pb", []byte("periodic-1"))
	mustPut(t, store, "vshard-002.pb", []byte("periodic-2"))
	mustPut(t, store, "vshard-001-ondemand-1700000000000.pb", []byte("ondemand-1"))
	mustPut(t, store, "vshard-002-ondemand-1700000000001.pb", []byte("ondemand-2"))
	mustPut(t, store, "unrelated.pb", []byte("noise"))

	// Prefix filter should pick up periodic + ondemand for
	// vshard-001 but NOT the unrelated.pb object.
	got, err := store.List(ctx, "vshard-001")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	keys := make(map[string]bool, len(got))
	for _, o := range got {
		keys[o.Key] = true
	}
	wantKeys := []string{"vshard-001.pb", "vshard-001-ondemand-1700000000000.pb"}
	for _, k := range wantKeys {
		if !keys[k] {
			t.Errorf("List missing expected key %q; got=%v", k, got)
		}
	}
	if keys["unrelated.pb"] {
		t.Errorf("List returned unrelated.pb despite prefix filter")
	}
	for _, o := range got {
		if o.Size == 0 {
			t.Errorf("List entry %q has zero Size", o.Key)
		}
		if o.LastModified.IsZero() {
			t.Errorf("List entry %q has zero LastModified", o.Key)
		}
	}

	// Delete one on-demand key; subsequent List no longer shows it.
	if err := store.Delete(ctx, "vshard-001-ondemand-1700000000000.pb"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err = store.List(ctx, "vshard-001-ondemand-")
	if err != nil {
		t.Fatalf("List after Delete: %v", err)
	}
	for _, o := range got {
		if o.Key == "vshard-001-ondemand-1700000000000.pb" {
			t.Errorf("deleted key still listed: %+v", o)
		}
	}
}

// TestFSBlobStore_ListEmptyPrefix enumerates every file under the
// baseDir when prefix is empty.
func TestFSBlobStore_ListEmptyPrefix(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewFSBlobStore(dir)
	mustPut(t, store, "a.pb", []byte("a"))
	mustPut(t, store, "b.pb", []byte("b"))
	got, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List(empty prefix): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d: %+v", len(got), got)
	}
}

// TestFSBlobStore_ListSkipsTmpStagingFiles guards the Put atomicity
// implementation detail (tmp+rename) from leaking into housekeeper
// scans. A .tmp sibling MUST NOT show up in List results even if a
// crashed Put left one behind.
func TestFSBlobStore_ListSkipsTmpStagingFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewFSBlobStore(dir)
	mustPut(t, store, "real.pb", []byte("real"))
	// Simulate a crashed Put: stray .tmp file in the dir.
	if err := os.WriteFile(filepath.Join(dir, "stray.pb.tmp"), []byte("half"), 0o644); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}
	got, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, o := range got {
		if strings.HasSuffix(o.Key, ".tmp") {
			t.Errorf("List returned staging file %q", o.Key)
		}
	}
	// real.pb should still surface.
	found := false
	for _, o := range got {
		if o.Key == "real.pb" {
			found = true
		}
	}
	if !found {
		t.Error("real.pb missing from List output")
	}
}

// TestFSBlobStore_ListMissingDir is the cold-start contract: a
// non-existent baseDir is a valid "empty store" (housekeeper sees
// nothing to clean up), not an error.
func TestFSBlobStore_ListMissingDir(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "does-not-exist")
	store := NewFSBlobStore(dir)
	got, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List against missing dir should not error, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %+v", got)
	}
}

// TestFSBlobStore_DeleteMissingIsNoOp — idempotent cleanup.
func TestFSBlobStore_DeleteMissingIsNoOp(t *testing.T) {
	ctx := context.Background()
	store := NewFSBlobStore(t.TempDir())
	if err := store.Delete(ctx, "never-existed.pb"); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}

func mustPut(t *testing.T, store *FSBlobStore, key string, data []byte) {
	t.Helper()
	if err := store.Put(context.Background(), key, data); err != nil {
		t.Fatalf("Put(%s): %v", key, err)
	}
}
