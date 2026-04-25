// BlobStore abstracts snapshot persistence so Save/Load can target either
// the local filesystem (single-node dev / legacy) or shared object storage
// (ADR-0058 phase 1 — enables vshard migration by letting a new owner node
// read whatever the old owner wrote).
//
// Keys passed to a BlobStore are complete filenames including any format
// extension (e.g. "shard-0.pb"). The store is responsible for namespacing
// them onto its backing medium (directory prefix for FS, key prefix for S3).
// Get returns os.ErrNotExist when the key is absent so callers can treat it
// as a cold-start signal.

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BlobStore is the minimal blob-level contract Save/Load depend on.
// Implementations MUST make Put atomic from a reader's perspective: a
// concurrent Get sees either the previous full object or the new full
// object, never a partial write.
type BlobStore interface {
	Put(ctx context.Context, key string, data []byte) error
	// Get returns os.ErrNotExist (matchable by errors.Is) when key is absent.
	Get(ctx context.Context, key string) ([]byte, error)
}

// BlobObject is the List result row. Callers inspect LastModified to
// decide whether to delete (ADR-0064 M1d housekeeper scans on-demand
// snapshot keys and deletes those older than the configured TTL).
type BlobObject struct {
	// Key is the full key as stored, EXCLUDING any implementation-
	// specific prefix (e.g. S3BlobStore.prefix). Pass it back to
	// Get / Delete verbatim.
	Key string
	// LastModified is the wall-clock time the backing store last
	// wrote the object. For FSBlobStore this is the file mtime;
	// for S3BlobStore this is the LastModified reported by
	// ListObjectsV2. The ADR-0064 housekeeper compares this to
	// time.Now()-TTL to decide deletion.
	LastModified time.Time
	// Size is the stored body size in bytes. Informational — the
	// housekeeper does not use it.
	Size int64
}

// BlobLister is the optional capability BlobStore implementations
// can support for scan + delete workloads (ADR-0064 M1d on-demand
// snapshot housekeeping, future periodic-snapshot archive rotation).
// It is intentionally NOT part of the core BlobStore interface so
// restricted-capability stubs (tests, read-only callers) stay
// buildable without implementing List/Delete.
//
// Implementations:
//   - *FSBlobStore: walks baseDir, filters by prefix, uses file
//     mtime for LastModified.
//   - *S3BlobStore: ListObjectsV2 with Prefix, paginated; reports
//     S3's LastModified and Size.
//
// Keys returned from List use the same namespace as Put / Get keys:
// for S3BlobStore the implementation-owned `prefix` is stripped so
// callers work in a flat key-space.
type BlobLister interface {
	List(ctx context.Context, prefix string) ([]BlobObject, error)
	Delete(ctx context.Context, key string) error
}

// FSBlobStore writes to a directory on the local filesystem using
// tmp+fsync+rename for atomicity.
type FSBlobStore struct {
	baseDir string
}

// NewFSBlobStore returns a store rooted at baseDir. The directory is
// created lazily on first Put.
func NewFSBlobStore(baseDir string) *FSBlobStore {
	return &FSBlobStore{baseDir: baseDir}
}

// Put writes data to baseDir/key, staging through a .tmp sibling and
// renaming on top to make the switch atomic.
func (s *FSBlobStore) Put(_ context.Context, key string, data []byte) error {
	path := filepath.Join(s.baseDir, key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("sync: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// Get reads baseDir/key. Missing files surface as os.ErrNotExist.
func (s *FSBlobStore) Get(_ context.Context, key string) ([]byte, error) {
	path := filepath.Join(s.baseDir, key)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return data, nil
}

// List walks baseDir recursively and returns every regular file
// whose relative path starts with prefix. Directories, symlinks,
// and the staging *.tmp files used by Put are skipped so the
// housekeeper never trips over a half-written object. An empty
// prefix lists everything under baseDir. A non-existent baseDir
// is treated as an empty store (returns nil, nil) — matches the
// "cold store, nothing to clean up" housekeeping expectation.
func (s *FSBlobStore) List(_ context.Context, prefix string) ([]BlobObject, error) {
	var out []BlobObject
	err := filepath.WalkDir(s.baseDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return filepath.SkipAll
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil // skip symlinks / devices
		}
		rel, err := filepath.Rel(s.baseDir, path)
		if err != nil {
			return fmt.Errorf("rel %s: %w", path, err)
		}
		// Put writes through a .tmp sibling; surfacing it here would
		// confuse the housekeeper (it is never a snapshot object).
		if strings.HasSuffix(rel, ".tmp") {
			return nil
		}
		if prefix != "" && !strings.HasPrefix(rel, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", path, err)
		}
		out = append(out, BlobObject{
			Key:          filepath.ToSlash(rel),
			LastModified: info.ModTime(),
			Size:         info.Size(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Delete removes baseDir/key. Missing files are a no-op (idempotent
// cleanup is a non-goal of the housekeeper contract — missing key
// means "already cleaned up").
func (s *FSBlobStore) Delete(_ context.Context, key string) error {
	path := filepath.Join(s.baseDir, key)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remove %s: %w", path, err)
	}
	return nil
}
