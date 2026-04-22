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
	"os"
	"path/filepath"
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
