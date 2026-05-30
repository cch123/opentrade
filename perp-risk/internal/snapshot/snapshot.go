// Package snapshot persists perp-risk's ADR-0071 coordinator state. The file is
// intentionally small JSON for now; the important invariant is the ADR-0048
// shape: folded fund/quota state and next-to-consume journal offsets are saved
// together atomically.
package snapshot

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/xargin/opentrade/pkg/perprisk"
)

type Snapshot struct {
	TsUnixMs    int64             `json:"ts_unix_ms"`
	Coordinator perprisk.Snapshot `json:"coordinator"`
}

func Load(path string) (Snapshot, bool, error) {
	if path == "" {
		return Snapshot{}, false, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, err
	}
	var s Snapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return Snapshot{}, false, err
	}
	return s, true, nil
}

func Save(path string, coord perprisk.Snapshot) error {
	if path == "" {
		return nil
	}
	if err := EnsureDir(path); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(Snapshot{
		TsUnixMs: time.Now().UnixMilli(), Coordinator: coord,
	}, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func EnsureDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}
