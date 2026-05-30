// Package snapshot persists perp-counter recovery state to disk and loads it on
// startup (ADR-0068 invariant #5 / ADR-0048). One file holds the atomic image
// captured under the service barrier: engine positions/wallets/marks/insurance
// + the service order store + the bound perp-trade-event offsets + idempotency
// watermarks.
//
// MVP uses JSON (the ADR-0049 debug encoding) — perp's snapshot producer is
// still an open question in ADR-0068 (self-produced here vs a trade-dump shadow
// replaying perp-journal), so a human-readable format keeps recovery debuggable
// while that's decided; a proto encoding is a drop-in follow-up.
package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/perp-counter/internal/service"
)

// PerpSnapshot is the full on-disk image.
type PerpSnapshot struct {
	Version  int              `json:"version"`
	TsUnixMs int64            `json:"ts_unix_ms"`
	Engine   engine.Snapshot  `json:"engine"`
	Service  service.Snapshot `json:"service"`
}

// formatVersion guards against silently loading an incompatible layout.
const formatVersion = 1

// Save atomically writes snap to path (temp file + rename, so a crash mid-write
// never leaves a torn snapshot). The parent directory must exist.
func Save(path string, snap PerpSnapshot) error {
	snap.Version = formatVersion
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write temp snapshot: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}
	return nil
}

// Load reads the snapshot at path. ok=false (nil error) when the file does not
// exist — a cold start. A version mismatch is a hard error (operator must
// migrate or wipe rather than silently mis-restore account state).
func Load(path string) (PerpSnapshot, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return PerpSnapshot{}, false, nil
		}
		return PerpSnapshot{}, false, fmt.Errorf("read snapshot: %w", err)
	}
	var snap PerpSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return PerpSnapshot{}, false, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	if snap.Version != formatVersion {
		return PerpSnapshot{}, false, fmt.Errorf("snapshot version %d != supported %d", snap.Version, formatVersion)
	}
	return snap, true, nil
}

// EnsureDir creates the snapshot's parent directory if missing.
func EnsureDir(path string) error {
	return os.MkdirAll(filepath.Dir(path), 0o755)
}
