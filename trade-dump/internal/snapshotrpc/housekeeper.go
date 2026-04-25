package snapshotrpc

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"

	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
)

// Housekeeper periodically scans the blob store for on-demand
// snapshot keys older than a configured TTL and deletes them
// (ADR-0064 §2.5 §G6).
//
// On-demand snapshots exist to be consumed by a single Counter
// startup, then discarded. The TakeSnapshot handler writes them
// with the filename shape `<prefix>vshard-NNN-ondemand-<unix_ms>`
// (.pb / .json extension appended by snapshotpkg.Save). This loop
// detects stale entries by name pattern + LastModified and Deletes
// them — the Counter caller has long since downloaded and restored.
//
// S3 also supports bucket Lifecycle rules that could handle this,
// but relying on those alone is fragile (operator configuration
// drift, per-bucket semantics); the application-level housekeeper
// is the primary mechanism, Lifecycle is defense-in-depth.
//
// Lifecycle model: construct via NewHousekeeper, then call Run(ctx)
// on a dedicated goroutine. Run returns when ctx is cancelled.
// Tick cadence and TTL are separate knobs:
//
//   - ScanInterval (default 5m) — how often the goroutine sweeps
//   - TTL (default 1h) — an entry's LastModified must be older
//     than now()-TTL to be deleted
//
// The two are intentionally independent so scans stay cheap even
// with a short TTL: ScanInterval is the cost rhythm (List + Delete
// RPCs), TTL is the correctness knob.
type Housekeeper struct {
	Lister       snapshotpkg.BlobLister
	OnDemandGlob string // "vshard-" matches every "vshard-NNN-ondemand-*" key
	TTL          time.Duration
	ScanInterval time.Duration
	Logger       *zap.Logger

	// nowFn is a seam for tests.
	nowFn func() time.Time
}

// NewHousekeeper validates and returns a ready Housekeeper. Zero
// values for TTL / ScanInterval / OnDemandGlob fall back to sensible
// defaults; a nil Lister causes Run to no-op (returns immediately)
// so callers wiring this against an unset blob backend don't have
// to special-case the goroutine spawn.
func NewHousekeeper(h Housekeeper) *Housekeeper {
	if h.OnDemandGlob == "" {
		h.OnDemandGlob = "vshard-"
	}
	if h.TTL <= 0 {
		h.TTL = 1 * time.Hour
	}
	if h.ScanInterval <= 0 {
		h.ScanInterval = 5 * time.Minute
	}
	if h.Logger == nil {
		h.Logger = zap.NewNop()
	}
	if h.nowFn == nil {
		h.nowFn = time.Now
	}
	return &h
}

// Run drives the scan loop until ctx is cancelled. Blocks; caller
// runs it on a dedicated goroutine.
//
// One sweep runs at startup (after a short skew delay to avoid
// pile-up with pipeline boot) so the very first cleanup doesn't
// wait a full ScanInterval.
func (h *Housekeeper) Run(ctx context.Context) {
	if h.Lister == nil {
		h.Logger.Info("housekeeper disabled (nil BlobLister)")
		return
	}
	// Initial skew: short random-ish delay so housekeeper doesn't
	// pile on top of pipeline start-up work. Small fixed offset is
	// fine — we don't care about decorrelation across replicas.
	const initialSkew = 10 * time.Second

	tick := time.NewTimer(initialSkew)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			h.Logger.Info("housekeeper stopping")
			return
		case <-tick.C:
			h.sweep(ctx)
			tick.Reset(h.ScanInterval)
		}
	}
}

// sweep runs one List + filter + Delete pass. Errors are logged and
// swallowed — the next sweep retries. Deliberately NOT returned so
// the run loop never dies on a transient blob-store hiccup.
func (h *Housekeeper) sweep(ctx context.Context) {
	start := h.nowFn()
	listed, err := h.Lister.List(ctx, h.OnDemandGlob)
	if err != nil {
		h.Logger.Warn("housekeeper list failed", zap.Error(err))
		return
	}
	cutoff := start.Add(-h.TTL)
	var deleted, kept, skipped int
	for _, obj := range listed {
		// Only consider objects whose key embeds the on-demand
		// marker "-ondemand-". This protects the periodic-snapshot
		// keys (which also match the broader "vshard-" prefix)
		// from ever being touched by this goroutine.
		if !strings.Contains(obj.Key, "-ondemand-") {
			skipped++
			continue
		}
		if obj.LastModified.After(cutoff) {
			kept++
			continue
		}
		if err := h.Lister.Delete(ctx, obj.Key); err != nil {
			h.Logger.Warn("housekeeper delete failed",
				zap.String("key", obj.Key),
				zap.Error(err))
			continue
		}
		deleted++
	}
	h.Logger.Info("housekeeper sweep complete",
		zap.Int("listed", len(listed)),
		zap.Int("deleted", deleted),
		zap.Int("kept", kept),
		zap.Int("skipped", skipped),
		zap.Duration("ttl", h.TTL),
		zap.Duration("elapsed", h.nowFn().Sub(start)))
}
