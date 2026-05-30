// Package index evaluates ADR-0069 composite index prices.
//
// The package deliberately separates the live source book from the pure
// composite function: exchange adapters and the self-market-data bridge only
// update SourceBook, while the mark tick goroutine snapshots it and evaluates a
// deterministic function. That keeps recovery/debugging tractable when a stale
// or outlier source changes liquidation eligibility.
package index

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/xargin/opentrade/pkg/dec"
)

var zero = dec.Zero

// SourceConfig describes one configured index constituent. Self marks the
// OpenTrade spot source so degraded self-reference can be surfaced explicitly
// instead of silently looking like a normal one-source index.
type SourceConfig struct {
	Name   string
	Weight dec.Decimal
	Self   bool
}

// Config is the per-perp-symbol composite-index policy.
type Config struct {
	Sources       []SourceConfig
	Quorum        int
	SourceMaxAge  time.Duration
	DeviationBand dec.Decimal
}

// Validate catches config states that would otherwise turn into silent stale
// indexes at runtime.
func (c Config) Validate() error {
	if len(c.Sources) == 0 {
		return errors.New("index: at least one source required")
	}
	if c.Quorum <= 0 {
		return errors.New("index: quorum must be positive")
	}
	if c.SourceMaxAge <= 0 {
		return errors.New("index: source_max_age must be positive")
	}
	for _, s := range c.Sources {
		if s.Name == "" {
			return errors.New("index: source name required")
		}
		if s.Weight.Sign() <= 0 {
			return errors.New("index: source weight must be positive")
		}
	}
	return nil
}

// Quote is the latest price observed for a source.
type Quote struct {
	Name     string
	Price    dec.Decimal
	TsUnixMs int64
}

// SourceQuote is a configured source joined with its latest observed quote.
type SourceQuote struct {
	SourceConfig
	Price    dec.Decimal
	TsUnixMs int64
	OK       bool
}

// SourceBook is a concurrency-safe latest-price table. It stores all sources
// by configured source name so external adapters can be restarted or replaced
// without changing the tick loop.
type SourceBook struct {
	mu     sync.RWMutex
	quotes map[string]Quote
}

// NewSourceBook returns an empty latest-price table.
func NewSourceBook() *SourceBook { return &SourceBook{quotes: map[string]Quote{}} }

// Upsert records one source update. Non-positive prices and blank names are
// ignored because treating them as valid would create an artificial low index.
func (b *SourceBook) Upsert(name string, price dec.Decimal, tsMs int64) bool {
	if name == "" || price.Sign() <= 0 {
		return false
	}
	if tsMs <= 0 {
		tsMs = time.Now().UnixMilli()
	}
	b.mu.Lock()
	b.quotes[name] = Quote{Name: name, Price: price, TsUnixMs: tsMs}
	b.mu.Unlock()
	return true
}

// Snapshot joins configured sources with their latest quote. Missing sources
// remain present with OK=false so Composite can make a quorum decision against
// the full configured source set.
func (b *SourceBook) Snapshot(sources []SourceConfig) []SourceQuote {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]SourceQuote, 0, len(sources))
	for _, src := range sources {
		q, ok := b.quotes[src.Name]
		out = append(out, SourceQuote{
			SourceConfig: src,
			Price:        q.Price,
			TsUnixMs:     q.TsUnixMs,
			OK:           ok,
		})
	}
	return out
}

// Result is one composite-index evaluation. HasIndex is false only before the
// first fresh index exists; after that, stale results carry the last-good index
// so MarkTick can keep showing unrealized PnL while liquidation is frozen.
type Result struct {
	Index        dec.Decimal
	HasIndex     bool
	Stale        bool
	Degraded     bool
	LiveCount    int
	UsedCount    int
	DroppedCount int
}

// Evaluator adds ADR-0069's last-good freeze behavior around the pure
// Composite function.
type Evaluator struct {
	cfg         Config
	lastGood    dec.Decimal
	hasLastGood bool
}

// NewEvaluator constructs a stateful evaluator for one perp symbol.
func NewEvaluator(cfg Config) (*Evaluator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Evaluator{cfg: cfg}, nil
}

// Eval evaluates a snapshot and freezes to the last-good index when quorum is
// lost. The returned Stale bit is still true so downstream consumers can block
// irreversible actions such as liquidation and funding settlement.
func (e *Evaluator) Eval(now time.Time, snap []SourceQuote) Result {
	res := Composite(snap, e.cfg, now.UnixMilli())
	if res.Stale {
		if e.hasLastGood {
			res.Index = e.lastGood
			res.HasIndex = true
		}
		return res
	}
	e.lastGood = res.Index
	e.hasLastGood = true
	res.HasIndex = true
	return res
}

// Composite is the deterministic ADR-0069 algorithm: stale filtering, quorum,
// median-band outlier rejection, and weighted average over the surviving
// subset. It intentionally does not know about last-good state.
func Composite(snap []SourceQuote, cfg Config, nowMs int64) Result {
	live := make([]SourceQuote, 0, len(snap))
	maxAgeMs := cfg.SourceMaxAge.Milliseconds()
	for _, q := range snap {
		if !q.OK || q.Price.Sign() <= 0 || q.Weight.Sign() <= 0 || q.TsUnixMs <= 0 {
			continue
		}
		if ageMs := nowMs - q.TsUnixMs; ageMs > maxAgeMs {
			continue
		}
		live = append(live, q)
	}
	res := Result{LiveCount: len(live)}
	if len(live) < cfg.Quorum {
		res.Stale = true
		return res
	}

	used := live
	if len(live) >= 3 && cfg.DeviationBand.Sign() > 0 {
		median := medianPrice(live)
		filtered := make([]SourceQuote, 0, len(live))
		for _, q := range live {
			if withinBand(q.Price, median, cfg.DeviationBand) {
				filtered = append(filtered, q)
			}
		}
		res.DroppedCount = len(live) - len(filtered)
		used = filtered
	}
	if len(used) == 0 {
		res.Stale = true
		return res
	}

	weighted, weightSum := zero, zero
	nonSelf := false
	for _, q := range used {
		weighted = weighted.Add(q.Price.Mul(q.Weight))
		weightSum = weightSum.Add(q.Weight)
		if !q.Self {
			nonSelf = true
		}
	}
	if weightSum.Sign() <= 0 {
		res.Stale = true
		return res
	}
	res.Index = weighted.Div(weightSum)
	res.UsedCount = len(used)
	res.Degraded = !nonSelf
	return res
}

func medianPrice(src []SourceQuote) dec.Decimal {
	prices := make([]dec.Decimal, 0, len(src))
	for _, q := range src {
		prices = append(prices, q.Price)
	}
	sort.Slice(prices, func(i, j int) bool { return prices[i].Cmp(prices[j]) < 0 })
	mid := len(prices) / 2
	if len(prices)%2 == 1 {
		return prices[mid]
	}
	return prices[mid-1].Add(prices[mid]).Div(dec.FromInt(2))
}

func withinBand(price, median, band dec.Decimal) bool {
	if median.Sign() <= 0 {
		return false
	}
	diff := price.Sub(median)
	if diff.Sign() < 0 {
		diff = diff.Neg()
	}
	return diff.Div(median).Cmp(band) <= 0
}
