package perpcfg

import (
	"context"
	"errors"
	"sort"
	"sync"
)

// Store errors. ErrSymbolExists / ErrSymbolNotFound map to HTTP conflict /
// not-found at the admin layer.
var (
	ErrSymbolExists   = errors.New("perpcfg: symbol already exists")
	ErrSymbolNotFound = errors.New("perpcfg: symbol not found")
	ErrVersionGone    = errors.New("perpcfg: config version not found")
)

// Catalog is one consistent read of the whole store: every spec plus every
// published version, with the anchor the read was taken at.
type Catalog struct {
	Anchor  uint64
	Symbols map[string]*CatalogSymbol
}

// CatalogSymbol bundles a spec with its full version history (ascending by
// config_version).
type CatalogSymbol struct {
	Spec     PerpSymbol
	Versions []*PerpSymbolConfig
}

// ActiveAt returns the highest version whose effective_from_ms <= nowMs.
// ok=false when no version is effective yet (e.g. only future-dated rows).
func (cs *CatalogSymbol) ActiveAt(nowMs int64) (*PerpSymbolConfig, bool) {
	for i := len(cs.Versions) - 1; i >= 0; i-- {
		if cs.Versions[i].EffectiveFromMs <= nowMs {
			return cs.Versions[i], true
		}
	}
	return nil, false
}

// At returns the exact version.
func (cs *CatalogSymbol) At(version uint64) (*PerpSymbolConfig, bool) {
	for _, v := range cs.Versions {
		if v.ConfigVersion == version {
			return v, true
		}
	}
	return nil, false
}

// Store is the catalog persistence boundary (ADR-0075 §1/§4/§5). MySQL is the
// production implementation; MemoryStore backs tests and broker-less dev.
//
// Write-side contract every implementation must keep:
//   - config_version is assigned by the store, strictly monotonic per symbol;
//   - a publish is atomic with the anchor bump (readers poll the anchor);
//   - published rows are immutable — rollback = publish a copy.
type Store interface {
	// AnchorVersion is the cheap poll target: it changes iff the catalog
	// changed.
	AnchorVersion(ctx context.Context) (uint64, error)
	// LoadAll reads the entire catalog (specs + full version history).
	LoadAll(ctx context.Context) (Catalog, error)
	// ListVersions returns a symbol's history, ascending.
	ListVersions(ctx context.Context, symbol string) ([]*PerpSymbolConfig, error)

	// CreateSymbol installs a new contract spec together with its version-1
	// config. initial.ConfigVersion is assigned (=1) by the store.
	CreateSymbol(ctx context.Context, spec PerpSymbol, initial PerpSymbolConfig) error
	// PublishConfig appends the next version for cfg.Symbol and returns the
	// assigned config_version. Structural validation (ValidateConfig,
	// ValidateRiskApply vs the previous version) runs inside the publish
	// transaction; the status transition matrix is the admin layer's check
	// (it owns the emergency-override path).
	PublishConfig(ctx context.Context, cfg PerpSymbolConfig) (uint64, error)

	Close() error
}

// ---------------------------------------------------------------------------
// MemoryStore
// ---------------------------------------------------------------------------

// MemoryStore is the in-memory Store for tests and broker-less dev runs.
type MemoryStore struct {
	mu      sync.Mutex
	anchor  uint64
	specs   map[string]PerpSymbol
	configs map[string][]*PerpSymbolConfig // ascending
	nowMs   func() int64                   // CreatedAtMs stamping; nil → 0
}

// NewMemoryStore returns an empty in-memory catalog.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		specs:   map[string]PerpSymbol{},
		configs: map[string][]*PerpSymbolConfig{},
	}
}

// SetClock installs a CreatedAtMs source (tests).
func (m *MemoryStore) SetClock(nowMs func() int64) { m.nowMs = nowMs }

func (m *MemoryStore) stamp() int64 {
	if m.nowMs == nil {
		return 0
	}
	return m.nowMs()
}

func (m *MemoryStore) AnchorVersion(context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.anchor, nil
}

func (m *MemoryStore) LoadAll(context.Context) (Catalog, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := Catalog{Anchor: m.anchor, Symbols: make(map[string]*CatalogSymbol, len(m.specs))}
	for sym, spec := range m.specs {
		cs := &CatalogSymbol{Spec: spec, Versions: make([]*PerpSymbolConfig, 0, len(m.configs[sym]))}
		for _, c := range m.configs[sym] {
			cp := *c
			cs.Versions = append(cs.Versions, &cp)
		}
		out.Symbols[sym] = cs
	}
	return out, nil
}

func (m *MemoryStore) ListVersions(_ context.Context, symbol string) ([]*PerpSymbolConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows, ok := m.configs[symbol]
	if !ok {
		return nil, ErrSymbolNotFound
	}
	out := make([]*PerpSymbolConfig, 0, len(rows))
	for _, c := range rows {
		cp := *c
		out = append(out, &cp)
	}
	return out, nil
}

func (m *MemoryStore) CreateSymbol(_ context.Context, spec PerpSymbol, initial PerpSymbolConfig) error {
	if err := ValidateSpec(spec); err != nil {
		return err
	}
	initial.Symbol = spec.Symbol
	initial.ConfigVersion = 1
	if err := ValidateConfig(spec, initial); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.specs[spec.Symbol]; exists {
		return ErrSymbolExists
	}
	spec.CreatedAtMs = m.stamp()
	initial.CreatedAtMs = spec.CreatedAtMs
	m.specs[spec.Symbol] = spec
	m.configs[spec.Symbol] = []*PerpSymbolConfig{&initial}
	m.anchor++
	return nil
}

func (m *MemoryStore) PublishConfig(_ context.Context, cfg PerpSymbolConfig) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	spec, ok := m.specs[cfg.Symbol]
	if !ok {
		return 0, ErrSymbolNotFound
	}
	rows := m.configs[cfg.Symbol]
	prev := rows[len(rows)-1]
	cfg.ConfigVersion = prev.ConfigVersion + 1
	if err := ValidateConfig(spec, cfg); err != nil {
		return 0, err
	}
	if err := ValidateRiskApply(prev.RiskTiers, cfg); err != nil {
		return 0, err
	}
	cfg.CreatedAtMs = m.stamp()
	cp := cfg
	m.configs[cfg.Symbol] = append(rows, &cp)
	m.anchor++
	return cfg.ConfigVersion, nil
}

func (m *MemoryStore) Close() error { return nil }

// sortVersions keeps a loaded history ascending — shared by store impls that
// read rows in unspecified order.
func sortVersions(rows []*PerpSymbolConfig) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].ConfigVersion < rows[j].ConfigVersion })
}
