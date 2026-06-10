package perpcfg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	// MySQL driver registration for OpenMySQLStore. Callers that bring their
	// own *sql.DB (NewMySQLStore) are free to register a different driver.
	_ "github.com/go-sql-driver/mysql"

	"github.com/xargin/opentrade/pkg/dec"
)

// MySQLStore is the production Store over the ADR-0075 catalog tables
// (deploy/docker/mysql-init/04-perp-symbol-catalog.sql).
type MySQLStore struct {
	db    *sql.DB
	nowMs func() int64
}

// MySQLConfig configures OpenMySQLStore.
type MySQLConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// OpenMySQLStore opens and pings the DSN. The caller owns Close.
func OpenMySQLStore(cfg MySQLConfig) (*MySQLStore, error) {
	if cfg.DSN == "" {
		return nil, errors.New("perpcfg: MySQL DSN is required")
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("perpcfg: sql.Open: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("perpcfg: mysql ping: %w", err)
	}
	return NewMySQLStore(db), nil
}

// NewMySQLStore wraps an existing pool (tests / shared pools).
func NewMySQLStore(db *sql.DB) *MySQLStore {
	return &MySQLStore{db: db, nowMs: func() int64 { return time.Now().UnixMilli() }}
}

func (s *MySQLStore) Close() error { return s.db.Close() }

func (s *MySQLStore) AnchorVersion(ctx context.Context) (uint64, error) {
	var v uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT version FROM perp_catalog_version WHERE id = 1`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil // anchor row not seeded yet — empty catalog
	}
	return v, err
}

const specColumns = `symbol, contract_type, base_asset, quote_asset, settle_asset,
	contract_size, price_scale, qty_scale, alias`

const configColumns = `symbol, config_version, status, precision_json, order_limits_json,
	risk_tiers_json, funding_json, pricing_json, fees_json, price_protection_json,
	risk_apply, reprice_policy_json, effective_from_ms, created_by, reason, source_version`

func (s *MySQLStore) LoadAll(ctx context.Context) (Catalog, error) {
	out := Catalog{Symbols: map[string]*CatalogSymbol{}}
	// Anchor first: if a publish lands between the three reads, the next poll
	// sees a changed anchor and reloads — the cache never wedges on a torn
	// read, it just refreshes once more.
	anchor, err := s.AnchorVersion(ctx)
	if err != nil {
		return out, err
	}
	out.Anchor = anchor

	specRows, err := s.db.QueryContext(ctx, `SELECT `+specColumns+` FROM perp_symbols`)
	if err != nil {
		return out, fmt.Errorf("perpcfg: load specs: %w", err)
	}
	defer specRows.Close()
	for specRows.Next() {
		spec, err := scanSpec(specRows)
		if err != nil {
			return out, err
		}
		out.Symbols[spec.Symbol] = &CatalogSymbol{Spec: spec}
	}
	if err := specRows.Err(); err != nil {
		return out, err
	}

	cfgRows, err := s.db.QueryContext(ctx,
		`SELECT `+configColumns+` FROM perp_symbol_configs ORDER BY symbol, config_version`)
	if err != nil {
		return out, fmt.Errorf("perpcfg: load configs: %w", err)
	}
	defer cfgRows.Close()
	for cfgRows.Next() {
		cfg, err := scanConfig(cfgRows)
		if err != nil {
			return out, err
		}
		cs := out.Symbols[cfg.Symbol]
		if cs == nil {
			// Config without a spec row: skip rather than fail the whole load —
			// the spec insert is transactional with version 1, so this only
			// happens on manual table surgery.
			continue
		}
		cs.Versions = append(cs.Versions, cfg)
	}
	if err := cfgRows.Err(); err != nil {
		return out, err
	}
	for _, cs := range out.Symbols {
		sortVersions(cs.Versions)
	}
	return out, nil
}

func (s *MySQLStore) ListVersions(ctx context.Context, symbol string) ([]*PerpSymbolConfig, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM perp_symbols WHERE symbol = ?`, symbol).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrSymbolNotFound
		}
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+configColumns+` FROM perp_symbol_configs WHERE symbol = ? ORDER BY config_version`, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*PerpSymbolConfig
	for rows.Next() {
		cfg, err := scanConfig(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cfg)
	}
	return out, rows.Err()
}

func (s *MySQLStore) CreateSymbol(ctx context.Context, spec PerpSymbol, initial PerpSymbolConfig) error {
	if err := ValidateSpec(spec); err != nil {
		return err
	}
	initial.Symbol = spec.Symbol
	initial.ConfigVersion = 1
	if err := ValidateConfig(spec, initial); err != nil {
		return err
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		var one int
		err := tx.QueryRowContext(ctx, `SELECT 1 FROM perp_symbols WHERE symbol = ? FOR UPDATE`, spec.Symbol).Scan(&one)
		switch {
		case err == nil:
			return ErrSymbolExists
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO perp_symbols
			(symbol, contract_type, base_asset, quote_asset, settle_asset,
			 contract_size, price_scale, qty_scale, alias, status, config_version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			spec.Symbol, string(spec.ContractType), spec.BaseAsset, spec.QuoteAsset, spec.SettleAsset,
			spec.ContractSize.String(), spec.PriceScale, spec.QtyScale, spec.Alias,
			string(initial.Status), initial.ConfigVersion); err != nil {
			return fmt.Errorf("perpcfg: insert spec: %w", err)
		}
		if err := insertConfig(ctx, tx, initial); err != nil {
			return err
		}
		return bumpAnchor(ctx, tx)
	})
}

func (s *MySQLStore) PublishConfig(ctx context.Context, cfg PerpSymbolConfig) (uint64, error) {
	var assigned uint64
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		// Lock the spec row: publishes for one symbol are strictly serialized,
		// which is what makes MAX(config_version)+1 assignment race-free.
		specRow := tx.QueryRowContext(ctx,
			`SELECT `+specColumns+` FROM perp_symbols WHERE symbol = ? FOR UPDATE`, cfg.Symbol)
		spec, err := scanSpec(specRow)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSymbolNotFound
		}
		if err != nil {
			return err
		}
		prevRow := tx.QueryRowContext(ctx,
			`SELECT `+configColumns+` FROM perp_symbol_configs
			 WHERE symbol = ? ORDER BY config_version DESC LIMIT 1`, cfg.Symbol)
		prev, err := scanConfig(prevRow)
		if err != nil {
			return fmt.Errorf("perpcfg: load latest version: %w", err)
		}
		cfg.ConfigVersion = prev.ConfigVersion + 1
		if err := ValidateConfig(spec, cfg); err != nil {
			return err
		}
		if err := ValidateRiskApply(prev.RiskTiers, cfg); err != nil {
			return err
		}
		if err := insertConfig(ctx, tx, cfg); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE perp_symbols SET status = ?, config_version = ? WHERE symbol = ?`,
			string(cfg.Status), cfg.ConfigVersion, cfg.Symbol); err != nil {
			return fmt.Errorf("perpcfg: update spec pointers: %w", err)
		}
		assigned = cfg.ConfigVersion
		return bumpAnchor(ctx, tx)
	})
	return assigned, err
}

func (s *MySQLStore) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func insertConfig(ctx context.Context, tx *sql.Tx, c PerpSymbolConfig) error {
	enc := func(v any) (string, error) {
		b, err := json.Marshal(v)
		return string(b), err
	}
	precision, err := enc(c.Precision)
	if err != nil {
		return err
	}
	limits, err := enc(c.OrderLimits)
	if err != nil {
		return err
	}
	tiers, err := enc(c.RiskTiers)
	if err != nil {
		return err
	}
	funding, err := enc(c.Funding)
	if err != nil {
		return err
	}
	pricing, err := enc(c.Pricing)
	if err != nil {
		return err
	}
	fees, err := enc(c.Fees)
	if err != nil {
		return err
	}
	protection, err := enc(c.PriceProtection)
	if err != nil {
		return err
	}
	var policy sql.NullString
	if c.RepricePolicy != nil {
		p, err := enc(c.RepricePolicy)
		if err != nil {
			return err
		}
		policy = sql.NullString{String: p, Valid: true}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO perp_symbol_configs
		(symbol, config_version, status, precision_json, order_limits_json,
		 risk_tiers_json, funding_json, pricing_json, fees_json, price_protection_json,
		 risk_apply, reprice_policy_json, effective_from_ms, created_by, reason, source_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.Symbol, c.ConfigVersion, string(c.Status), precision, limits,
		tiers, funding, pricing, fees, protection,
		string(c.RiskApply), policy, c.EffectiveFromMs, c.CreatedBy, c.Reason, c.SourceVersion); err != nil {
		return fmt.Errorf("perpcfg: insert config: %w", err)
	}
	return nil
}

func bumpAnchor(ctx context.Context, tx *sql.Tx) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE perp_catalog_version SET version = version + 1 WHERE id = 1`)
	if err != nil {
		return fmt.Errorf("perpcfg: bump anchor: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		// Anchor row missing (fresh DB without the seed insert) — create it.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO perp_catalog_version (id, version) VALUES (1, 1)`); err != nil {
			return fmt.Errorf("perpcfg: seed anchor: %w", err)
		}
	}
	return nil
}

// rowScanner abstracts *sql.Row / *sql.Rows.
type rowScanner interface{ Scan(dest ...any) error }

func scanSpec(r rowScanner) (PerpSymbol, error) {
	var (
		s            PerpSymbol
		contractType string
		contractSize string
	)
	if err := r.Scan(&s.Symbol, &contractType, &s.BaseAsset, &s.QuoteAsset, &s.SettleAsset,
		&contractSize, &s.PriceScale, &s.QtyScale, &s.Alias); err != nil {
		return s, err
	}
	s.ContractType = ContractType(contractType)
	size, err := dec.Parse(contractSize)
	if err != nil {
		return s, fmt.Errorf("perpcfg: bad contract_size %q: %w", contractSize, err)
	}
	s.ContractSize = size
	return s, nil
}

func scanConfig(r rowScanner) (*PerpSymbolConfig, error) {
	var (
		c                                                            PerpSymbolConfig
		status, riskApply                                            string
		precision, limits, tiers, funding, pricing, fees, protection []byte
		policy                                                       sql.NullString
	)
	if err := r.Scan(&c.Symbol, &c.ConfigVersion, &status, &precision, &limits,
		&tiers, &funding, &pricing, &fees, &protection,
		&riskApply, &policy, &c.EffectiveFromMs, &c.CreatedBy, &c.Reason, &c.SourceVersion); err != nil {
		return nil, err
	}
	c.Status = Status(status)
	c.RiskApply = RiskApply(riskApply)
	for _, f := range []struct {
		raw []byte
		out any
	}{
		{precision, &c.Precision}, {limits, &c.OrderLimits}, {tiers, &c.RiskTiers},
		{funding, &c.Funding}, {pricing, &c.Pricing}, {fees, &c.Fees},
		{protection, &c.PriceProtection},
	} {
		if len(f.raw) == 0 {
			continue
		}
		if err := json.Unmarshal(f.raw, f.out); err != nil {
			return nil, fmt.Errorf("perpcfg: decode config %s v%d: %w", c.Symbol, c.ConfigVersion, err)
		}
	}
	if policy.Valid && policy.String != "" && policy.String != "null" {
		var p RiskRepricePolicy
		if err := json.Unmarshal([]byte(policy.String), &p); err != nil {
			return nil, fmt.Errorf("perpcfg: decode reprice policy %s v%d: %w", c.Symbol, c.ConfigVersion, err)
		}
		c.RepricePolicy = &p
	}
	return &c, nil
}
