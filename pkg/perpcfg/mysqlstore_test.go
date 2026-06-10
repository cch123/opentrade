package perpcfg

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func specRow(spec PerpSymbol) []driver.Value {
	return []driver.Value{
		spec.Symbol, string(spec.ContractType), spec.BaseAsset, spec.QuoteAsset,
		spec.SettleAsset, spec.ContractSize.String(), spec.PriceScale, spec.QtyScale, spec.Alias,
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func configRow(t *testing.T, c PerpSymbolConfig) []driver.Value {
	t.Helper()
	return []driver.Value{
		c.Symbol, c.ConfigVersion, string(c.Status),
		mustJSON(t, c.Precision), mustJSON(t, c.OrderLimits), mustJSON(t, c.RiskTiers),
		mustJSON(t, c.Funding), mustJSON(t, c.Pricing), mustJSON(t, c.Fees),
		mustJSON(t, c.PriceProtection), string(c.RiskApply), nil,
		c.EffectiveFromMs, c.CreatedBy, c.Reason, c.SourceVersion,
	}
}

var (
	specCols = []string{"symbol", "contract_type", "base_asset", "quote_asset", "settle_asset",
		"contract_size", "price_scale", "qty_scale", "alias"}
	configCols = []string{"symbol", "config_version", "status", "precision_json", "order_limits_json",
		"risk_tiers_json", "funding_json", "pricing_json", "fees_json", "price_protection_json",
		"risk_apply", "reprice_policy_json", "effective_from_ms", "created_by", "reason", "source_version"}
)

// TestMySQLPublishConfigTx pins the publish transaction shape: spec row lock,
// monotonic version assignment from the latest row, immutable insert, spec
// pointer update, anchor bump — all inside one transaction.
func TestMySQLPublishConfigTx(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewMySQLStore(db)

	spec := validSpec()
	prev := validConfig() // v1 on disk

	mock.ExpectBegin()
	specRows := sqlmock.NewRows(specCols).AddRow(specRow(spec)...)
	mock.ExpectQuery(regexp.QuoteMeta("FROM perp_symbols WHERE symbol = ? FOR UPDATE")).
		WithArgs("BTC-USDT-PERP").WillReturnRows(specRows)
	mock.ExpectQuery(regexp.QuoteMeta("ORDER BY config_version DESC LIMIT 1")).
		WithArgs("BTC-USDT-PERP").
		WillReturnRows(sqlmock.NewRows(configCols).AddRow(configRow(t, prev)...))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO perp_symbol_configs")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE perp_symbols SET status = ?, config_version = ?")).
		WithArgs("CANCEL_ONLY", uint64(2), "BTC-USDT-PERP").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE perp_catalog_version SET version = version + 1")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	next := validConfig()
	next.Status = StatusCancelOnly
	ver, err := store.PublishConfig(context.Background(), next)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ver != 2 {
		t.Fatalf("assigned version = %d, want 2", ver)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestMySQLPublishValidationRollsBack pins fail-closed publishing: a config
// that fails structural validation must roll the transaction back and write
// nothing.
func TestMySQLPublishValidationRollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewMySQLStore(db)

	spec := validSpec()
	prev := validConfig()

	mock.ExpectBegin()
	specRows := sqlmock.NewRows(specCols).AddRow(specRow(spec)...)
	mock.ExpectQuery(regexp.QuoteMeta("FOR UPDATE")).WillReturnRows(specRows)
	mock.ExpectQuery(regexp.QuoteMeta("ORDER BY config_version DESC LIMIT 1")).
		WillReturnRows(sqlmock.NewRows(configCols).AddRow(configRow(t, prev)...))
	mock.ExpectRollback()

	bad := validConfig()
	bad.Status = "PAUSED" // not in the status set
	if _, err := store.PublishConfig(context.Background(), bad); err == nil {
		t.Fatal("invalid status must fail")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestMySQLLoadAllRoundTrip pins column order + JSON decode of the LoadAll
// read path against the same encoding insertConfig writes.
func TestMySQLLoadAllRoundTrip(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewMySQLStore(db)

	spec := validSpec()
	cfg := validConfig()
	cfg.RepricePolicy = nil

	mock.ExpectQuery(regexp.QuoteMeta("SELECT version FROM perp_catalog_version")).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(7))
	specRows := sqlmock.NewRows(specCols).AddRow(specRow(spec)...)
	mock.ExpectQuery(regexp.QuoteMeta("FROM perp_symbols")).WillReturnRows(specRows)
	mock.ExpectQuery(regexp.QuoteMeta("FROM perp_symbol_configs ORDER BY symbol, config_version")).
		WillReturnRows(sqlmock.NewRows(configCols).AddRow(configRow(t, cfg)...))

	cat, err := store.LoadAll(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cat.Anchor != 7 {
		t.Fatalf("anchor = %d", cat.Anchor)
	}
	cs := cat.Symbols["BTC-USDT-PERP"]
	if cs == nil || len(cs.Versions) != 1 {
		t.Fatalf("catalog shape: %+v", cat.Symbols)
	}
	got := cs.Versions[0]
	if got.Status != StatusTrading || len(got.RiskTiers) != 2 ||
		got.Funding.IntervalSeconds != 28800 || got.RiskApply != RiskApplyStaged {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
