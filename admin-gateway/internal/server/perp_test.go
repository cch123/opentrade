package server

// perp_test.go covers the ADR-0075 §5 admin surface: create, publish (merge
// + version assignment), status transitions (matrix + emergency force),
// rollback-as-copy, version listing, OCC precondition, and the audit trail
// every mutation must leave.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/admin-gateway/internal/counterclient"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpcfg"
)

// fakeProjector is a perp-counter shard stub for the §3 dry-run.
type fakeProjector struct {
	resp *perprpc.ProjectRiskConfigResponse
	err  error
	hits int
}

func (f *fakeProjector) ProjectRiskConfig(_ context.Context, _ *connect.Request[perprpc.ProjectRiskConfigRequest]) (*connect.Response[perprpc.ProjectRiskConfigResponse], error) {
	f.hits++
	if f.err != nil {
		return nil, f.err
	}
	resp := f.resp
	if resp == nil {
		resp = &perprpc.ProjectRiskConfigResponse{}
	}
	return connect.NewResponse(resp), nil
}

func newPerpTestServer(t *testing.T, projectors ...PerpProjector) (*Server, *perpcfg.MemoryStore, string) {
	t.Helper()
	rec := &recordingCounter{}
	sc, err := counterclient.NewSharded([]counterclient.Counter{rec})
	if err != nil {
		t.Fatal(err)
	}
	store := perpcfg.NewMemoryStore()
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	audit, err := adminaudit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = audit.Close() })
	srv, err := New(Config{Counter: sc, PerpCatalog: store, PerpCounters: projectors,
		Audit: audit, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	return srv, store, auditPath
}

func perpCreateBody() createPerpSymbolBody {
	return createPerpSymbolBody{
		PerpSymbol: perpcfg.PerpSymbol{
			Symbol: "BTC-USDT-PERP", ContractType: perpcfg.ContractLinearPerp,
			BaseAsset: "BTC", QuoteAsset: "USDT", SettleAsset: "USDT",
			ContractSize: dec.FromInt(1), PriceScale: 2, QtyScale: 3,
		},
		Config: perpcfg.PerpSymbolConfig{
			Status:    perpcfg.StatusTrading, // dev bootstrap; default is PREOPEN
			Precision: perpcfg.Precision{TickSize: dec.New("0.5"), QtyStep: dec.New("0.001")},
			OrderLimits: perpcfg.OrderLimits{
				MinOrderQty: dec.New("0.001"), MaxOrderQty: dec.New("1000"), MinNotional: dec.New("5"),
			},
			RiskTiers: []perpcfg.RiskTier{
				{RiskID: 1, MaxNotional: dec.New("50000"), MaintMarginRatio: dec.New("0.005"),
					MaxLeverage: dec.New("100"), LiqFeeRate: dec.New("0.001")},
				{RiskID: 2, MaxNotional: dec.FromInt(0), MaintMarginRatio: dec.New("0.01"),
					MaxLeverage: dec.New("50"), LiqFeeRate: dec.New("0.002")},
			},
			Funding: perpcfg.FundingParams{IntervalSeconds: 28800, InterestRate: dec.New("0.0003"),
				Cap: dec.New("0.0075"), Floor: dec.New("-0.0075"), Clamp: dec.New("0.0005")},
			Pricing: perpcfg.PricingParams{MarkEmaAlpha: dec.New("0.1"),
				ImpactNotional: dec.New("20000"), IndexDeviationBand: dec.New("0.05")},
			Fees: perpcfg.FeeParams{MakerFeeRate: dec.New("0.0002"), TakerFeeRate: dec.New("0.00055")},
		},
	}
}

func do(t *testing.T, srv *Server, method, url string, body any) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq(method, url, body))
	return rr
}

func mustCreatePerp(t *testing.T, srv *Server) {
	t.Helper()
	rr := do(t, srv, "POST", "/admin/perp/symbols", perpCreateBody())
	if rr.Code != http.StatusOK {
		t.Fatalf("create: status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestPerpCreateAndGet(t *testing.T) {
	srv, store, auditPath := newPerpTestServer(t)
	mustCreatePerp(t, srv)

	versions, err := store.ListVersions(context.Background(), "BTC-USDT-PERP")
	if err != nil || len(versions) != 1 || versions[0].ConfigVersion != 1 {
		t.Fatalf("store state: %+v err=%v", versions, err)
	}
	entries, err := adminaudit.ReadAll(auditPath)
	if err != nil || len(entries) != 1 || entries[0].Op != "admin.perp.symbol.create" {
		t.Fatalf("audit: %+v err=%v", entries, err)
	}

	rr := do(t, srv, "GET", "/admin/perp/symbols/BTC-USDT-PERP", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("get: %d %s", rr.Code, rr.Body.String())
	}
	// Duplicate create → 409.
	if rr := do(t, srv, "POST", "/admin/perp/symbols", perpCreateBody()); rr.Code != http.StatusConflict {
		t.Fatalf("duplicate create: %d", rr.Code)
	}
}

func TestPerpUpdateConfigPublishesMergedVersion(t *testing.T) {
	srv, store, auditPath := newPerpTestServer(t)
	mustCreatePerp(t, srv)

	rr := do(t, srv, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", map[string]any{
		"precision":         map[string]any{"tick_size": "0.1", "qty_step": "0.001"},
		"reason":            "tighter grid",
		"effective_from_ms": 0,
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("update: %d %s", rr.Code, rr.Body.String())
	}
	var resp struct {
		ConfigVersion uint64   `json:"config_version"`
		Changed       []string `json:"changed"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.ConfigVersion != 2 || len(resp.Changed) != 1 || resp.Changed[0] != "precision" {
		t.Fatalf("resp: %+v", resp)
	}
	versions, _ := store.ListVersions(context.Background(), "BTC-USDT-PERP")
	v2 := versions[1]
	// Merge: precision replaced, everything else carried over from v1.
	if !dec.Equal(v2.Precision.TickSize, dec.New("0.1")) ||
		v2.Funding.IntervalSeconds != 28800 || len(v2.RiskTiers) != 2 ||
		v2.Status != perpcfg.StatusTrading || v2.Reason != "tighter grid" {
		t.Fatalf("merged v2: %+v", v2)
	}
	entries, _ := adminaudit.ReadAll(auditPath)
	last := entries[len(entries)-1]
	if last.Op != "admin.perp.config.publish" || last.Params["new_version"].(float64) != 2 {
		t.Fatalf("audit: %+v", last)
	}
}

func TestPerpUpdateConfigOCC(t *testing.T) {
	srv, _, _ := newPerpTestServer(t)
	mustCreatePerp(t, srv)
	rr := do(t, srv, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", map[string]any{
		"expected_version": 7,
		"reason":           "stale editor",
	})
	if rr.Code != http.StatusConflict {
		t.Fatalf("OCC mismatch: %d %s", rr.Code, rr.Body.String())
	}
}

func TestPerpStatusTransitions(t *testing.T) {
	srv, store, auditPath := newPerpTestServer(t)
	mustCreatePerp(t, srv)

	// Legal: TRADING -> CANCEL_ONLY.
	rr := do(t, srv, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/status", setPerpStatusBody{
		Status: perpcfg.StatusCancelOnly, Reason: "incident",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("legal transition: %d %s", rr.Code, rr.Body.String())
	}

	// Illegal without force: CANCEL_ONLY -> DELIVERED.
	rr = do(t, srv, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/status", setPerpStatusBody{
		Status: perpcfg.StatusDelivered, Reason: "nope",
	})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("illegal transition: %d %s", rr.Code, rr.Body.String())
	}

	// Forced without reason → 400; with reason → published + audited forced.
	rr = do(t, srv, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/status", setPerpStatusBody{
		Status: perpcfg.StatusDelivered, Force: true,
	})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("forced without reason: %d", rr.Code)
	}
	rr = do(t, srv, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/status", setPerpStatusBody{
		Status: perpcfg.StatusDelivered, Force: true, Reason: "ticket OPS-42",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("forced with reason: %d %s", rr.Code, rr.Body.String())
	}
	versions, _ := store.ListVersions(context.Background(), "BTC-USDT-PERP")
	if len(versions) != 3 || versions[2].Status != perpcfg.StatusDelivered {
		t.Fatalf("versions: %d last=%+v", len(versions), versions[len(versions)-1])
	}
	entries, _ := adminaudit.ReadAll(auditPath)
	last := entries[len(entries)-1]
	if last.Op != "admin.perp.status.set" || last.Params["forced"] != true {
		t.Fatalf("forced audit: %+v", last)
	}
}

func TestPerpRollbackPublishesCopy(t *testing.T) {
	srv, store, _ := newPerpTestServer(t)
	mustCreatePerp(t, srv)
	// v2 changes the tick.
	rr := do(t, srv, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", map[string]any{
		"precision": map[string]any{"tick_size": "0.1", "qty_step": "0.001"},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("v2: %d", rr.Code)
	}
	// Roll back to v1: v3 is a copy, history intact.
	rr = do(t, srv, "POST", "/admin/perp/symbols/BTC-USDT-PERP/rollback", rollbackPerpBody{
		TargetVersion: 1, Reason: "bad tick",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("rollback: %d %s", rr.Code, rr.Body.String())
	}
	versions, _ := store.ListVersions(context.Background(), "BTC-USDT-PERP")
	if len(versions) != 3 {
		t.Fatalf("history must keep all rows, got %d", len(versions))
	}
	v3 := versions[2]
	if !dec.Equal(v3.Precision.TickSize, dec.New("0.5")) || v3.SourceVersion != 1 || v3.ConfigVersion != 3 {
		t.Fatalf("rollback row: %+v", v3)
	}
	// Unknown target → 404.
	rr = do(t, srv, "POST", "/admin/perp/symbols/BTC-USDT-PERP/rollback", rollbackPerpBody{TargetVersion: 99})
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown target: %d", rr.Code)
	}
}

func TestPerpListVersionsAndSymbols(t *testing.T) {
	srv, _, _ := newPerpTestServer(t)
	mustCreatePerp(t, srv)
	rr := do(t, srv, "GET", "/admin/perp/symbols/BTC-USDT-PERP/versions", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("versions: %d", rr.Code)
	}
	var out struct {
		Versions []perpcfg.PerpSymbolConfig `json:"versions"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if len(out.Versions) != 1 || out.Versions[0].ConfigVersion != 1 {
		t.Fatalf("versions body: %+v", out)
	}
	rr = do(t, srv, "GET", "/admin/perp/symbols", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list: %d", rr.Code)
	}
}

func TestPerpRoutes503WithoutStore(t *testing.T) {
	rec := &recordingCounter{}
	sc, _ := counterclient.NewSharded([]counterclient.Counter{rec})
	srv, err := New(Config{Counter: sc, Audit: adminaudit.NopLogger{}, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	rr := do(t, srv, "GET", "/admin/perp/symbols", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rr.Code)
	}
}

// TestPerpImmediateTighteningRequiresPolicy pins the §3 guardrail end to
// end through the admin surface, including the dry-run fan-out.
func TestPerpImmediateTighteningRequiresPolicy(t *testing.T) {
	tighter := []map[string]any{
		{"risk_id": 1, "max_notional": "50000", "maintenance_margin_ratio": "0.005",
			"max_leverage": "75", "liq_fee_rate": "0.001"},
		{"risk_id": 2, "max_notional": "0", "maintenance_margin_ratio": "0.01",
			"max_leverage": "50", "liq_fee_rate": "0.002"},
	}
	withPolicy := func(maxAffected int, allowMass bool) map[string]any {
		return map[string]any{
			"risk_tiers": tighter, "risk_apply": "IMMEDIATE",
			"reprice_policy": map[string]any{"policy_id": "POL-1",
				"max_affected_accounts": maxAffected, "allow_mass_liquidation": allowMass,
				"reason": "delever"},
		}
	}

	// No policy at all → refused by validation.
	srv, _, _ := newPerpTestServer(t, &fakeProjector{})
	mustCreatePerp(t, srv)
	rr := do(t, srv, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", map[string]any{
		"risk_tiers": tighter, "risk_apply": "IMMEDIATE",
	})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("IMMEDIATE tightening without policy: %d %s", rr.Code, rr.Body.String())
	}

	// Policy but NO perp shards configured → fail-closed (cannot dry-run).
	srvNoShards, _, _ := newPerpTestServer(t)
	mustCreatePerp(t, srvNoShards)
	rr = do(t, srvNoShards, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", withPolicy(100, false))
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "dry-run") {
		t.Fatalf("policy without shards: %d %s", rr.Code, rr.Body.String())
	}

	// Clean projection → publish passes, audit carries dry-run counts.
	proj := &fakeProjector{resp: &perprpc.ProjectRiskConfigResponse{
		AffectedAccounts: 7, PositionsScanned: 40,
	}}
	srvOK, _, auditPath := newPerpTestServer(t, proj)
	mustCreatePerp(t, srvOK)
	rr = do(t, srvOK, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", withPolicy(100, false))
	if rr.Code != http.StatusOK {
		t.Fatalf("clean projection: %d %s", rr.Code, rr.Body.String())
	}
	if proj.hits != 1 {
		t.Fatalf("projector hits = %d", proj.hits)
	}
	entries, _ := adminaudit.ReadAll(auditPath)
	last := entries[len(entries)-1]
	if last.Params["dry_run_affected"].(float64) != 7 {
		t.Fatalf("audit dry-run params: %+v", last.Params)
	}

	// Budget exceeded → refused.
	srvOver, _, _ := newPerpTestServer(t, &fakeProjector{resp: &perprpc.ProjectRiskConfigResponse{
		AffectedAccounts: 101,
	}})
	mustCreatePerp(t, srvOver)
	rr = do(t, srvOver, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", withPolicy(100, false))
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "exceeds") {
		t.Fatalf("budget exceeded: %d %s", rr.Code, rr.Body.String())
	}

	// Would-be liquidations need allow_mass_liquidation.
	liq := &fakeProjector{resp: &perprpc.ProjectRiskConfigResponse{
		AffectedAccounts: 5, LiquidatableAccounts: 2,
	}}
	srvLiq, _, _ := newPerpTestServer(t, liq)
	mustCreatePerp(t, srvLiq)
	rr = do(t, srvLiq, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", withPolicy(100, false))
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "mass liquidation") {
		t.Fatalf("mass liquidation refused: %d %s", rr.Code, rr.Body.String())
	}
	rr = do(t, srvLiq, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", withPolicy(100, true))
	if rr.Code != http.StatusOK {
		t.Fatalf("mass liquidation allowed by policy: %d %s", rr.Code, rr.Body.String())
	}

	// A shard error fails closed.
	srvErr, _, _ := newPerpTestServer(t, &fakeProjector{err: errors.New("shard down")})
	mustCreatePerp(t, srvErr)
	rr = do(t, srvErr, "PUT", "/admin/perp/symbols/BTC-USDT-PERP/config", withPolicy(100, true))
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "shard") {
		t.Fatalf("shard error: %d %s", rr.Code, rr.Body.String())
	}
}
