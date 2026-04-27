package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/admin-gateway/internal/counterclient"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/auth"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// recordingCounter captures AdminCancelOrders calls.
type recordingCounter struct {
	fn   func(*counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error)
	hits int
}

func (r *recordingCounter) AdminCancelOrders(_ context.Context, req *connect.Request[counterrpc.AdminCancelOrdersRequest]) (*connect.Response[counterrpc.AdminCancelOrdersResponse], error) {
	r.hits++
	if r.fn == nil {
		return connect.NewResponse(&counterrpc.AdminCancelOrdersResponse{}), nil
	}
	resp, err := r.fn(req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func newTestServer(t *testing.T, shardFns ...func(*counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error)) (*Server, *etcdcfg.MemorySource, string, []*recordingCounter) {
	t.Helper()
	clients := make([]counterclient.Counter, len(shardFns))
	recs := make([]*recordingCounter, len(shardFns))
	for i, fn := range shardFns {
		rec := &recordingCounter{fn: fn}
		recs[i] = rec
		clients[i] = rec
	}
	if len(clients) == 0 {
		rec := &recordingCounter{}
		recs = append(recs, rec)
		clients = append(clients, rec)
	}
	sc, err := counterclient.NewSharded(clients)
	if err != nil {
		t.Fatal(err)
	}
	mem := etcdcfg.NewMemorySource()
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	audit, err := adminaudit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = audit.Close() })
	srv, err := New(Config{
		Counter: sc,
		Etcd:    NewMemoryShim(mem),
		Audit:   audit,
		Logger:  zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, mem, auditPath, recs
}

// adminReq crafts a request already decorated as admin-role (simulates
// the outer AdminMiddleware + RequireAdmin chain).
func adminReq(method, url string, body any) *http.Request {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	r := httptest.NewRequest(method, url, bytes.NewReader(b))
	ctx := auth.WithUserID(r.Context(), "ops-bot")
	ctx = auth.WithRole(ctx, auth.RoleAdmin)
	return r.WithContext(ctx)
}

func TestPutSymbol_WritesEtcdAndAudit(t *testing.T) {
	srv, mem, auditPath, _ := newTestServer(t)

	req := adminReq("PUT", "/admin/symbols/BTC-USDT", putSymbolBody{Shard: "match-0", Trading: true, Version: "v1"})
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	cfgs, _, _ := mem.List(context.Background())
	got, ok := cfgs["BTC-USDT"]
	if !ok || got.Shard != "match-0" || !got.Trading || got.Version != "v1" {
		t.Fatalf("etcd state: %+v", cfgs)
	}
	entries, err := adminaudit.ReadAll(auditPath)
	if err != nil || len(entries) != 1 {
		t.Fatalf("audit entries: %+v err=%v", entries, err)
	}
	e := entries[0]
	if e.Op != "admin.symbol.put" || e.Target != "BTC-USDT" || e.AdminID != "ops-bot" || e.Status != adminaudit.StatusOK {
		t.Fatalf("entry=%+v", e)
	}
}

// ADR-0053 M1.a precision fixtures ------------------------------------------

func singleTierFixture() []etcdcfg.PrecisionTier {
	return []etcdcfg.PrecisionTier{
		{
			PriceFrom:                     dec.New("0"),
			PriceTo:                       dec.New("0"),
			TickSize:                      dec.New("0.01"),
			StepSize:                      dec.New("0.00001"),
			QuoteStepSize:                 dec.New("0.01"),
			MinQty:                        dec.New("0.0001"),
			MaxQty:                        dec.New("1000"),
			MinQuoteQty:                   dec.New("1"),
			MinQuoteAmount:                dec.New("5"),
			MarketMinQty:                  dec.New("0.0001"),
			MarketMaxQty:                  dec.New("500"),
			EnforceMinQuoteAmountOnMarket: true,
			AvgPriceMins:                  5,
		},
	}
}

func TestPutSymbol_WithPrecision_WritesAll(t *testing.T) {
	srv, mem, auditPath, _ := newTestServer(t)
	body := putSymbolBody{
		Shard:            "match-0",
		Trading:          true,
		Version:          "v1",
		BaseAsset:        "BTC",
		QuoteAsset:       "USDT",
		PrecisionVersion: 1,
		Tiers:            singleTierFixture(),
	}
	req := adminReq("PUT", "/admin/symbols/BTC-USDT", body)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	cfgs, _, _ := mem.List(context.Background())
	got := cfgs["BTC-USDT"]
	if got.BaseAsset != "BTC" || got.QuoteAsset != "USDT" || got.PrecisionVersion != 1 {
		t.Fatalf("precision metadata lost: %+v", got)
	}
	if !got.HasPrecision() || len(got.Tiers) != 1 {
		t.Fatalf("tiers lost: %+v", got.Tiers)
	}
	if got.Tiers[0].TickSize.String() != "0.01" || got.Tiers[0].StepSize.String() != "0.00001" {
		t.Fatalf("tier decimal fields lost: %+v", got.Tiers[0])
	}

	entries, err := adminaudit.ReadAll(auditPath)
	if err != nil || len(entries) != 1 {
		t.Fatalf("audit entries: %+v err=%v", entries, err)
	}
	params := entries[0].Params
	// JSON unmarshal gives float64 for numbers, so coerce.
	if v, ok := params["tier_count"].(float64); !ok || int(v) != 1 {
		t.Errorf("audit tier_count=%v", params["tier_count"])
	}
	if params["base_asset"] != "BTC" || params["quote_asset"] != "USDT" {
		t.Errorf("audit asset fields: %+v", params)
	}
	if v, ok := params["precision_version"].(float64); !ok || int(v) != 1 {
		t.Errorf("audit precision_version=%v", params["precision_version"])
	}
}

func TestPutSymbol_InvalidTiers_Rejected(t *testing.T) {
	srv, mem, auditPath, _ := newTestServer(t)
	// Missing first-tier PriceFrom=0 → ValidateTiers rejects.
	bad := []etcdcfg.PrecisionTier{
		{
			PriceFrom: dec.New("1"), PriceTo: dec.New("0"),
			TickSize: dec.New("0.01"), StepSize: dec.New("0.00001"),
		},
	}
	body := putSymbolBody{Shard: "match-0", Trading: true, Tiers: bad}
	req := adminReq("PUT", "/admin/symbols/BTC-USDT", body)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "first tier PriceFrom must be 0") {
		t.Errorf("error message lost reason: %s", rr.Body.String())
	}
	// etcd must be untouched — rejection happens before Put.
	cfgs, _, _ := mem.List(context.Background())
	if _, ok := cfgs["BTC-USDT"]; ok {
		t.Error("etcd should not have been written on validation failure")
	}
	// Audit should have no entries — we reject before recording (rejection
	// is a client error, not an admin action worth auditing). Current
	// server.go behaviour: audit only on successful / etcd-level failures.
	entries, _ := adminaudit.ReadAll(auditPath)
	if len(entries) != 0 {
		t.Errorf("expected no audit for 400, got %+v", entries)
	}
}

func TestPutSymbol_LegacyBodyOmitsPrecisionKeys(t *testing.T) {
	srv, mem, _, _ := newTestServer(t)
	// Old-school PUT — no precision fields at all.
	body := putSymbolBody{Shard: "match-0", Trading: true, Version: "v1"}
	req := adminReq("PUT", "/admin/symbols/BTC-USDT", body)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	cfgs, _, _ := mem.List(context.Background())
	got := cfgs["BTC-USDT"]
	if got.HasPrecision() {
		t.Errorf("legacy PUT should produce zero-value precision, got %+v", got)
	}
	if got.BaseAsset != "" || got.QuoteAsset != "" || got.PrecisionVersion != 0 {
		t.Errorf("legacy PUT should leave precision metadata empty: %+v", got)
	}
}

func TestGetSymbol_EchoesPrecision(t *testing.T) {
	srv, mem, _, _ := newTestServer(t)
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT", etcdcfg.SymbolConfig{
		Shard:            "match-0",
		Trading:          true,
		BaseAsset:        "BTC",
		QuoteAsset:       "USDT",
		PrecisionVersion: 2,
		Tiers:            singleTierFixture(),
	})
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("GET", "/admin/symbols/BTC-USDT", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Symbol symbolJSON `json:"symbol"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Symbol.BaseAsset != "BTC" || out.Symbol.PrecisionVersion != 2 {
		t.Errorf("echoed precision metadata wrong: %+v", out.Symbol)
	}
	if len(out.Symbol.Tiers) != 1 {
		t.Fatalf("echoed tier count: %d", len(out.Symbol.Tiers))
	}
}

func TestPutSymbol_ScheduledChangeValidated(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	effective, _ := time.Parse(time.RFC3339, "2026-05-01T00:00:00Z")
	// Valid scheduled change.
	body := putSymbolBody{
		Shard: "match-0", Trading: true,
		ScheduledChange: &etcdcfg.PrecisionChange{
			EffectiveAt:         effective,
			NewTiers:            singleTierFixture(),
			NewPrecisionVersion: 2,
			Reason:              "test",
		},
	}
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("PUT", "/admin/symbols/BTC-USDT", body))
	if rr.Code != http.StatusOK {
		t.Fatalf("valid schedule: status=%d body=%s", rr.Code, rr.Body.String())
	}

	// Invalid scheduled change — missing NewPrecisionVersion.
	body.ScheduledChange.NewPrecisionVersion = 0
	rr2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr2, adminReq("PUT", "/admin/symbols/BTC-USDT", body))
	if rr2.Code != http.StatusBadRequest {
		t.Errorf("zero NewPrecisionVersion: status=%d", rr2.Code)
	}
}

// ---- M1.b precision rollout tests ------------------------------------------

// seedSymbolWithTiers writes a symbol into the MemorySource with the given
// tier list and a specified precision version.
func seedSymbolWithTiers(t *testing.T, mem *etcdcfg.MemorySource, symbol string, tiers []etcdcfg.PrecisionTier, ver uint64) {
	t.Helper()
	_, err := mem.PutCtx(context.Background(), symbol, etcdcfg.SymbolConfig{
		Shard:            "match-0",
		Trading:          true,
		BaseAsset:        "BTC",
		QuoteAsset:       "USDT",
		PrecisionVersion: ver,
		Tiers:            tiers,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSchedulePrecision_ValidSplit(t *testing.T) {
	srv, mem, auditPath, _ := newTestServer(t)
	// Seed single open tier.
	seedSymbolWithTiers(t, mem, "BTC-USDT", singleTierFixture(), 1)

	// Split [0, +∞) into [0, 1000) + [1000, +∞).
	split := []etcdcfg.PrecisionTier{
		{
			PriceFrom: dec.New("0"), PriceTo: dec.New("1000"),
			TickSize: dec.New("0.01"), StepSize: dec.New("0.00001"),
		},
		{
			PriceFrom: dec.New("1000"), PriceTo: dec.New("0"),
			TickSize: dec.New("1"), StepSize: dec.New("0.0001"),
		},
	}
	effective, _ := time.Parse(time.RFC3339, "2026-05-01T00:00:00Z")
	body := etcdcfg.PrecisionChange{
		EffectiveAt:         effective,
		NewTiers:            split,
		NewPrecisionVersion: 2,
		Reason:              "BTC price band expansion",
	}
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("PUT", "/admin/symbols/BTC-USDT/precision", body))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	// etcd reflects the staged change but current tiers are untouched.
	cfgs, _, _ := mem.List(context.Background())
	got := cfgs["BTC-USDT"]
	if got.ScheduledChange == nil {
		t.Fatal("ScheduledChange was not staged")
	}
	if len(got.ScheduledChange.NewTiers) != 2 {
		t.Errorf("new_tier_count=%d", len(got.ScheduledChange.NewTiers))
	}
	if got.ScheduledChange.NewPrecisionVersion != 2 {
		t.Errorf("NewPrecisionVersion=%d", got.ScheduledChange.NewPrecisionVersion)
	}
	if len(got.Tiers) != 1 {
		t.Errorf("current tiers should remain 1-deep until EffectiveAt: got %d", len(got.Tiers))
	}
	if got.PrecisionVersion != 1 {
		t.Errorf("PrecisionVersion should remain 1 until rollout: got %d", got.PrecisionVersion)
	}

	// Audit recorded.
	entries, _ := adminaudit.ReadAll(auditPath)
	if len(entries) != 1 || entries[0].Op != "admin.precision.schedule" {
		t.Fatalf("audit: %+v", entries)
	}
	if v, ok := entries[0].Params["new_tier_count"].(float64); !ok || int(v) != 2 {
		t.Errorf("audit new_tier_count=%v", entries[0].Params["new_tier_count"])
	}
}

func TestSchedulePrecision_MergeRejected(t *testing.T) {
	srv, mem, _, _ := newTestServer(t)
	// Seed with two tiers at boundary 1000.
	twoTier := []etcdcfg.PrecisionTier{
		{PriceFrom: dec.New("0"), PriceTo: dec.New("1000"), TickSize: dec.New("0.01"), StepSize: dec.New("0.00001")},
		{PriceFrom: dec.New("1000"), PriceTo: dec.New("0"), TickSize: dec.New("1"), StepSize: dec.New("0.0001")},
	}
	seedSymbolWithTiers(t, mem, "BTC-USDT", twoTier, 1)

	// Attempt to merge back to single tier — drops boundary 1000.
	effective, _ := time.Parse(time.RFC3339, "2026-05-01T00:00:00Z")
	body := etcdcfg.PrecisionChange{
		EffectiveAt:         effective,
		NewTiers:            singleTierFixture(),
		NewPrecisionVersion: 2,
	}
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("PUT", "/admin/symbols/BTC-USDT/precision", body))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "drop old boundary") {
		t.Errorf("error missing evolution reason: %s", rr.Body.String())
	}
	// No schedule should be staged.
	cfgs, _, _ := mem.List(context.Background())
	if cfgs["BTC-USDT"].ScheduledChange != nil {
		t.Error("ScheduledChange must not be staged on validation failure")
	}
}

func TestSchedulePrecision_WrongVersion(t *testing.T) {
	srv, mem, _, _ := newTestServer(t)
	seedSymbolWithTiers(t, mem, "BTC-USDT", singleTierFixture(), 5)

	effective, _ := time.Parse(time.RFC3339, "2026-05-01T00:00:00Z")
	body := etcdcfg.PrecisionChange{
		EffectiveAt:         effective,
		NewTiers:            singleTierFixture(),
		NewPrecisionVersion: 7, // want 6 (= 5 + 1)
	}
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("PUT", "/admin/symbols/BTC-USDT/precision", body))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "must equal current (5) + 1 = 6") {
		t.Errorf("error: %s", rr.Body.String())
	}
}

func TestSchedulePrecision_SymbolNotFound(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	effective, _ := time.Parse(time.RFC3339, "2026-05-01T00:00:00Z")
	body := etcdcfg.PrecisionChange{
		EffectiveAt: effective, NewTiers: singleTierFixture(), NewPrecisionVersion: 1,
	}
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("PUT", "/admin/symbols/NOPE-USDT/precision", body))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestSchedulePrecision_MissingEffectiveAt(t *testing.T) {
	srv, mem, _, _ := newTestServer(t)
	seedSymbolWithTiers(t, mem, "BTC-USDT", singleTierFixture(), 1)
	body := etcdcfg.PrecisionChange{
		// EffectiveAt zero
		NewTiers: singleTierFixture(), NewPrecisionVersion: 2,
	}
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("PUT", "/admin/symbols/BTC-USDT/precision", body))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "effective_at") {
		t.Errorf("error: %s", rr.Body.String())
	}
}

func TestCancelSchedulePrecision_ClearsPending(t *testing.T) {
	srv, mem, auditPath, _ := newTestServer(t)
	seedSymbolWithTiers(t, mem, "BTC-USDT", singleTierFixture(), 1)
	// Stage a valid change.
	effective, _ := time.Parse(time.RFC3339, "2026-05-01T00:00:00Z")
	split := []etcdcfg.PrecisionTier{
		{PriceFrom: dec.New("0"), PriceTo: dec.New("1000"), TickSize: dec.New("0.01"), StepSize: dec.New("0.00001")},
		{PriceFrom: dec.New("1000"), PriceTo: dec.New("0"), TickSize: dec.New("1"), StepSize: dec.New("0.0001")},
	}
	body := etcdcfg.PrecisionChange{EffectiveAt: effective, NewTiers: split, NewPrecisionVersion: 2}
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("PUT", "/admin/symbols/BTC-USDT/precision", body))
	if rr.Code != http.StatusOK {
		t.Fatalf("seed schedule: %d", rr.Code)
	}

	// Cancel.
	rr2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr2, adminReq("DELETE", "/admin/symbols/BTC-USDT/precision", nil))
	if rr2.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%s", rr2.Code, rr2.Body.String())
	}
	var out struct {
		HadScheduled bool `json:"had_scheduled"`
	}
	_ = json.Unmarshal(rr2.Body.Bytes(), &out)
	if !out.HadScheduled {
		t.Error("had_scheduled should be true after cancelling a staged change")
	}
	cfgs, _, _ := mem.List(context.Background())
	if cfgs["BTC-USDT"].ScheduledChange != nil {
		t.Error("ScheduledChange should be nil after cancel")
	}
	entries, _ := adminaudit.ReadAll(auditPath)
	if len(entries) != 2 || entries[1].Op != "admin.precision.cancel_schedule" {
		t.Errorf("audit: %+v", entries)
	}
}

func TestCancelSchedulePrecision_Idempotent(t *testing.T) {
	srv, mem, _, _ := newTestServer(t)
	seedSymbolWithTiers(t, mem, "BTC-USDT", singleTierFixture(), 1)
	// No pending change.
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("DELETE", "/admin/symbols/BTC-USDT/precision", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		HadScheduled bool `json:"had_scheduled"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if out.HadScheduled {
		t.Error("had_scheduled should be false when nothing was pending")
	}
}

func TestDeleteSymbol_IdempotentAndAudited(t *testing.T) {
	srv, mem, auditPath, _ := newTestServer(t)
	_, _ = mem.PutCtx(context.Background(), "ETH-USDT", etcdcfg.SymbolConfig{Shard: "match-1", Trading: true})

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("DELETE", "/admin/symbols/ETH-USDT", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("first delete: status=%d", rr.Code)
	}
	rr2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr2, adminReq("DELETE", "/admin/symbols/ETH-USDT", nil))
	if rr2.Code != http.StatusNotFound {
		t.Fatalf("second delete: status=%d", rr2.Code)
	}
	entries, _ := adminaudit.ReadAll(auditPath)
	if len(entries) != 2 {
		t.Fatalf("entries: %+v", entries)
	}
	if entries[0].Params["existed"] != true || entries[1].Params["existed"] != false {
		t.Fatalf("existed flags: %v %v", entries[0].Params["existed"], entries[1].Params["existed"])
	}
}

func TestListSymbols(t *testing.T) {
	srv, mem, _, _ := newTestServer(t)
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT", etcdcfg.SymbolConfig{Shard: "match-0", Trading: true})
	_, _ = mem.PutCtx(context.Background(), "ETH-USDT", etcdcfg.SymbolConfig{Shard: "match-1", Trading: false, Version: "v2"})

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("GET", "/admin/symbols", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Revision int64        `json:"revision"`
		Symbols  []symbolJSON `json:"symbols"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if len(out.Symbols) != 2 {
		t.Fatalf("symbols=%d", len(out.Symbols))
	}
}

func TestCancelOrders_RequiresFilter(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("POST", "/admin/cancel-orders", cancelOrdersBody{}))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestCancelOrders_PerUserHitsSingleShard(t *testing.T) {
	var s0Seen, s1Seen string
	srv, _, _, recs := newTestServer(t,
		func(req *counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error) {
			s0Seen = req.UserId
			return &counterrpc.AdminCancelOrdersResponse{Cancelled: 2, ShardId: 0}, nil
		},
		func(req *counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error) {
			s1Seen = req.UserId
			return &counterrpc.AdminCancelOrdersResponse{Cancelled: 2, ShardId: 1}, nil
		},
	)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("POST", "/admin/cancel-orders", cancelOrdersBody{UserID: "alice", Reason: "risk"}))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	hits := [2]int{recs[0].hits, recs[1].hits}
	if hits[0]+hits[1] != 1 {
		t.Fatalf("exactly one shard should be hit, got %+v", hits)
	}
	if s0Seen+s1Seen != "alice" {
		t.Fatalf("user propagated wrong: %q %q", s0Seen, s1Seen)
	}
}

func TestCancelOrders_BySymbolFansOutAndAggregates(t *testing.T) {
	srv, _, _, recs := newTestServer(t,
		func(_ *counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error) {
			return &counterrpc.AdminCancelOrdersResponse{Cancelled: 3, Skipped: 1, ShardId: 0}, nil
		},
		func(_ *counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error) {
			return &counterrpc.AdminCancelOrdersResponse{Cancelled: 4, Skipped: 0, ShardId: 1}, nil
		},
	)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("POST", "/admin/cancel-orders", cancelOrdersBody{Symbol: "BTC-USDT"}))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if recs[0].hits != 1 || recs[1].hits != 1 {
		t.Fatalf("broadcast miss: %d %d", recs[0].hits, recs[1].hits)
	}
	var out struct {
		Cancelled    uint32           `json:"cancelled"`
		Skipped      uint32           `json:"skipped"`
		ShardResults []map[string]any `json:"shard_results"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if out.Cancelled != 7 || out.Skipped != 1 || len(out.ShardResults) != 2 {
		t.Fatalf("out=%+v", out)
	}
}

func TestEtcdDisabledServes503(t *testing.T) {
	sc, _ := counterclient.NewSharded([]counterclient.Counter{&recordingCounter{}})
	srv, err := New(Config{Counter: sc, Audit: adminaudit.NopLogger{}, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, adminReq("GET", "/admin/symbols", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rr.Code)
	}
}

func TestHealthzPublic(t *testing.T) {
	srv, _, _, _ := newTestServer(t)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, httptest.NewRequest("GET", "/admin/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
}
