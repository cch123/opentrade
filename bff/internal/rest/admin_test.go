package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/bff/internal/auth"
	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// newAdminTestServer builds an AdminServer backed by an in-memory etcd
// source, a JSONL audit logger in a temp directory, and the provided
// Counter (which may be nil when only symbol endpoints are exercised).
func newAdminTestServer(t *testing.T, counter adminTestCounter) (*AdminServer, *etcdcfg.MemorySource, string) {
	t.Helper()
	mem := etcdcfg.NewMemorySource()
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	audit, err := adminaudit.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = audit.Close() })
	srv := NewAdminServer(counter, NewMemoryAdminShim(mem), audit, zap.NewNop())
	return srv, mem, auditPath
}

// adminTestCounter is a minimal Counter impl for admin tests. It records
// every AdminCancelOrders call so the handler test can assert dispatch.
type adminTestCounter struct {
	adminCancelFn func(*counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error)
}

func (c adminTestCounter) PlaceOrder(context.Context, *counterrpc.PlaceOrderRequest, ...grpc.CallOption) (*counterrpc.PlaceOrderResponse, error) {
	return nil, nil
}
func (c adminTestCounter) CancelOrder(context.Context, *counterrpc.CancelOrderRequest, ...grpc.CallOption) (*counterrpc.CancelOrderResponse, error) {
	return nil, nil
}
func (c adminTestCounter) QueryOrder(context.Context, *counterrpc.QueryOrderRequest, ...grpc.CallOption) (*counterrpc.QueryOrderResponse, error) {
	return nil, nil
}
func (c adminTestCounter) Transfer(context.Context, *counterrpc.TransferRequest, ...grpc.CallOption) (*counterrpc.TransferResponse, error) {
	return nil, nil
}
func (c adminTestCounter) QueryBalance(context.Context, *counterrpc.QueryBalanceRequest, ...grpc.CallOption) (*counterrpc.QueryBalanceResponse, error) {
	return nil, nil
}
func (c adminTestCounter) AdminCancelOrders(_ context.Context, req *counterrpc.AdminCancelOrdersRequest, _ ...grpc.CallOption) (*counterrpc.AdminCancelOrdersResponse, error) {
	if c.adminCancelFn == nil {
		return &counterrpc.AdminCancelOrdersResponse{Cancelled: 0}, nil
	}
	return c.adminCancelFn(req)
}

// adminRequest builds a request already carrying an admin role in the
// context (simulating the RequireAdmin / auth middleware that would
// normally run ahead of the handler).
func adminRequest(method, url string, body any) *http.Request {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	r := httptest.NewRequest(method, url, bytes.NewReader(b))
	ctx := auth.WithUserID(r.Context(), "ops-bot")
	ctx = auth.WithRole(ctx, auth.RoleAdmin)
	return r.WithContext(ctx)
}

func TestAdmin_PutSymbolWritesEtcdAndAudit(t *testing.T) {
	srv, mem, auditPath := newAdminTestServer(t, adminTestCounter{})
	h := srv.Handler()

	req := adminRequest("PUT", "/admin/symbols/BTC-USDT", putSymbolBody{Shard: "match-0", Trading: true, Version: "v1"})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}

	cfgs, _, _ := mem.List(context.Background())
	got, ok := cfgs["BTC-USDT"]
	if !ok || got.Shard != "match-0" || !got.Trading || got.Version != "v1" {
		t.Fatalf("etcd state: %+v", cfgs)
	}
	entries, err := adminaudit.ReadAll(auditPath)
	if err != nil || len(entries) != 1 {
		t.Fatalf("audit entries: %v %v", entries, err)
	}
	e := entries[0]
	if e.Op != "admin.symbol.put" || e.Target != "BTC-USDT" || e.Status != adminaudit.StatusOK || e.AdminID != "ops-bot" {
		t.Fatalf("entry = %+v", e)
	}
	if e.Params["shard"] != "match-0" {
		t.Fatalf("params = %+v", e.Params)
	}
}

func TestAdmin_PutSymbolRejectsMissingShard(t *testing.T) {
	srv, _, _ := newAdminTestServer(t, adminTestCounter{})
	h := srv.Handler()
	req := adminRequest("PUT", "/admin/symbols/BTC-USDT", putSymbolBody{Trading: true})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
}

func TestAdmin_DeleteSymbol(t *testing.T) {
	srv, mem, auditPath := newAdminTestServer(t, adminTestCounter{})
	h := srv.Handler()
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT", etcdcfg.SymbolConfig{Shard: "match-0", Trading: true})

	req := adminRequest("DELETE", "/admin/symbols/BTC-USDT", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
	cfgs, _, _ := mem.List(context.Background())
	if _, ok := cfgs["BTC-USDT"]; ok {
		t.Fatalf("still present: %+v", cfgs)
	}

	// Second delete returns 404 but still audits.
	req2 := adminRequest("DELETE", "/admin/symbols/BTC-USDT", nil)
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusNotFound {
		t.Fatalf("second delete: status = %d", rr2.Code)
	}

	entries, _ := adminaudit.ReadAll(auditPath)
	if len(entries) != 2 {
		t.Fatalf("audit entries: %+v", entries)
	}
	if entries[0].Params["existed"] != true {
		t.Fatalf("entry0 existed: %+v", entries[0])
	}
	if entries[1].Params["existed"] != false {
		t.Fatalf("entry1 existed: %+v", entries[1])
	}
}

func TestAdmin_ListSymbols(t *testing.T) {
	srv, mem, _ := newAdminTestServer(t, adminTestCounter{})
	h := srv.Handler()
	_, _ = mem.PutCtx(context.Background(), "BTC-USDT", etcdcfg.SymbolConfig{Shard: "match-0", Trading: true})
	_, _ = mem.PutCtx(context.Background(), "ETH-USDT", etcdcfg.SymbolConfig{Shard: "match-1", Trading: false, Version: "v2"})

	req := adminRequest("GET", "/admin/symbols", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
	var out struct {
		Revision int64        `json:"revision"`
		Symbols  []symbolJSON `json:"symbols"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Symbols) != 2 {
		t.Fatalf("symbols len = %d", len(out.Symbols))
	}
}

func TestAdmin_EtcdDisabled503s(t *testing.T) {
	srv := NewAdminServer(adminTestCounter{}, nil,
		adminaudit.NopLogger{}, zap.NewNop())
	h := srv.Handler()
	req := adminRequest("GET", "/admin/symbols", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestAdmin_CancelOrdersRequiresFilter(t *testing.T) {
	srv, _, _ := newAdminTestServer(t, adminTestCounter{})
	h := srv.Handler()
	req := adminRequest("POST", "/admin/cancel-orders", cancelOrdersBody{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
}

func TestAdmin_CancelOrdersPerUser(t *testing.T) {
	var capturedUser, capturedSymbol, capturedReason string
	c := adminTestCounter{
		adminCancelFn: func(req *counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error) {
			capturedUser = req.UserId
			capturedSymbol = req.Symbol
			capturedReason = req.Reason
			return &counterrpc.AdminCancelOrdersResponse{Cancelled: 3, Skipped: 1, ShardId: 2}, nil
		},
	}
	srv, _, auditPath := newAdminTestServer(t, c)
	h := srv.Handler()

	req := adminRequest("POST", "/admin/cancel-orders", cancelOrdersBody{UserID: "alice", Symbol: "BTC-USDT", Reason: "risk"})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
	if capturedUser != "alice" || capturedSymbol != "BTC-USDT" || capturedReason != "risk" {
		t.Fatalf("req = user=%q symbol=%q reason=%q", capturedUser, capturedSymbol, capturedReason)
	}
	var out struct {
		Cancelled    uint32           `json:"cancelled"`
		Skipped      uint32           `json:"skipped"`
		ShardResults []map[string]any `json:"shard_results"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if out.Cancelled != 3 || out.Skipped != 1 {
		t.Fatalf("out = %+v", out)
	}
	if len(out.ShardResults) != 1 {
		t.Fatalf("shard results: %+v", out.ShardResults)
	}

	entries, _ := adminaudit.ReadAll(auditPath)
	if len(entries) != 1 || entries[0].Op != "admin.cancel-orders" || entries[0].AdminID != "ops-bot" {
		t.Fatalf("audit = %+v", entries)
	}
}

// Ensure tests don't leak to disk after t.TempDir cleans.
func TestAdmin_AuditPathCleansUp(t *testing.T) {
	_, _, path := newAdminTestServer(t, adminTestCounter{})
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("audit file should exist during test: %v", err)
	}
}
