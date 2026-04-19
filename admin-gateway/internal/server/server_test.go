package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/admin-gateway/internal/counterclient"
	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/auth"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// recordingCounter captures AdminCancelOrders calls.
type recordingCounter struct {
	fn   func(*counterrpc.AdminCancelOrdersRequest) (*counterrpc.AdminCancelOrdersResponse, error)
	hits int
}

func (r *recordingCounter) AdminCancelOrders(_ context.Context, req *counterrpc.AdminCancelOrdersRequest, _ ...grpc.CallOption) (*counterrpc.AdminCancelOrdersResponse, error) {
	r.hits++
	if r.fn == nil {
		return &counterrpc.AdminCancelOrdersResponse{}, nil
	}
	return r.fn(req)
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
