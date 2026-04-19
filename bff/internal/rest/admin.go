package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/bff/internal/auth"
	"github.com/xargin/opentrade/bff/internal/client"
	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// AdminServer hosts /admin/* endpoints (ADR-0052). Construction is
// independent from Server so admin concerns don't leak into the user plane.
type AdminServer struct {
	counter client.Counter
	etcd    AdminEtcdSource // nil when --etcd is empty; symbol endpoints 503
	audit   adminaudit.Logger
	logger  *zap.Logger

	// Admin ops touch etcd + Kafka via gRPC; 5s is plenty in a healthy
	// cluster and we don't want to wedge on a dead shard.
	requestTimeout time.Duration
}

// AdminEtcdSource is the subset of etcdcfg we need for admin CRUD. It combines
// Writer (Put/Delete) with List (so GET can return the current state).
// etcdcfg.EtcdSource and etcdcfg.MemorySource both satisfy it.
type AdminEtcdSource interface {
	List(ctx context.Context) (map[string]etcdcfg.SymbolConfig, int64, error)
	Put(ctx context.Context, symbol string, cfg etcdcfg.SymbolConfig) (int64, error)
	Delete(ctx context.Context, symbol string) (bool, int64, error)
}

// memorySourceAdminShim adapts *etcdcfg.MemorySource (which keeps its
// pre-ctx Put/Delete helpers for tests) to the AdminEtcdSource interface.
type memorySourceAdminShim struct{ s *etcdcfg.MemorySource }

// NewMemoryAdminShim wraps a MemorySource so BFF admin handler tests can
// share a single Source instance across the admin plane and any watchers.
func NewMemoryAdminShim(s *etcdcfg.MemorySource) AdminEtcdSource { return memorySourceAdminShim{s: s} }

func (m memorySourceAdminShim) List(ctx context.Context) (map[string]etcdcfg.SymbolConfig, int64, error) {
	return m.s.List(ctx)
}
func (m memorySourceAdminShim) Put(ctx context.Context, symbol string, cfg etcdcfg.SymbolConfig) (int64, error) {
	return m.s.PutCtx(ctx, symbol, cfg)
}
func (m memorySourceAdminShim) Delete(ctx context.Context, symbol string) (bool, int64, error) {
	return m.s.DeleteCtx(ctx, symbol)
}

// NewAdminServer wires an AdminServer. counter is required (used for
// /admin/cancel-orders). etcd may be nil — then /admin/symbols returns
// 503. audit is required: admin-plane calls must always be auditable.
func NewAdminServer(counter client.Counter, etcd AdminEtcdSource, audit adminaudit.Logger, logger *zap.Logger) *AdminServer {
	if audit == nil {
		audit = adminaudit.NopLogger{}
	}
	return &AdminServer{
		counter:        counter,
		etcd:           etcd,
		audit:          audit,
		logger:         logger,
		requestTimeout: 5 * time.Second,
	}
}

// Handler mounts the admin routes. The returned handler expects the caller
// to wrap it with auth middleware + RequireAdmin externally (so we don't
// duplicate the middleware chain wiring from main.go).
func (s *AdminServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /admin/symbols", s.handleListSymbols)
	mux.HandleFunc("GET /admin/symbols/{symbol}", s.handleGetSymbol)
	mux.HandleFunc("PUT /admin/symbols/{symbol}", s.handlePutSymbol)
	mux.HandleFunc("DELETE /admin/symbols/{symbol}", s.handleDeleteSymbol)
	mux.HandleFunc("POST /admin/cancel-orders", s.handleCancelOrders)
	return mux
}

// ---------------------------------------------------------------------------
// Symbol CRUD
// ---------------------------------------------------------------------------

type symbolJSON struct {
	Symbol  string `json:"symbol"`
	Shard   string `json:"shard"`
	Trading bool   `json:"trading"`
	Version string `json:"version,omitempty"`
}

type putSymbolBody struct {
	Shard   string `json:"shard"`
	Trading bool   `json:"trading"`
	Version string `json:"version,omitempty"`
}

func (s *AdminServer) handleListSymbols(w http.ResponseWriter, r *http.Request) {
	if s.etcd == nil {
		writeError(w, http.StatusServiceUnavailable, "etcd not configured")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	cfgs, rev, err := s.etcd.List(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	out := make([]symbolJSON, 0, len(cfgs))
	for sym, c := range cfgs {
		out = append(out, symbolJSON{Symbol: sym, Shard: c.Shard, Trading: c.Trading, Version: c.Version})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"revision": rev,
		"symbols":  out,
	})
}

func (s *AdminServer) handleGetSymbol(w http.ResponseWriter, r *http.Request) {
	if s.etcd == nil {
		writeError(w, http.StatusServiceUnavailable, "etcd not configured")
		return
	}
	symbol := r.PathValue("symbol")
	if err := etcdcfg.ValidateSymbol(symbol); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	cfgs, rev, err := s.etcd.List(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	c, ok := cfgs[symbol]
	if !ok {
		writeError(w, http.StatusNotFound, "symbol not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"revision": rev,
		"symbol":   symbolJSON{Symbol: symbol, Shard: c.Shard, Trading: c.Trading, Version: c.Version},
	})
}

func (s *AdminServer) handlePutSymbol(w http.ResponseWriter, r *http.Request) {
	if s.etcd == nil {
		writeError(w, http.StatusServiceUnavailable, "etcd not configured")
		return
	}
	symbol := r.PathValue("symbol")
	if err := etcdcfg.ValidateSymbol(symbol); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body putSymbolBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.Shard == "" {
		writeError(w, http.StatusBadRequest, "shard is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	rev, putErr := s.etcd.Put(ctx, symbol, etcdcfg.SymbolConfig{
		Shard: body.Shard, Trading: body.Trading, Version: body.Version,
	})
	params := map[string]any{"shard": body.Shard, "trading": body.Trading}
	if body.Version != "" {
		params["version"] = body.Version
	}
	if err := s.writeAudit(r, adminaudit.Entry{
		Op:     "admin.symbol.put",
		Target: symbol,
		Params: params,
		Status: statusFromErr(putErr),
		Error:  errString(putErr),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("audit write failed: %v (symbol write err: %v)", err, putErr))
		return
	}
	if putErr != nil {
		writeError(w, http.StatusBadGateway, putErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": rev, "symbol": symbol})
}

func (s *AdminServer) handleDeleteSymbol(w http.ResponseWriter, r *http.Request) {
	if s.etcd == nil {
		writeError(w, http.StatusServiceUnavailable, "etcd not configured")
		return
	}
	symbol := r.PathValue("symbol")
	if err := etcdcfg.ValidateSymbol(symbol); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	existed, rev, delErr := s.etcd.Delete(ctx, symbol)
	if err := s.writeAudit(r, adminaudit.Entry{
		Op:     "admin.symbol.delete",
		Target: symbol,
		Params: map[string]any{"existed": existed},
		Status: statusFromErr(delErr),
		Error:  errString(delErr),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("audit write failed: %v (symbol delete err: %v)", err, delErr))
		return
	}
	if delErr != nil {
		writeError(w, http.StatusBadGateway, delErr.Error())
		return
	}
	if !existed {
		writeJSON(w, http.StatusNotFound, map[string]any{"revision": rev, "existed": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": rev, "existed": true})
}

// ---------------------------------------------------------------------------
// Batch cancel
// ---------------------------------------------------------------------------

type cancelOrdersBody struct {
	UserID string `json:"user_id,omitempty"`
	Symbol string `json:"symbol,omitempty"`
	Reason string `json:"reason,omitempty"`
}

func (s *AdminServer) handleCancelOrders(w http.ResponseWriter, r *http.Request) {
	var body cancelOrdersBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.UserID == "" && body.Symbol == "" {
		writeError(w, http.StatusBadRequest, "user_id or symbol required")
		return
	}

	sharded, isSharded := s.counter.(*client.ShardedCounter)
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	var (
		totalCancelled uint32
		totalSkipped   uint32
		shardResults   []map[string]any
		rpcErr         error
	)

	if body.UserID == "" && isSharded {
		// Symbol-only fan-out: show per-shard counts to the caller.
		results, err := sharded.BroadcastAdminCancelOrders(ctx, &counterrpc.AdminCancelOrdersRequest{
			UserId: body.UserID,
			Symbol: body.Symbol,
			Reason: body.Reason,
		})
		rpcErr = err
		for i, r := range results {
			if r == nil {
				shardResults = append(shardResults, map[string]any{
					"shard_id": i, "error": shardErr(rpcErr, i, results),
				})
				continue
			}
			totalCancelled += r.Cancelled
			totalSkipped += r.Skipped
			shardResults = append(shardResults, map[string]any{
				"shard_id": r.ShardId, "cancelled": r.Cancelled, "skipped": r.Skipped,
			})
		}
	} else {
		resp, err := s.counter.AdminCancelOrders(ctx, &counterrpc.AdminCancelOrdersRequest{
			UserId: body.UserID,
			Symbol: body.Symbol,
			Reason: body.Reason,
		})
		rpcErr = err
		if resp != nil {
			totalCancelled = resp.Cancelled
			totalSkipped = resp.Skipped
			shardResults = append(shardResults, map[string]any{
				"shard_id": resp.ShardId, "cancelled": resp.Cancelled, "skipped": resp.Skipped,
			})
		}
	}

	params := map[string]any{
		"cancelled": totalCancelled,
		"skipped":   totalSkipped,
	}
	if body.UserID != "" {
		params["user_id"] = body.UserID
	}
	if body.Symbol != "" {
		params["symbol"] = body.Symbol
	}
	if body.Reason != "" {
		params["reason"] = body.Reason
	}
	if err := s.writeAudit(r, adminaudit.Entry{
		Op:     "admin.cancel-orders",
		Target: adminCancelTarget(body),
		Params: params,
		Status: statusFromErr(rpcErr),
		Error:  errString(rpcErr),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("audit write failed: %v (rpc err: %v)", err, rpcErr))
		return
	}
	if rpcErr != nil {
		writeGRPCError(w, rpcErr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cancelled":     totalCancelled,
		"skipped":       totalSkipped,
		"shard_results": shardResults,
	})
}

// ---------------------------------------------------------------------------
// audit helpers
// ---------------------------------------------------------------------------

func (s *AdminServer) writeAudit(r *http.Request, e adminaudit.Entry) error {
	if e.AdminID == "" {
		if uid, err := auth.UserID(r.Context()); err == nil {
			e.AdminID = uid
		}
	}
	if e.RemoteIP == "" {
		e.RemoteIP = clientIP(r)
	}
	if reqID := r.Header.Get("X-Request-Id"); reqID != "" {
		e.RequestID = reqID
	}
	return s.audit.Log(e)
}

func statusFromErr(err error) string {
	if err == nil {
		return adminaudit.StatusOK
	}
	return adminaudit.StatusFailed
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// shardErr surfaces the aggregated error onto the specific shard slot that
// failed. For MVP all that matters is "this shard errored" — we don't try
// to demultiplex which shard produced which error beyond that.
func shardErr(agg error, i int, results []*counterrpc.AdminCancelOrdersResponse) string {
	if agg == nil {
		return ""
	}
	if i < len(results) && results[i] != nil {
		return ""
	}
	return agg.Error()
}

func adminCancelTarget(b cancelOrdersBody) string {
	switch {
	case b.UserID != "" && b.Symbol != "":
		return b.UserID + "|" + b.Symbol
	case b.UserID != "":
		return b.UserID
	default:
		return b.Symbol
	}
}

