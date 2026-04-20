// Package server hosts admin-gateway's /admin/* HTTP handlers (ADR-0052).
//
// admin-gateway is a separate process from BFF — ops / internal plane,
// not 2C. It dials the same Counter shards as BFF but only calls the
// admin-only RPCs; symbol lifecycle (CRUD) goes straight to etcd under
// the match shard-config key prefix. Every mutating call is recorded to
// the audit log *before* it returns, so the JSONL file is a strict
// prefix of committed state.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/admin-gateway/internal/counterclient"
	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/auth"
	"github.com/xargin/opentrade/pkg/etcdcfg"
	"github.com/xargin/opentrade/pkg/shard"
)

// EtcdSource is the subset of etcdcfg we need for admin CRUD (Writer +
// List). etcdcfg.EtcdSource / MemorySource both satisfy via the shim below.
type EtcdSource interface {
	List(ctx context.Context) (map[string]etcdcfg.SymbolConfig, int64, error)
	Put(ctx context.Context, symbol string, cfg etcdcfg.SymbolConfig) (int64, error)
	Delete(ctx context.Context, symbol string) (bool, int64, error)
}

// NewMemoryShim wraps etcdcfg.MemorySource so tests can share a single
// source across the admin plane and match-side watchers.
func NewMemoryShim(s *etcdcfg.MemorySource) EtcdSource { return memoryShim{s: s} }

type memoryShim struct{ s *etcdcfg.MemorySource }

func (m memoryShim) List(ctx context.Context) (map[string]etcdcfg.SymbolConfig, int64, error) {
	return m.s.List(ctx)
}
func (m memoryShim) Put(ctx context.Context, symbol string, cfg etcdcfg.SymbolConfig) (int64, error) {
	return m.s.PutCtx(ctx, symbol, cfg)
}
func (m memoryShim) Delete(ctx context.Context, symbol string) (bool, int64, error) {
	return m.s.DeleteCtx(ctx, symbol)
}

// Server is the admin-plane HTTP handler set.
type Server struct {
	shardedCounter *counterclient.Sharded
	etcd           EtcdSource
	audit          adminaudit.Logger
	logger         *zap.Logger
	requestTimeout time.Duration
}

// Config bundles dependencies.
type Config struct {
	Counter        *counterclient.Sharded // required for /admin/cancel-orders
	Etcd           EtcdSource             // optional; nil → /admin/symbols 503
	Audit          adminaudit.Logger      // required; NopLogger accepted
	Logger         *zap.Logger
	RequestTimeout time.Duration // default 5s
}

// New wires a Server. Counter and Audit are required; missing Etcd
// surfaces 503 on /admin/symbols without blocking the process.
func New(cfg Config) (*Server, error) {
	if cfg.Counter == nil {
		return nil, fmt.Errorf("admin server: counter is required")
	}
	if cfg.Audit == nil {
		cfg.Audit = adminaudit.NopLogger{}
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	return &Server{
		shardedCounter: cfg.Counter,
		etcd:           cfg.Etcd,
		audit:          cfg.Audit,
		logger:         cfg.Logger,
		requestTimeout: cfg.RequestTimeout,
	}, nil
}

// Handler mounts admin routes. The caller is expected to wrap the
// returned handler with auth.AdminMiddleware(store, logger) +
// auth.RequireAdmin at the outer layer so every route is gated on
// role=admin API-Key. /admin/healthz is a special case — mount it
// outside the RequireAdmin chain if you want public readiness probes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /admin/symbols", s.handleListSymbols)
	mux.HandleFunc("GET /admin/symbols/{symbol}", s.handleGetSymbol)
	mux.HandleFunc("PUT /admin/symbols/{symbol}", s.handlePutSymbol)
	mux.HandleFunc("DELETE /admin/symbols/{symbol}", s.handleDeleteSymbol)
	// ADR-0053 M1.b: precision rollout protocol — schedule a tier
	// migration that takes effect at a future EffectiveAt. Separate from
	// the bulk PUT /admin/symbols/{symbol} so evolution (vs first install)
	// can enforce ValidateTierEvolution.
	mux.HandleFunc("PUT /admin/symbols/{symbol}/precision", s.handleSchedulePrecision)
	mux.HandleFunc("DELETE /admin/symbols/{symbol}/precision", s.handleCancelSchedulePrecision)
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

	// Precision block (ADR-0053). Omitted entirely when the symbol has no
	// tiers — legacy clients see no new fields.
	BaseAsset        string                    `json:"base_asset,omitempty"`
	QuoteAsset       string                    `json:"quote_asset,omitempty"`
	PrecisionVersion uint64                    `json:"precision_version,omitempty"`
	Tiers            []etcdcfg.PrecisionTier   `json:"tiers,omitempty"`
	ScheduledChange  *etcdcfg.PrecisionChange  `json:"scheduled_change,omitempty"`
}

func symbolJSONFrom(symbol string, c etcdcfg.SymbolConfig) symbolJSON {
	return symbolJSON{
		Symbol:           symbol,
		Shard:            c.Shard,
		Trading:          c.Trading,
		Version:          c.Version,
		BaseAsset:        c.BaseAsset,
		QuoteAsset:       c.QuoteAsset,
		PrecisionVersion: c.PrecisionVersion,
		Tiers:            c.Tiers,
		ScheduledChange:  c.ScheduledChange,
	}
}

// putSymbolBody is the PUT /admin/symbols/{symbol} request body. Precision
// fields (ADR-0053) are optional — legacy PUTs that only send shard /
// trading / version continue to work untouched (M0 compatibility).
type putSymbolBody struct {
	Shard   string `json:"shard"`
	Trading bool   `json:"trading"`
	Version string `json:"version,omitempty"`

	BaseAsset        string                   `json:"base_asset,omitempty"`
	QuoteAsset       string                   `json:"quote_asset,omitempty"`
	PrecisionVersion uint64                   `json:"precision_version,omitempty"`
	Tiers            []etcdcfg.PrecisionTier  `json:"tiers,omitempty"`
	ScheduledChange  *etcdcfg.PrecisionChange `json:"scheduled_change,omitempty"`

	// ADR-0054 order slot limits. Zero = use service default.
	MaxOpenLimitOrders         uint32 `json:"max_open_limit_orders,omitempty"`
	MaxActiveConditionalOrders uint32 `json:"max_active_conditional_orders,omitempty"`
}

// maxOrderSlotUpper caps the two ADR-0054 slot fields. 10_000 is several
// orders of magnitude above any realistic MM deployment and guards against
// typos (100_000 instead of 100).
const maxOrderSlotUpper = 10_000

func (s *Server) handleListSymbols(w http.ResponseWriter, r *http.Request) {
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
		out = append(out, symbolJSONFrom(sym, c))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"revision": rev,
		"symbols":  out,
	})
}

func (s *Server) handleGetSymbol(w http.ResponseWriter, r *http.Request) {
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
		"symbol":   symbolJSONFrom(symbol, c),
	})
}

func (s *Server) handlePutSymbol(w http.ResponseWriter, r *http.Request) {
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

	// ADR-0053 M1.a: validate precision payload before touching etcd. Tiers
	// empty = compatibility mode, skip. ScheduledChange's NewTiers must
	// also be self-consistent; evolution (vs. existing Tiers) is enforced
	// by M1.b via the dedicated /precision endpoint, not here — this
	// endpoint is for first install / bulk backfill (M2) and fully trusts
	// the admin's input for tier shape, only structural validity.
	if len(body.Tiers) > 0 {
		if err := etcdcfg.ValidateTiers(body.Tiers); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if body.ScheduledChange != nil {
		if err := etcdcfg.ValidateTiers(body.ScheduledChange.NewTiers); err != nil {
			writeError(w, http.StatusBadRequest, "scheduled_change: "+err.Error())
			return
		}
		if body.ScheduledChange.NewPrecisionVersion == 0 {
			writeError(w, http.StatusBadRequest, "scheduled_change.new_precision_version must be > 0")
			return
		}
	}
	// ADR-0054 order slot sanity: non-zero fields must fit in a reasonable
	// range. Zero means "use service default".
	if body.MaxOpenLimitOrders > maxOrderSlotUpper {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("max_open_limit_orders must be <= %d", maxOrderSlotUpper))
		return
	}
	if body.MaxActiveConditionalOrders > maxOrderSlotUpper {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("max_active_conditional_orders must be <= %d", maxOrderSlotUpper))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	rev, putErr := s.etcd.Put(ctx, symbol, etcdcfg.SymbolConfig{
		Shard:                      body.Shard,
		Trading:                    body.Trading,
		Version:                    body.Version,
		BaseAsset:                  body.BaseAsset,
		QuoteAsset:                 body.QuoteAsset,
		PrecisionVersion:           body.PrecisionVersion,
		Tiers:                      body.Tiers,
		ScheduledChange:            body.ScheduledChange,
		MaxOpenLimitOrders:         body.MaxOpenLimitOrders,
		MaxActiveConditionalOrders: body.MaxActiveConditionalOrders,
	})
	params := map[string]any{"shard": body.Shard, "trading": body.Trading}
	if body.Version != "" {
		params["version"] = body.Version
	}
	if body.BaseAsset != "" {
		params["base_asset"] = body.BaseAsset
	}
	if body.QuoteAsset != "" {
		params["quote_asset"] = body.QuoteAsset
	}
	if body.PrecisionVersion != 0 {
		params["precision_version"] = body.PrecisionVersion
	}
	if n := len(body.Tiers); n > 0 {
		params["tier_count"] = n
	}
	if body.ScheduledChange != nil {
		params["scheduled_change"] = map[string]any{
			"effective_at":          body.ScheduledChange.EffectiveAt.Format(time.RFC3339),
			"new_precision_version": body.ScheduledChange.NewPrecisionVersion,
			"new_tier_count":        len(body.ScheduledChange.NewTiers),
			"reason":                body.ScheduledChange.Reason,
		}
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
	writeJSON(w, http.StatusOK, map[string]any{
		"revision":          rev,
		"symbol":            symbol,
		"precision_version": body.PrecisionVersion,
	})
}

func (s *Server) handleDeleteSymbol(w http.ResponseWriter, r *http.Request) {
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
// Precision rollout (ADR-0053 M1.b)
// ---------------------------------------------------------------------------

// handleSchedulePrecision stages a PrecisionChange on an existing symbol.
// Unlike PUT /admin/symbols/{symbol} (which does a blanket replace and is
// appropriate for first install / M2 backfill), this endpoint enforces
// ValidateTierEvolution against the current Tiers — merges / deletes /
// boundary shifts are rejected here.
//
// Body: etcdcfg.PrecisionChange JSON.
// Success: 200 with the stored PrecisionChange echo. The change is visible
// immediately in GET /admin/symbols/{symbol}.ScheduledChange; match /
// counter watchers are responsible for enacting the swap at EffectiveAt
// (out of scope for M1.b).
func (s *Server) handleSchedulePrecision(w http.ResponseWriter, r *http.Request) {
	if s.etcd == nil {
		writeError(w, http.StatusServiceUnavailable, "etcd not configured")
		return
	}
	symbol := r.PathValue("symbol")
	if err := etcdcfg.ValidateSymbol(symbol); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var change etcdcfg.PrecisionChange
	if err := readJSON(r, &change); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Fetch current cfg — schedule is only valid against an existing symbol.
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	cfgs, _, err := s.etcd.List(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	current, ok := cfgs[symbol]
	if !ok {
		writeError(w, http.StatusNotFound, "symbol not found — use PUT /admin/symbols/{symbol} for first install")
		return
	}

	// Validate the change payload itself.
	if err := etcdcfg.ValidateTiers(change.NewTiers); err != nil {
		writeError(w, http.StatusBadRequest, "new_tiers: "+err.Error())
		return
	}
	// Evolution rule: only subdivide / append. Merges & deletes rejected.
	if err := etcdcfg.ValidateTierEvolution(current.Tiers, change.NewTiers); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Version must strictly bump by 1 — idempotency key for the rollout.
	wantVer := current.PrecisionVersion + 1
	if change.NewPrecisionVersion != wantVer {
		writeError(w, http.StatusBadRequest, fmt.Sprintf(
			"new_precision_version must equal current (%d) + 1 = %d, got %d",
			current.PrecisionVersion, wantVer, change.NewPrecisionVersion))
		return
	}
	if change.EffectiveAt.IsZero() {
		writeError(w, http.StatusBadRequest, "effective_at is required")
		return
	}

	// Stage the change onto the existing cfg and write back.
	changeCopy := change
	current.ScheduledChange = &changeCopy
	rev, putErr := s.etcd.Put(ctx, symbol, current)

	params := map[string]any{
		"effective_at":          change.EffectiveAt.Format(time.RFC3339),
		"new_precision_version": change.NewPrecisionVersion,
		"new_tier_count":        len(change.NewTiers),
	}
	if change.Reason != "" {
		params["reason"] = change.Reason
	}
	if err := s.writeAudit(r, adminaudit.Entry{
		Op:     "admin.precision.schedule",
		Target: symbol,
		Params: params,
		Status: statusFromErr(putErr),
		Error:  errString(putErr),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("audit write failed: %v (put err: %v)", err, putErr))
		return
	}
	if putErr != nil {
		writeError(w, http.StatusBadGateway, putErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"revision":         rev,
		"symbol":           symbol,
		"scheduled_change": change,
	})
}

// handleCancelSchedulePrecision clears a pending PrecisionChange. Idempotent:
// returns 200 with scheduled=false whether or not a change was pending.
func (s *Server) handleCancelSchedulePrecision(w http.ResponseWriter, r *http.Request) {
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
	cfgs, _, err := s.etcd.List(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	current, ok := cfgs[symbol]
	if !ok {
		writeError(w, http.StatusNotFound, "symbol not found")
		return
	}
	hadChange := current.ScheduledChange != nil
	current.ScheduledChange = nil
	rev, putErr := s.etcd.Put(ctx, symbol, current)

	if err := s.writeAudit(r, adminaudit.Entry{
		Op:     "admin.precision.cancel_schedule",
		Target: symbol,
		Params: map[string]any{"had_scheduled_change": hadChange},
		Status: statusFromErr(putErr),
		Error:  errString(putErr),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("audit write failed: %v (put err: %v)", err, putErr))
		return
	}
	if putErr != nil {
		writeError(w, http.StatusBadGateway, putErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"revision":         rev,
		"symbol":           symbol,
		"had_scheduled":    hadChange,
	})
}

// ---------------------------------------------------------------------------
// Batch cancel
// ---------------------------------------------------------------------------

type cancelOrdersBody struct {
	UserID string `json:"user_id,omitempty"`
	Symbol string `json:"symbol,omitempty"`
	Reason string `json:"reason,omitempty"`
}

func (s *Server) handleCancelOrders(w http.ResponseWriter, r *http.Request) {
	var body cancelOrdersBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.UserID == "" && body.Symbol == "" {
		writeError(w, http.StatusBadRequest, "user_id or symbol required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	var (
		totalCancelled uint32
		totalSkipped   uint32
		shardResults   []map[string]any
		rpcErr         error
	)

	req := &counterrpc.AdminCancelOrdersRequest{
		UserId: body.UserID,
		Symbol: body.Symbol,
		Reason: body.Reason,
	}

	if body.UserID != "" {
		// Single shard: xxhash-route the user (same fn BFF uses).
		shardID := shard.Index(body.UserID, s.shardedCounter.Shards())
		resp, err := s.shardedCounter.Shard(shardID).AdminCancelOrders(ctx, req)
		rpcErr = err
		if resp != nil {
			totalCancelled = resp.Cancelled
			totalSkipped = resp.Skipped
			shardResults = append(shardResults, map[string]any{
				"shard_id": resp.ShardId, "cancelled": resp.Cancelled, "skipped": resp.Skipped,
			})
		}
	} else {
		// Symbol-only: fan-out, per-shard counts visible to caller.
		results, err := s.shardedCounter.BroadcastAdminCancelOrders(ctx, req)
		rpcErr = err
		for i, r := range results {
			if r == nil {
				shardResults = append(shardResults, map[string]any{
					"shard_id": i, "error": shardErrString(rpcErr, i, results),
				})
				continue
			}
			totalCancelled += r.Cancelled
			totalSkipped += r.Skipped
			shardResults = append(shardResults, map[string]any{
				"shard_id": r.ShardId, "cancelled": r.Cancelled, "skipped": r.Skipped,
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
		Target: cancelTarget(body),
		Params: params,
		Status: statusFromErr(rpcErr),
		Error:  errString(rpcErr),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("audit write failed: %v (rpc err: %v)", err, rpcErr))
		return
	}
	if rpcErr != nil {
		writeError(w, http.StatusBadGateway, rpcErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cancelled":     totalCancelled,
		"skipped":       totalSkipped,
		"shard_results": shardResults,
	})
}

// ---------------------------------------------------------------------------
// audit / helpers
// ---------------------------------------------------------------------------

func (s *Server) writeAudit(r *http.Request, e adminaudit.Entry) error {
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

func shardErrString(agg error, i int, results []*counterrpc.AdminCancelOrdersResponse) string {
	if agg == nil {
		return ""
	}
	if i < len(results) && results[i] != nil {
		return ""
	}
	return agg.Error()
}

func cancelTarget(b cancelOrdersBody) string {
	switch {
	case b.UserID != "" && b.Symbol != "":
		return b.UserID + "|" + b.Symbol
	case b.UserID != "":
		return b.UserID
	default:
		return b.Symbol
	}
}

// ---------------------------------------------------------------------------
// HTTP JSON primitives (duplicated from BFF so admin-gateway has no
// dependency on BFF's internal packages — cross-process boundary).
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func readJSON(r *http.Request, out any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}

func clientIP(r *http.Request) string {
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return host
}
