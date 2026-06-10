package server

// perp.go is the ADR-0075 §5 admin surface over the perp symbol catalog:
// CreatePerpSymbol, UpdatePerpSymbolConfig (publish), SetPerpSymbolStatus,
// RollbackPerpSymbolConfig, ListPerpSymbolVersions. Every mutation is
// audited with operator / previous→new version / changed parameter groups /
// reason before the HTTP response is written, and rollback NEVER rewrites
// history — it publishes a new version copying the target (source_version
// records the provenance).

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/auth"
	"github.com/xargin/opentrade/pkg/perpcfg"
)

// perpRoutes mounts the catalog endpoints. Nil store → 503 (mirrors the etcd
// nil handling for spot symbols).
func (s *Server) perpRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /admin/perp/symbols", s.handleCreatePerpSymbol)
	mux.HandleFunc("GET /admin/perp/symbols", s.handleListPerpSymbols)
	mux.HandleFunc("GET /admin/perp/symbols/{symbol}", s.handleGetPerpSymbol)
	mux.HandleFunc("GET /admin/perp/symbols/{symbol}/versions", s.handleListPerpVersions)
	mux.HandleFunc("PUT /admin/perp/symbols/{symbol}/config", s.handleUpdatePerpConfig)
	mux.HandleFunc("PUT /admin/perp/symbols/{symbol}/status", s.handleSetPerpStatus)
	mux.HandleFunc("POST /admin/perp/symbols/{symbol}/rollback", s.handleRollbackPerpConfig)
}

func (s *Server) perpStoreOr503(w http.ResponseWriter) bool {
	if s.perp == nil {
		writeError(w, http.StatusServiceUnavailable, "perp catalog not configured")
		return false
	}
	return true
}

// latestPerpVersion returns the symbol's most recently PUBLISHED config (the
// merge / transition basis — deliberately not the effective-time view, so
// scheduling a future version then editing again builds on the scheduled
// content, not the still-active old one).
func (s *Server) latestPerpVersion(ctx context.Context, symbol string) (*perpcfg.PerpSymbolConfig, error) {
	versions, err := s.perp.ListVersions(ctx, symbol)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("symbol has no published config")
	}
	return versions[len(versions)-1], nil
}

// ---------------------------------------------------------------------------
// Create / read
// ---------------------------------------------------------------------------

type createPerpSymbolBody struct {
	perpcfg.PerpSymbol
	Config perpcfg.PerpSymbolConfig `json:"config"`
}

func (s *Server) handleCreatePerpSymbol(w http.ResponseWriter, r *http.Request) {
	if !s.perpStoreOr503(w) {
		return
	}
	var body createPerpSymbolBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	body.Config.Symbol = body.Symbol
	if body.Config.Status == "" {
		body.Config.Status = perpcfg.StatusPreopen // the matrix's entry state
	}
	if body.Config.RiskApply == "" {
		body.Config.RiskApply = perpcfg.RiskApplyStaged
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	createErr := s.perp.CreateSymbol(ctx, body.PerpSymbol, body.Config)
	if err := s.writeAudit(r, adminaudit.Entry{
		Op: "admin.perp.symbol.create", Target: body.Symbol,
		Params: map[string]any{
			"contract_type": string(body.ContractType),
			"base_asset":    body.BaseAsset, "quote_asset": body.QuoteAsset,
			"status": string(body.Config.Status), "config_version": 1,
			"effective_from_ms": body.Config.EffectiveFromMs,
			"reason":            body.Config.Reason,
		},
		Status: statusFromErr(createErr), Error: errString(createErr),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("audit write failed: %v (create err: %v)", err, createErr))
		return
	}
	if createErr != nil {
		writePerpStoreErr(w, createErr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"symbol": body.Symbol, "config_version": 1})
}

func (s *Server) handleListPerpSymbols(w http.ResponseWriter, r *http.Request) {
	if !s.perpStoreOr503(w) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	cat, err := s.perp.LoadAll(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	nowMs := time.Now().UnixMilli()
	type row struct {
		Spec          perpcfg.PerpSymbol        `json:"spec"`
		Active        *perpcfg.PerpSymbolConfig `json:"active,omitempty"`
		LatestVersion uint64                    `json:"latest_version"`
	}
	rows := make([]row, 0, len(cat.Symbols))
	for _, cs := range cat.Symbols {
		rw := row{Spec: cs.Spec}
		if len(cs.Versions) > 0 {
			rw.LatestVersion = cs.Versions[len(cs.Versions)-1].ConfigVersion
		}
		if active, ok := cs.ActiveAt(nowMs); ok {
			rw.Active = active
		}
		rows = append(rows, rw)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Spec.Symbol < rows[j].Spec.Symbol })
	writeJSON(w, http.StatusOK, map[string]any{"anchor": cat.Anchor, "symbols": rows})
}

func (s *Server) handleGetPerpSymbol(w http.ResponseWriter, r *http.Request) {
	if !s.perpStoreOr503(w) {
		return
	}
	symbol := r.PathValue("symbol")
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	cat, err := s.perp.LoadAll(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	cs, ok := cat.Symbols[symbol]
	if !ok {
		writeError(w, http.StatusNotFound, "symbol not found")
		return
	}
	active, _ := cs.ActiveAt(time.Now().UnixMilli())
	writeJSON(w, http.StatusOK, map[string]any{
		"spec": cs.Spec, "active": active,
		"latest_version": cs.Versions[len(cs.Versions)-1].ConfigVersion,
	})
}

func (s *Server) handleListPerpVersions(w http.ResponseWriter, r *http.Request) {
	if !s.perpStoreOr503(w) {
		return
	}
	symbol := r.PathValue("symbol")
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	versions, err := s.perp.ListVersions(ctx, symbol)
	if err != nil {
		writePerpStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"symbol": symbol, "versions": versions})
}

// ---------------------------------------------------------------------------
// Publish (config update / status transition / rollback)
// ---------------------------------------------------------------------------

// updatePerpConfigBody is a JSON merge over the latest published version:
// provided parameter groups replace, omitted ones carry over. Status changes
// are NOT accepted here — SetPerpSymbolStatus owns the transition matrix.
type updatePerpConfigBody struct {
	Precision       *perpcfg.Precision         `json:"precision"`
	OrderLimits     *perpcfg.OrderLimits       `json:"order_limits"`
	RiskTiers       []perpcfg.RiskTier         `json:"risk_tiers"`
	Funding         *perpcfg.FundingParams     `json:"funding"`
	Pricing         *perpcfg.PricingParams     `json:"pricing"`
	Fees            *perpcfg.FeeParams         `json:"fees"`
	PriceProtection *perpcfg.PriceProtection   `json:"price_protection"`
	RiskApply       perpcfg.RiskApply          `json:"risk_apply"`
	RepricePolicy   *perpcfg.RiskRepricePolicy `json:"reprice_policy"`
	EffectiveFromMs int64                      `json:"effective_from_ms"`
	Reason          string                     `json:"reason"`
	// ExpectedVersion is an optional optimistic concurrency control (OCC)
	// precondition: when non-zero the publish is refused unless the latest
	// published version still equals it (two operators editing
	// concurrently get a 409 instead of silently merging over each other).
	ExpectedVersion uint64 `json:"expected_version"`
}

func (s *Server) handleUpdatePerpConfig(w http.ResponseWriter, r *http.Request) {
	if !s.perpStoreOr503(w) {
		return
	}
	symbol := r.PathValue("symbol")
	var body updatePerpConfigBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	prev, err := s.latestPerpVersion(ctx, symbol)
	if err != nil {
		writePerpStoreErr(w, err)
		return
	}
	if body.ExpectedVersion != 0 && body.ExpectedVersion != prev.ConfigVersion {
		writeError(w, http.StatusConflict, fmt.Sprintf(
			"expected_version %d, latest is %d", body.ExpectedVersion, prev.ConfigVersion))
		return
	}
	next := *prev
	if body.Precision != nil {
		next.Precision = *body.Precision
	}
	if body.OrderLimits != nil {
		next.OrderLimits = *body.OrderLimits
	}
	if body.RiskTiers != nil {
		next.RiskTiers = body.RiskTiers
	}
	if body.Funding != nil {
		next.Funding = *body.Funding
	}
	if body.Pricing != nil {
		next.Pricing = *body.Pricing
	}
	if body.Fees != nil {
		next.Fees = *body.Fees
	}
	if body.PriceProtection != nil {
		next.PriceProtection = *body.PriceProtection
	}
	if body.RiskApply != "" {
		next.RiskApply = body.RiskApply
	}
	next.RepricePolicy = body.RepricePolicy // explicit per publish, never inherited
	next.EffectiveFromMs = body.EffectiveFromMs
	next.Reason = body.Reason
	next.SourceVersion = 0
	next.CreatedBy = adminID(r)

	s.publishPerpConfig(w, r, ctx, "admin.perp.config.publish", prev, next, map[string]any{})
}

type setPerpStatusBody struct {
	Status          perpcfg.Status `json:"status"`
	Reason          string         `json:"reason"`
	EffectiveFromMs int64          `json:"effective_from_ms"`
	// Force bypasses the ADR-0075 §2 transition matrix (admin emergency
	// path). A reason is mandatory; the audit entry records forced=true.
	Force bool `json:"force"`
}

func (s *Server) handleSetPerpStatus(w http.ResponseWriter, r *http.Request) {
	if !s.perpStoreOr503(w) {
		return
	}
	symbol := r.PathValue("symbol")
	var body setPerpStatusBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !body.Status.Valid() {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown status %q", body.Status))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	prev, err := s.latestPerpVersion(ctx, symbol)
	if err != nil {
		writePerpStoreErr(w, err)
		return
	}
	if !perpcfg.CanTransition(prev.Status, body.Status) {
		if !body.Force {
			writeError(w, http.StatusUnprocessableEntity, fmt.Sprintf(
				"illegal transition %s -> %s (ADR-0075 §2 matrix); use force with a reason for the emergency path",
				prev.Status, body.Status))
			return
		}
		if body.Reason == "" {
			writeError(w, http.StatusBadRequest, "forced transition requires a reason")
			return
		}
	}
	next := *prev
	next.Status = body.Status
	next.EffectiveFromMs = body.EffectiveFromMs
	next.Reason = body.Reason
	next.RepricePolicy = nil
	next.SourceVersion = 0
	next.CreatedBy = adminID(r)

	s.publishPerpConfig(w, r, ctx, "admin.perp.status.set", prev, next, map[string]any{
		"from_status": string(prev.Status), "to_status": string(body.Status),
		"forced": body.Force && !perpcfg.CanTransition(prev.Status, body.Status),
	})
}

type rollbackPerpBody struct {
	TargetVersion   uint64 `json:"target_version"`
	Reason          string `json:"reason"`
	EffectiveFromMs int64  `json:"effective_from_ms"`
	Force           bool   `json:"force"` // matrix bypass when the target's status differs
}

func (s *Server) handleRollbackPerpConfig(w http.ResponseWriter, r *http.Request) {
	if !s.perpStoreOr503(w) {
		return
	}
	symbol := r.PathValue("symbol")
	var body rollbackPerpBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.TargetVersion == 0 {
		writeError(w, http.StatusBadRequest, "target_version is required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	versions, err := s.perp.ListVersions(ctx, symbol)
	if err != nil {
		writePerpStoreErr(w, err)
		return
	}
	prev := versions[len(versions)-1]
	var target *perpcfg.PerpSymbolConfig
	for _, v := range versions {
		if v.ConfigVersion == body.TargetVersion {
			target = v
			break
		}
	}
	if target == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("version %d not found", body.TargetVersion))
		return
	}
	// Rolling back to a version with a different status is a transition and
	// obeys the same matrix / emergency rules.
	if target.Status != prev.Status && !perpcfg.CanTransition(prev.Status, target.Status) {
		if !body.Force {
			writeError(w, http.StatusUnprocessableEntity, fmt.Sprintf(
				"rollback implies illegal transition %s -> %s; use force with a reason",
				prev.Status, target.Status))
			return
		}
		if body.Reason == "" {
			writeError(w, http.StatusBadRequest, "forced transition requires a reason")
			return
		}
	}
	next := *target
	next.SourceVersion = target.ConfigVersion
	next.EffectiveFromMs = body.EffectiveFromMs
	next.Reason = body.Reason
	next.RepricePolicy = target.RepricePolicy
	next.CreatedBy = adminID(r)

	s.publishPerpConfig(w, r, ctx, "admin.perp.config.rollback", prev, next, map[string]any{
		"target_version": body.TargetVersion,
	})
}

// publishPerpConfig runs the shared publish + audit + respond tail. extra is
// merged into the audit params after the standard fields.
func (s *Server) publishPerpConfig(w http.ResponseWriter, r *http.Request, ctx context.Context,
	op string, prev *perpcfg.PerpSymbolConfig, next perpcfg.PerpSymbolConfig, extra map[string]any) {
	newVersion, pubErr := s.perp.PublishConfig(ctx, next)
	params := map[string]any{
		"prev_version": prev.ConfigVersion,
		"new_version":  newVersion,
		"changed":      configDiff(prev, &next),
		"risk_apply":   string(next.RiskApply),
	}
	if next.EffectiveFromMs != 0 {
		params["effective_from_ms"] = next.EffectiveFromMs
	}
	if next.Reason != "" {
		params["reason"] = next.Reason
	}
	if next.RepricePolicy != nil {
		params["reprice_policy_id"] = next.RepricePolicy.PolicyID
	}
	for k, v := range extra {
		params[k] = v
	}
	if err := s.writeAudit(r, adminaudit.Entry{
		Op: op, Target: next.Symbol, Params: params,
		Status: statusFromErr(pubErr), Error: errString(pubErr),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("audit write failed: %v (publish err: %v)", err, pubErr))
		return
	}
	if pubErr != nil {
		writePerpStoreErr(w, pubErr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"symbol": next.Symbol, "config_version": newVersion,
		"changed": configDiff(prev, &next), "status": string(next.Status),
	})
}

// configDiff lists the parameter groups that differ between two versions —
// the human-scannable half of the audit diff (the full content is always
// reconstructable from the two immutable version rows).
func configDiff(old, new *perpcfg.PerpSymbolConfig) []string {
	out := []string{}
	add := func(name string, differs bool) {
		if differs {
			out = append(out, name)
		}
	}
	add("status", old.Status != new.Status)
	add("precision", !reflect.DeepEqual(old.Precision, new.Precision))
	add("order_limits", !reflect.DeepEqual(old.OrderLimits, new.OrderLimits))
	add("risk_tiers", !reflect.DeepEqual(old.RiskTiers, new.RiskTiers))
	add("funding", !reflect.DeepEqual(old.Funding, new.Funding))
	add("pricing", !reflect.DeepEqual(old.Pricing, new.Pricing))
	add("fees", !reflect.DeepEqual(old.Fees, new.Fees))
	add("price_protection", !reflect.DeepEqual(old.PriceProtection, new.PriceProtection))
	add("risk_apply", old.RiskApply != new.RiskApply)
	add("reprice_policy", !reflect.DeepEqual(old.RepricePolicy, new.RepricePolicy))
	return out
}

func writePerpStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, perpcfg.ErrSymbolExists):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, perpcfg.ErrSymbolNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	default:
		// Validation failures read as 400s; transport errors as 502 would
		// need error typing the store doesn't expose yet — 400 is the safer
		// default for a human-driven admin plane (the message carries it).
		writeError(w, http.StatusBadRequest, err.Error())
	}
}

func adminID(r *http.Request) string {
	if uid, err := auth.UserID(r.Context()); err == nil {
		return strconv.FormatUint(uid, 10)
	}
	return ""
}
