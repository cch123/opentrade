package rest

// perp.go routes the perp (USDT-margined futures) REST surface to perp-counter
// (ADR-0068 M7). Perp lives in its own /v1/perp/* namespace; PlaceOrder differs
// from spot by carrying leverage + reduce_only and may be REJECTED by the
// pre-trade margin gate (ADR-0068 §4), which the client must surface.

import (
	"net/http"
	"strconv"

	"connectrpc.com/connect"

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/auth"
)

// requirePerp writes 503 and returns false when the perp client is not wired.
func (s *Server) requirePerp(w http.ResponseWriter) bool {
	if s.perp == nil {
		writeError(w, http.StatusServiceUnavailable, "perp trading is not enabled")
		return false
	}
	return true
}

func perpUserID(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return 0, false
	}
	return userID, true
}

type perpPlaceOrderBody struct {
	ClientOrderID string `json:"client_order_id,omitempty"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`       // "buy" / "sell"
	OrderType     string `json:"order_type"` // "limit" / "market"
	TIF           string `json:"tif"`        // "gtc" / "ioc" / "fok" / "post_only"
	Price         string `json:"price,omitempty"`
	Qty           string `json:"qty"`
	Leverage      string `json:"leverage"`
	ReduceOnly    bool   `json:"reduce_only,omitempty"`
}

func (s *Server) handlePerpPlaceOrder(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpPlaceOrderBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.Symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol is required")
		return
	}
	side, err := parseSide(body.Side)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ot, err := parseOrderType(body.OrderType)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	tif, err := parseTIF(body.TIF)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.perp.PlaceOrder(r.Context(), connect.NewRequest(&perprpc.PlaceOrderRequest{
		UserId:        userID,
		ClientOrderId: body.ClientOrderID,
		Symbol:        body.Symbol,
		Side:          side,
		OrderType:     ot,
		Tif:           tif,
		Price:         body.Price,
		Qty:           body.Qty,
		Leverage:      body.Leverage,
		ReduceOnly:    body.ReduceOnly,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"order_id":            resp.Msg.OrderId,
		"client_order_id":     resp.Msg.ClientOrderId,
		"accepted":            resp.Msg.Accepted,
		"reject_reason":       resp.Msg.RejectReason,
		"received_ts_unix_ms": resp.Msg.ReceivedTsUnixMs,
	})
}

func (s *Server) handlePerpCancelOrder(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	orderID, err := strconv.ParseUint(r.PathValue("order_id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid order_id")
		return
	}
	resp, err := s.perp.CancelOrder(r.Context(), connect.NewRequest(&perprpc.CancelOrderRequest{
		UserId: userID, OrderId: orderID,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"order_id": resp.Msg.OrderId, "accepted": resp.Msg.Accepted,
	})
}

func (s *Server) handlePerpPositions(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	resp, err := s.perp.QueryPositions(r.Context(), connect.NewRequest(&perprpc.QueryPositionsRequest{
		UserId: userID, Symbol: r.URL.Query().Get("symbol"),
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	positions := make([]map[string]any, 0, len(resp.Msg.Positions))
	for _, p := range resp.Msg.Positions {
		positions = append(positions, map[string]any{
			"symbol":         p.Symbol,
			"side":           sideToString(p.Side),
			"size":           p.Size,
			"entry_price":    p.EntryPrice,
			"margin":         p.Margin,
			"leverage":       p.Leverage,
			"margin_mode":    perpMarginModeToString(p.MarginMode),
			"mark_price":     p.MarkPrice,
			"realized_pnl":   p.RealizedPnl,
			"unrealized_pnl": p.UnrealizedPnl,
			"margin_ratio":   p.MarginRatio,
			"liq_price":      p.LiqPrice,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"positions": positions})
}

func (s *Server) handlePerpMargin(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	resp, err := s.perp.QueryMargin(r.Context(), connect.NewRequest(&perprpc.QueryMarginRequest{UserId: userID}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset":                      resp.Msg.Asset,
		"free_balance":               resp.Msg.FreeBalance,
		"order_margin_reserved":      resp.Msg.OrderMarginReserved,
		"isolated_margin_locked":     resp.Msg.IsolatedMarginLocked,
		"isolated_unrealized_pnl":    resp.Msg.IsolatedUnrealizedPnl,
		"cross_unrealized_pnl":       resp.Msg.CrossUnrealizedPnl,
		"cross_initial_required":     resp.Msg.CrossInitialRequired,
		"cross_maintenance_required": resp.Msg.CrossMaintenanceRequired,
		"available_to_trade":         resp.Msg.AvailableToTrade,
		"available_to_withdraw":      resp.Msg.AvailableToWithdraw,
	})
}

func perpMarginModeToString(m perprpc.MarginMode) string {
	switch m {
	case perprpc.MarginMode_MARGIN_MODE_ISOLATED:
		return "isolated"
	case perprpc.MarginMode_MARGIN_MODE_CROSS:
		return "cross"
	}
	return "unspecified"
}

// --- ADR-0074 account / position config surface ---------------------------

type perpMarginModeBody struct {
	Symbol       string `json:"symbol"`
	MarginMode   string `json:"margin_mode"`             // "isolated" / "cross"
	TargetMargin string `json:"target_margin,omitempty"` // cross→isolated only
	ClientOpID   string `json:"client_op_id,omitempty"`
}

func (s *Server) handlePerpSetMarginMode(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpMarginModeBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	mode := perprpc.MarginMode_MARGIN_MODE_UNSPECIFIED
	switch body.MarginMode {
	case "isolated":
		mode = perprpc.MarginMode_MARGIN_MODE_ISOLATED
	case "cross":
		mode = perprpc.MarginMode_MARGIN_MODE_CROSS
	default:
		writeError(w, http.StatusBadRequest, "margin_mode must be isolated or cross")
		return
	}
	resp, err := s.perp.SetMarginMode(r.Context(), connect.NewRequest(&perprpc.SetMarginModeRequest{
		UserId: userID, Symbol: body.Symbol, TargetMode: mode,
		TargetMargin: body.TargetMargin, ClientOpId: body.ClientOpID,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":           resp.Msg.Accepted,
		"reject_reason":      resp.Msg.RejectReason,
		"margin_mode":        perpMarginModeToString(resp.Msg.MarginMode),
		"position_margin":    resp.Msg.PositionMargin,
		"free_balance_after": resp.Msg.FreeBalanceAfter,
	})
}

type perpAdjustMarginBody struct {
	Symbol     string `json:"symbol"`
	Delta      string `json:"delta"` // signed decimal
	ClientOpID string `json:"client_op_id,omitempty"`
}

func (s *Server) handlePerpAdjustMargin(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpAdjustMarginBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.perp.AdjustIsolatedMargin(r.Context(), connect.NewRequest(&perprpc.AdjustIsolatedMarginRequest{
		UserId: userID, Symbol: body.Symbol, Delta: body.Delta, ClientOpId: body.ClientOpID,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":           resp.Msg.Accepted,
		"reject_reason":      resp.Msg.RejectReason,
		"position_margin":    resp.Msg.PositionMargin,
		"free_balance_after": resp.Msg.FreeBalanceAfter,
	})
}

type perpAutoAddBody struct {
	Symbol         string `json:"symbol"`
	Enabled        bool   `json:"enabled"`
	MaxAddPerEvent string `json:"max_add_per_event,omitempty"`
	ClientOpID     string `json:"client_op_id,omitempty"`
}

func (s *Server) handlePerpSetAutoAdd(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpAutoAddBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.perp.SetAutoAddMargin(r.Context(), connect.NewRequest(&perprpc.SetAutoAddMarginRequest{
		UserId: userID, Symbol: body.Symbol, Enabled: body.Enabled,
		MaxAddPerEvent: body.MaxAddPerEvent, ClientOpId: body.ClientOpID,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": resp.Msg.Accepted, "reject_reason": resp.Msg.RejectReason,
	})
}

type perpLeverageBody struct {
	Symbol     string `json:"symbol"`
	Leverage   string `json:"leverage"`
	ClientOpID string `json:"client_op_id,omitempty"`
}

func (s *Server) handlePerpSetLeverage(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpLeverageBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.perp.SetPositionLeverage(r.Context(), connect.NewRequest(&perprpc.SetPositionLeverageRequest{
		UserId: userID, Symbol: body.Symbol, Leverage: body.Leverage, ClientOpId: body.ClientOpID,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":           resp.Msg.Accepted,
		"reject_reason":      resp.Msg.RejectReason,
		"leverage":           resp.Msg.Leverage,
		"position_margin":    resp.Msg.PositionMargin,
		"free_balance_after": resp.Msg.FreeBalanceAfter,
	})
}

type perpRiskIDBody struct {
	Symbol     string `json:"symbol"`
	RiskID     uint32 `json:"risk_id"`
	ClientOpID string `json:"client_op_id,omitempty"`
}

func (s *Server) handlePerpSetRiskID(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpRiskIDBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.perp.SetRiskId(r.Context(), connect.NewRequest(&perprpc.SetRiskIdRequest{
		UserId: userID, Symbol: body.Symbol, RiskId: body.RiskID, ClientOpId: body.ClientOpID,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": resp.Msg.Accepted, "reject_reason": resp.Msg.RejectReason,
		"risk_id": resp.Msg.RiskId,
	})
}

func (s *Server) handlePerpPositionConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	resp, err := s.perp.QueryPositionConfig(r.Context(), connect.NewRequest(&perprpc.QueryPositionConfigRequest{
		UserId: userID, Symbol: r.URL.Query().Get("symbol"),
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	configs := make([]map[string]any, 0, len(resp.Msg.Configs))
	for _, c := range resp.Msg.Configs {
		configs = append(configs, map[string]any{
			"symbol":                 c.Symbol,
			"margin_mode":            perpMarginModeToString(c.MarginMode),
			"leverage":               c.Leverage,
			"risk_id":                c.RiskId,
			"auto_add_margin":        c.AutoAddMargin,
			"auto_add_max":           c.AutoAddMax,
			"effective_max_leverage": c.EffectiveMaxLeverage,
			"max_notional":           c.MaxNotional,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"configs": configs})
}

func (s *Server) handlePerpAccountConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	resp, err := s.perp.QueryAccountConfig(r.Context(), connect.NewRequest(&perprpc.QueryAccountConfigRequest{UserId: userID}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	limits := make([]map[string]any, 0, len(resp.Msg.LeverageLimits))
	for _, l := range resp.Msg.LeverageLimits {
		limits = append(limits, map[string]any{
			"symbol": l.Symbol, "max_leverage": l.MaxLeverage, "reason": l.Reason,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"settle_asset":    resp.Msg.SettleAsset,
		"risk_model":      resp.Msg.RiskModel,
		"cross_pool_id":   resp.Msg.CrossPoolId,
		"leverage_limits": limits,
	})
}

func (s *Server) handlePerpMarginAdjustments(w http.ResponseWriter, r *http.Request) {
	if s.history == nil {
		writeError(w, http.StatusServiceUnavailable, "history service not configured")
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	resp, err := s.history.ListPerpMarginAdjustments(r.Context(), connect.NewRequest(&historypb.ListPerpMarginAdjustmentsRequest{
		UserId: userID, Symbol: q.Get("symbol"),
		SinceMs: parseInt64Query(q.Get("since_ms")), UntilMs: parseInt64Query(q.Get("until_ms")),
		Cursor: q.Get("cursor"), Limit: parseInt32Query(q.Get("limit")),
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"adjustments": resp.Msg.Adjustments, "next_cursor": resp.Msg.NextCursor,
	})
}

func (s *Server) handlePerpConfigLogs(w http.ResponseWriter, r *http.Request) {
	if s.history == nil {
		writeError(w, http.StatusServiceUnavailable, "history service not configured")
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	resp, err := s.history.ListPerpConfigLogs(r.Context(), connect.NewRequest(&historypb.ListPerpConfigLogsRequest{
		UserId: userID, Symbol: q.Get("symbol"),
		SinceMs: parseInt64Query(q.Get("since_ms")), UntilMs: parseInt64Query(q.Get("until_ms")),
		Cursor: q.Get("cursor"), Limit: parseInt32Query(q.Get("limit")),
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"logs": resp.Msg.Logs, "next_cursor": resp.Msg.NextCursor,
	})
}
