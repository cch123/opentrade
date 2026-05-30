package rest

// perp.go routes the perp (USDT-margined futures) REST surface to perp-counter
// (ADR-0068 M7). Perp lives in its own /v1/perp/* namespace; PlaceOrder differs
// from spot by carrying leverage + reduce_only and may be REJECTED by the
// pre-trade margin gate (ADR-0068 §4), which the client must surface.

import (
	"net/http"
	"strconv"

	"connectrpc.com/connect"

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
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
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
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
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
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
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
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	resp, err := s.perp.QueryMargin(r.Context(), connect.NewRequest(&perprpc.QueryMarginRequest{UserId: userID}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset":           resp.Msg.Asset,
		"available":       resp.Msg.Available,
		"reserved":        resp.Msg.Reserved,
		"position_margin": resp.Msg.PositionMargin,
		"unrealized_pnl":  resp.Msg.UnrealizedPnl,
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
