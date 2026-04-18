package rest

import (
	"net/http"
	"strconv"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	"github.com/xargin/opentrade/bff/internal/auth"
)

type placeConditionalBody struct {
	ClientConditionalID string `json:"client_conditional_id,omitempty"`
	Symbol              string `json:"symbol"`
	Side                string `json:"side"`       // "buy" / "sell"
	Type                string `json:"type"`       // "stop_loss" / "stop_loss_limit" / "take_profit" / "take_profit_limit"
	StopPrice           string `json:"stop_price"`
	LimitPrice          string `json:"limit_price,omitempty"`
	Qty                 string `json:"qty,omitempty"`
	QuoteQty            string `json:"quote_qty,omitempty"`
	TIF                 string `json:"tif,omitempty"`
}

// handlePlaceConditional POST /v1/conditional — forwards to the
// conditional service after lifting the authenticated user id off the
// request context (ADR-0040 §REST).
func (s *Server) handlePlaceConditional(w http.ResponseWriter, r *http.Request) {
	if s.conditional == nil {
		writeError(w, http.StatusServiceUnavailable, "conditional service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var body placeConditionalBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	side, err := parseSide(body.Side)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	typ, err := parseConditionalType(body.Type)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	tif := eventpb.TimeInForce_TIME_IN_FORCE_GTC
	if body.TIF != "" {
		t, err := parseTIF(body.TIF)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		tif = t
	}
	resp, err := s.conditional.PlaceConditional(r.Context(), &condrpc.PlaceConditionalRequest{
		UserId:              userID,
		ClientConditionalId: body.ClientConditionalID,
		Symbol:              body.Symbol,
		Side:                side,
		Type:                typ,
		StopPrice:           body.StopPrice,
		LimitPrice:          body.LimitPrice,
		Qty:                 body.Qty,
		QuoteQty:            body.QuoteQty,
		Tif:                 tif,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":                  resp.Id,
		"status":              conditionalStatusLabel(resp.Status),
		"accepted":            resp.Accepted,
		"received_ts_unix_ms": resp.ReceivedTsUnixMs,
	})
}

// handleCancelConditional DELETE /v1/conditional/{id}.
func (s *Server) handleCancelConditional(w http.ResponseWriter, r *http.Request) {
	if s.conditional == nil {
		writeError(w, http.StatusServiceUnavailable, "conditional service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	resp, err := s.conditional.CancelConditional(r.Context(), &condrpc.CancelConditionalRequest{
		UserId: userID, Id: id,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":       resp.Id,
		"accepted": resp.Accepted,
		"status":   conditionalStatusLabel(resp.Status),
	})
}

// handleQueryConditional GET /v1/conditional/{id}.
func (s *Server) handleQueryConditional(w http.ResponseWriter, r *http.Request) {
	if s.conditional == nil {
		writeError(w, http.StatusServiceUnavailable, "conditional service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	resp, err := s.conditional.QueryConditional(r.Context(), &condrpc.QueryConditionalRequest{
		UserId: userID, Id: id,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, conditionalToJSON(resp.Conditional))
}

// handleListConditionals GET /v1/conditional?include_inactive=true.
func (s *Server) handleListConditionals(w http.ResponseWriter, r *http.Request) {
	if s.conditional == nil {
		writeError(w, http.StatusServiceUnavailable, "conditional service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	include := r.URL.Query().Get("include_inactive") == "true"
	resp, err := s.conditional.ListConditionals(r.Context(), &condrpc.ListConditionalsRequest{
		UserId: userID, IncludeInactive: include,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(resp.Conditionals))
	for _, c := range resp.Conditionals {
		out = append(out, conditionalToJSON(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"conditionals": out})
}

// ---------------------------------------------------------------------------
// JSON <-> proto glue
// ---------------------------------------------------------------------------

func conditionalToJSON(c *condrpc.Conditional) map[string]any {
	if c == nil {
		return nil
	}
	return map[string]any{
		"id":                    c.Id,
		"client_conditional_id": c.ClientConditionalId,
		"symbol":                c.Symbol,
		"side":                  sideToString(c.Side),
		"type":                  conditionalTypeLabel(c.Type),
		"stop_price":            c.StopPrice,
		"limit_price":           c.LimitPrice,
		"qty":                   c.Qty,
		"quote_qty":             c.QuoteQty,
		"tif":                   tifToString(c.Tif),
		"status":                conditionalStatusLabel(c.Status),
		"created_at_unix_ms":    c.CreatedAtUnixMs,
		"triggered_at_unix_ms":  c.TriggeredAtUnixMs,
		"placed_order_id":       c.PlacedOrderId,
		"reject_reason":         c.RejectReason,
	}
}

func parseConditionalType(s string) (condrpc.ConditionalType, error) {
	switch s {
	case "stop_loss", "STOP_LOSS":
		return condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS, nil
	case "stop_loss_limit", "STOP_LOSS_LIMIT":
		return condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT, nil
	case "take_profit", "TAKE_PROFIT":
		return condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT, nil
	case "take_profit_limit", "TAKE_PROFIT_LIMIT":
		return condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT, nil
	}
	return condrpc.ConditionalType_CONDITIONAL_TYPE_UNSPECIFIED, badRequest("type", s)
}

func conditionalTypeLabel(t condrpc.ConditionalType) string {
	switch t {
	case condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS:
		return "stop_loss"
	case condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT:
		return "stop_loss_limit"
	case condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT:
		return "take_profit"
	case condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT:
		return "take_profit_limit"
	}
	return ""
}

func conditionalStatusLabel(s condrpc.ConditionalStatus) string {
	switch s {
	case condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING:
		return "pending"
	case condrpc.ConditionalStatus_CONDITIONAL_STATUS_TRIGGERED:
		return "triggered"
	case condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED:
		return "canceled"
	case condrpc.ConditionalStatus_CONDITIONAL_STATUS_REJECTED:
		return "rejected"
	}
	return "unknown"
}
