package rest

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"connectrpc.com/connect"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	"github.com/xargin/opentrade/pkg/auth"
)

type placeTriggerBody struct {
	ClientTriggerID string `json:"client_trigger_id,omitempty"`
	Symbol              string `json:"symbol"`
	Side                string `json:"side"`       // "buy" / "sell"
	Type                string `json:"type"`       // "stop_loss" / "stop_loss_limit" / "take_profit" / "take_profit_limit" / "trailing_stop_loss"
	StopPrice           string `json:"stop_price,omitempty"`
	LimitPrice          string `json:"limit_price,omitempty"`
	Qty                 string `json:"qty,omitempty"`
	QuoteQty            string `json:"quote_qty,omitempty"`
	TIF                 string `json:"tif,omitempty"`
	// Optional absolute expiry (unix ms); 0 = never expires. ADR-0043.
	ExpiresAtUnixMs int64 `json:"expires_at_unix_ms,omitempty"`
	// Trailing-stop fields (ADR-0045). trailing_delta_bps required for
	// type "trailing_stop_loss", forbidden otherwise.
	TrailingDeltaBps int32  `json:"trailing_delta_bps,omitempty"`
	ActivationPrice  string `json:"activation_price,omitempty"`
}

// handlePlaceTrigger POST /v1/trigger — forwards to the
// trigger service after lifting the authenticated user id off the
// request context (ADR-0040 §REST).
func (s *Server) handlePlaceTrigger(w http.ResponseWriter, r *http.Request) {
	if s.trigger == nil {
		writeError(w, http.StatusServiceUnavailable, "trigger service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var body placeTriggerBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	side, err := parseSide(body.Side)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	typ, err := parseTriggerType(body.Type)
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
	resp, err := s.trigger.PlaceTrigger(r.Context(), connect.NewRequest(&condrpc.PlaceTriggerRequest{
		UserId:           userID,
		ClientTriggerId:  body.ClientTriggerID,
		Symbol:           body.Symbol,
		Side:             side,
		Type:             typ,
		StopPrice:        body.StopPrice,
		LimitPrice:       body.LimitPrice,
		Qty:              body.Qty,
		QuoteQty:         body.QuoteQty,
		Tif:              tif,
		ExpiresAtUnixMs:  body.ExpiresAtUnixMs,
		TrailingDeltaBps: body.TrailingDeltaBps,
		ActivationPrice:  body.ActivationPrice,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":                  resp.Msg.Id,
		"status":              triggerStatusLabel(resp.Msg.Status),
		"accepted":            resp.Msg.Accepted,
		"received_ts_unix_ms": resp.Msg.ReceivedTsUnixMs,
	})
}

// handleCancelTrigger DELETE /v1/trigger/{id}.
func (s *Server) handleCancelTrigger(w http.ResponseWriter, r *http.Request) {
	if s.trigger == nil {
		writeError(w, http.StatusServiceUnavailable, "trigger service not configured")
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
	resp, err := s.trigger.CancelTrigger(r.Context(), connect.NewRequest(&condrpc.CancelTriggerRequest{
		UserId: userID, Id: id,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":       resp.Msg.Id,
		"accepted": resp.Msg.Accepted,
		"status":   triggerStatusLabel(resp.Msg.Status),
	})
}

// handleQueryTrigger GET /v1/trigger/{id}. Tries the trigger
// service first (hot path for active PENDING triggers) and falls back
// to history's long-term projection on NotFound — so a client looking up
// a trigger that aged out of the trigger service's in-memory
// terminal buffer still gets a row (ADR-0047).
func (s *Server) handleQueryTrigger(w http.ResponseWriter, r *http.Request) {
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

	if s.trigger != nil {
		resp, err := s.trigger.QueryTrigger(r.Context(), connect.NewRequest(&condrpc.QueryTriggerRequest{
			UserId: userID, Id: id,
		}))
		if err == nil {
			writeJSON(w, http.StatusOK, triggerToJSON(resp.Msg.Trigger))
			return
		}
		if connect.CodeOf(err) != connect.CodeNotFound || s.history == nil {
			writeConnectError(w, err)
			return
		}
		// Fall through to history lookup below.
	} else if s.history == nil {
		writeError(w, http.StatusServiceUnavailable, "trigger service not configured")
		return
	}

	hresp, herr := s.history.GetTrigger(r.Context(), connect.NewRequest(&historypb.GetTriggerRequest{
		UserId: userID, Id: id,
	}))
	if herr != nil {
		writeConnectError(w, herr)
		return
	}
	writeJSON(w, http.StatusOK, historyTriggerToJSON(hresp.Msg.Trigger))
}

// placeOCOBody is the REST request for POST /v1/trigger/oco.
type placeOCOBody struct {
	ClientOCOID string                 `json:"client_oco_id,omitempty"`
	Legs        []placeTriggerBody `json:"legs"`
}

// handlePlaceOCO POST /v1/trigger/oco — forwards an N-leg OCO request
// (ADR-0044). All legs share the authenticated user_id; validation of
// same-symbol / same-side / reservation shape runs inside the trigger
// service.
func (s *Server) handlePlaceOCO(w http.ResponseWriter, r *http.Request) {
	if s.trigger == nil {
		writeError(w, http.StatusServiceUnavailable, "trigger service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var body placeOCOBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(body.Legs) < 2 {
		writeError(w, http.StatusBadRequest, "OCO requires at least two legs")
		return
	}
	legs := make([]*condrpc.PlaceTriggerRequest, len(body.Legs))
	for i, lb := range body.Legs {
		side, perr := parseSide(lb.Side)
		if perr != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("leg %d: %s", i, perr.Error()))
			return
		}
		typ, perr := parseTriggerType(lb.Type)
		if perr != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("leg %d: %s", i, perr.Error()))
			return
		}
		tif := eventpb.TimeInForce_TIME_IN_FORCE_GTC
		if lb.TIF != "" {
			t, terr := parseTIF(lb.TIF)
			if terr != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("leg %d: %s", i, terr.Error()))
				return
			}
			tif = t
		}
		legs[i] = &condrpc.PlaceTriggerRequest{
			UserId:              userID,
			ClientTriggerId: lb.ClientTriggerID,
			Symbol:              lb.Symbol,
			Side:                side,
			Type:                typ,
			StopPrice:           lb.StopPrice,
			LimitPrice:          lb.LimitPrice,
			Qty:                 lb.Qty,
			QuoteQty:            lb.QuoteQty,
			Tif:                 tif,
			ExpiresAtUnixMs:     lb.ExpiresAtUnixMs,
			TrailingDeltaBps:    lb.TrailingDeltaBps,
			ActivationPrice:     lb.ActivationPrice,
		}
	}
	resp, err := s.trigger.PlaceOCO(r.Context(), connect.NewRequest(&condrpc.PlaceOCORequest{
		UserId:      userID,
		ClientOcoId: body.ClientOCOID,
		Legs:        legs,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	legsOut := make([]map[string]any, len(resp.Msg.Legs))
	for i, lr := range resp.Msg.Legs {
		legsOut[i] = map[string]any{
			"id":       lr.Id,
			"status":   triggerStatusLabel(lr.Status),
			"accepted": lr.Accepted,
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"oco_group_id":        resp.Msg.OcoGroupId,
		"legs":                legsOut,
		"accepted":            resp.Msg.Accepted,
		"received_ts_unix_ms": resp.Msg.ReceivedTsUnixMs,
	})
}

// handleListTriggers GET /v1/trigger.
//
// Query params:
//
//	scope=active|terminal|all (default: active for back-compat)
//	include_inactive=true (legacy; equivalent to scope=all)
//	symbol=, since_ms=, until_ms=, cursor=, limit= (history only)
//
// Scope `active` routes to the trigger service's in-memory hot path
// (no pagination — active set is small). `terminal` and `all` route to
// history's MySQL projection with cursor pagination (ADR-0047).
func (s *Server) handleListTriggers(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	q := r.URL.Query()
	scope := strings.ToLower(q.Get("scope"))
	if scope == "" {
		if q.Get("include_inactive") == "true" {
			scope = "all"
		} else {
			scope = "active"
		}
	}

	switch scope {
	case "active":
		if s.trigger == nil {
			writeError(w, http.StatusServiceUnavailable, "trigger service not configured")
			return
		}
		resp, err := s.trigger.ListTriggers(r.Context(), connect.NewRequest(&condrpc.ListTriggersRequest{
			UserId: userID, IncludeInactive: false,
		}))
		if err != nil {
			writeConnectError(w, err)
			return
		}
		out := make([]map[string]any, 0, len(resp.Msg.Triggers))
		for _, c := range resp.Msg.Triggers {
			out = append(out, triggerToJSON(c))
		}
		writeJSON(w, http.StatusOK, map[string]any{"triggers": out})
	case "terminal", "all":
		if s.history == nil {
			// Back-compat: without history deployed, fall back to the
			// trigger service's in-memory terminal buffer for "all"
			// (MVP-14 behaviour). "terminal" alone truly needs history.
			if scope == "all" && s.trigger != nil {
				resp, err := s.trigger.ListTriggers(r.Context(), connect.NewRequest(&condrpc.ListTriggersRequest{
					UserId: userID, IncludeInactive: true,
				}))
				if err != nil {
					writeConnectError(w, err)
					return
				}
				out := make([]map[string]any, 0, len(resp.Msg.Triggers))
				for _, c := range resp.Msg.Triggers {
					out = append(out, triggerToJSON(c))
				}
				writeJSON(w, http.StatusOK, map[string]any{"triggers": out})
				return
			}
			writeError(w, http.StatusServiceUnavailable, "history service not configured (needed for terminal triggers; ADR-0047)")
			return
		}
		histScope := historypb.TriggerScope_TRIGGER_SCOPE_TERMINAL
		if scope == "all" {
			histScope = historypb.TriggerScope_TRIGGER_SCOPE_ALL
		}
		resp, err := s.history.ListTriggers(r.Context(), connect.NewRequest(&historypb.ListTriggersRequest{
			UserId:  userID,
			Symbol:  q.Get("symbol"),
			Scope:   histScope,
			SinceMs: parseInt64Query(q.Get("since_ms")),
			UntilMs: parseInt64Query(q.Get("until_ms")),
			Cursor:  q.Get("cursor"),
			Limit:   parseInt32Query(q.Get("limit")),
		}))
		if err != nil {
			writeConnectError(w, err)
			return
		}
		out := make([]map[string]any, 0, len(resp.Msg.Triggers))
		for _, c := range resp.Msg.Triggers {
			out = append(out, historyTriggerToJSON(c))
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"triggers":    out,
			"next_cursor": resp.Msg.NextCursor,
		})
	default:
		writeError(w, http.StatusBadRequest, "invalid scope: "+scope)
	}
}

// ---------------------------------------------------------------------------
// JSON <-> proto glue
// ---------------------------------------------------------------------------

func triggerToJSON(c *condrpc.Trigger) map[string]any {
	if c == nil {
		return nil
	}
	return map[string]any{
		"id":                    c.Id,
		"client_trigger_id": c.ClientTriggerId,
		"symbol":                c.Symbol,
		"side":                  sideToString(c.Side),
		"type":                  triggerTypeLabel(c.Type),
		"stop_price":            c.StopPrice,
		"limit_price":           c.LimitPrice,
		"qty":                   c.Qty,
		"quote_qty":             c.QuoteQty,
		"tif":                   tifToString(c.Tif),
		"status":                triggerStatusLabel(c.Status),
		"created_at_unix_ms":    c.CreatedAtUnixMs,
		"triggered_at_unix_ms":  c.TriggeredAtUnixMs,
		"placed_order_id":       c.PlacedOrderId,
		"reject_reason":         c.RejectReason,
		"expires_at_unix_ms":    c.ExpiresAtUnixMs,
		"oco_group_id":          c.OcoGroupId,
		"trailing_delta_bps":    c.TrailingDeltaBps,
		"activation_price":      c.ActivationPrice,
		"trailing_watermark":    c.TrailingWatermark,
		"trailing_active":       c.TrailingActive,
	}
}

// historyTriggerToJSON maps history.Trigger to the same shape as
// triggerToJSON so clients can treat both sources uniformly. Field
// names match the trigger service's output (`placed_order_id`, not
// `triggered_order_id`) for API stability.
func historyTriggerToJSON(c *historypb.Trigger) map[string]any {
	if c == nil {
		return nil
	}
	return map[string]any{
		"id":                    c.Id,
		"client_trigger_id": c.ClientTriggerId,
		"symbol":                c.Symbol,
		"side":                  sideToString(c.Side),
		"type":                  triggerTypeLabel(c.Type),
		"stop_price":            c.StopPrice,
		"limit_price":           c.LimitPrice,
		"qty":                   c.Qty,
		"quote_qty":             c.QuoteQty,
		"tif":                   tifToString(c.Tif),
		"status":                triggerStatusLabel(c.Status),
		"created_at_unix_ms":    c.CreatedAtUnixMs,
		"triggered_at_unix_ms":  c.TriggeredAtUnixMs,
		"placed_order_id":       c.TriggeredOrderId,
		"reject_reason":         c.RejectReason,
		"expires_at_unix_ms":    c.ExpiresAtUnixMs,
		"oco_group_id":          c.OcoGroupId,
		"trailing_delta_bps":    c.TrailingDeltaBps,
		"activation_price":      c.ActivationPrice,
		"trailing_watermark":    c.TrailingWatermark,
		"trailing_active":       c.TrailingActive,
		"source":                "history",
	}
}

func parseTriggerType(s string) (condrpc.TriggerType, error) {
	switch s {
	case "stop_loss", "STOP_LOSS":
		return condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS, nil
	case "stop_loss_limit", "STOP_LOSS_LIMIT":
		return condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT, nil
	case "take_profit", "TAKE_PROFIT":
		return condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT, nil
	case "take_profit_limit", "TAKE_PROFIT_LIMIT":
		return condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT_LIMIT, nil
	case "trailing_stop_loss", "TRAILING_STOP_LOSS":
		return condrpc.TriggerType_TRIGGER_TYPE_TRAILING_STOP_LOSS, nil
	}
	return condrpc.TriggerType_TRIGGER_TYPE_UNSPECIFIED, badRequest("type", s)
}

func triggerTypeLabel(t condrpc.TriggerType) string {
	switch t {
	case condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS:
		return "stop_loss"
	case condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT:
		return "stop_loss_limit"
	case condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT:
		return "take_profit"
	case condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT_LIMIT:
		return "take_profit_limit"
	case condrpc.TriggerType_TRIGGER_TYPE_TRAILING_STOP_LOSS:
		return "trailing_stop_loss"
	}
	return ""
}

func triggerStatusLabel(s condrpc.TriggerStatus) string {
	switch s {
	case condrpc.TriggerStatus_TRIGGER_STATUS_PENDING:
		return "pending"
	case condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED:
		return "triggered"
	case condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED:
		return "canceled"
	case condrpc.TriggerStatus_TRIGGER_STATUS_REJECTED:
		return "rejected"
	case condrpc.TriggerStatus_TRIGGER_STATUS_EXPIRED:
		return "expired"
	}
	return "unknown"
}
