package rest

import (
	"net/http"
	"strconv"
	"strings"

	"connectrpc.com/connect"

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/pkg/auth"
)

// ---------------------------------------------------------------------------
// GET /v1/orders
// ---------------------------------------------------------------------------

func (s *Server) handleListOrders(w http.ResponseWriter, r *http.Request) {
	if s.history == nil {
		writeError(w, http.StatusServiceUnavailable, "history service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	q := r.URL.Query()
	scope, err := parseOrderScope(q.Get("scope"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	statuses, err := parseOrderStatusCSV(q.Get("status"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	req := &historypb.ListOrdersRequest{
		UserId:   userID,
		Symbol:   q.Get("symbol"),
		Scope:    scope,
		Statuses: statuses,
		SinceMs:  parseInt64Query(q.Get("since_ms")),
		UntilMs:  parseInt64Query(q.Get("until_ms")),
		Cursor:   q.Get("cursor"),
		Limit:    parseInt32Query(q.Get("limit")),
	}
	resp, err := s.history.ListOrders(r.Context(), connect.NewRequest(req))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(resp.Msg.Orders))
	for _, o := range resp.Msg.Orders {
		out = append(out, orderToJSON(o))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"orders":      out,
		"next_cursor": resp.Msg.NextCursor,
	})
}

// ---------------------------------------------------------------------------
// GET /v1/trades
// ---------------------------------------------------------------------------

func (s *Server) handleListTrades(w http.ResponseWriter, r *http.Request) {
	if s.history == nil {
		writeError(w, http.StatusServiceUnavailable, "history service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	q := r.URL.Query()
	req := &historypb.ListTradesRequest{
		UserId:  userID,
		Symbol:  q.Get("symbol"),
		SinceMs: parseInt64Query(q.Get("since_ms")),
		UntilMs: parseInt64Query(q.Get("until_ms")),
		Cursor:  q.Get("cursor"),
		Limit:   parseInt32Query(q.Get("limit")),
	}
	resp, err := s.history.ListTrades(r.Context(), connect.NewRequest(req))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(resp.Msg.Trades))
	for _, t := range resp.Msg.Trades {
		out = append(out, tradeToJSON(t))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"trades":      out,
		"next_cursor": resp.Msg.NextCursor,
	})
}

// ---------------------------------------------------------------------------
// GET /v1/account-logs
// ---------------------------------------------------------------------------

func (s *Server) handleListAccountLogs(w http.ResponseWriter, r *http.Request) {
	if s.history == nil {
		writeError(w, http.StatusServiceUnavailable, "history service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	q := r.URL.Query()
	var bizTypes []string
	if raw := strings.TrimSpace(q.Get("biz_type")); raw != "" {
		for _, t := range strings.Split(raw, ",") {
			if t = strings.TrimSpace(t); t != "" {
				bizTypes = append(bizTypes, t)
			}
		}
	}
	req := &historypb.ListAccountLogsRequest{
		UserId:   userID,
		Asset:    q.Get("asset"),
		BizTypes: bizTypes,
		SinceMs:  parseInt64Query(q.Get("since_ms")),
		UntilMs:  parseInt64Query(q.Get("until_ms")),
		Cursor:   q.Get("cursor"),
		Limit:    parseInt32Query(q.Get("limit")),
	}
	resp, err := s.history.ListAccountLogs(r.Context(), connect.NewRequest(req))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(resp.Msg.Logs))
	for _, l := range resp.Msg.Logs {
		out = append(out, accountLogToJSON(l))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"logs":        out,
		"next_cursor": resp.Msg.NextCursor,
	})
}

// ---------------------------------------------------------------------------
// parsers + renderers
// ---------------------------------------------------------------------------

func parseOrderScope(s string) (historypb.OrderScope, error) {
	switch strings.ToLower(s) {
	case "", "all":
		return historypb.OrderScope_ORDER_SCOPE_ALL, nil
	case "open":
		return historypb.OrderScope_ORDER_SCOPE_OPEN, nil
	case "terminal":
		return historypb.OrderScope_ORDER_SCOPE_TERMINAL, nil
	}
	return 0, badRequest("scope", s)
}

func parseOrderStatusCSV(raw string) ([]historypb.OrderStatus, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var out []historypb.OrderStatus
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		st, err := parseOrderStatus(s)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

func parseOrderStatus(s string) (historypb.OrderStatus, error) {
	switch strings.ToLower(s) {
	case "new":
		return historypb.OrderStatus_ORDER_STATUS_NEW, nil
	case "partially_filled":
		return historypb.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED, nil
	case "filled":
		return historypb.OrderStatus_ORDER_STATUS_FILLED, nil
	case "canceled":
		return historypb.OrderStatus_ORDER_STATUS_CANCELED, nil
	case "rejected":
		return historypb.OrderStatus_ORDER_STATUS_REJECTED, nil
	case "expired":
		return historypb.OrderStatus_ORDER_STATUS_EXPIRED, nil
	}
	return 0, badRequest("status", s)
}

func parseInt64Query(s string) int64 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseInt32Query(s string) int32 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(n)
}

func orderStatusToString(s historypb.OrderStatus) string {
	switch s {
	case historypb.OrderStatus_ORDER_STATUS_NEW:
		return "new"
	case historypb.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED:
		return "partially_filled"
	case historypb.OrderStatus_ORDER_STATUS_FILLED:
		return "filled"
	case historypb.OrderStatus_ORDER_STATUS_CANCELED:
		return "canceled"
	case historypb.OrderStatus_ORDER_STATUS_REJECTED:
		return "rejected"
	case historypb.OrderStatus_ORDER_STATUS_EXPIRED:
		return "expired"
	}
	return "unknown"
}

func orderToJSON(o *historypb.Order) map[string]any {
	return map[string]any{
		"order_id":        o.OrderId,
		"client_order_id": o.ClientOrderId,
		"symbol":          o.Symbol,
		"side":            sideToString(o.Side),
		"order_type":      orderTypeToString(o.OrderType),
		"tif":             tifToString(o.Tif),
		"price":           o.Price,
		"qty":             o.Qty,
		"filled_qty":      o.FilledQty,
		"frozen_amount":   o.FrozenAmt,
		"status":          orderStatusToString(o.Status),
		"reject_reason":   o.RejectReason,
		"created_at":      o.CreatedAtUnixMs,
		"updated_at":      o.UpdatedAtUnixMs,
		"source":          orderSource(o.ClientOrderId),
	}
}

// orderSource surfaces "trigger" for orders placed by the trigger
// service (`client_order_id = "trig-<id>"`, per ADR-0040). Other orders
// report "user". Cheap, derived, and matches the UI tab expectations
// (MVP-15 §触发后产生的真实订单).
func orderSource(clientOrderID string) string {
	if strings.HasPrefix(clientOrderID, "trig-") {
		return "trigger"
	}
	return "user"
}

func tradeToJSON(t *historypb.Trade) map[string]any {
	return map[string]any{
		"trade_id": t.TradeId,
		"symbol":   t.Symbol,
		"price":    t.Price,
		"qty":      t.Qty,
		"role":     tradeRoleToString(t.Role),
		"order_id": t.OrderId,
		"side":     sideToString(t.Side),
		"ts":       t.TsUnixMs,
	}
}

func tradeRoleToString(r historypb.TradeRole) string {
	switch r {
	case historypb.TradeRole_TRADE_ROLE_MAKER:
		return "maker"
	case historypb.TradeRole_TRADE_ROLE_TAKER:
		return "taker"
	}
	return ""
}

func accountLogToJSON(l *historypb.AccountLog) map[string]any {
	return map[string]any{
		"shard_id":       l.ShardId,
		"counter_seq_id": l.CounterSeqId,
		"asset":          l.Asset,
		"delta_avail":    l.DeltaAvail,
		"delta_frozen":   l.DeltaFrozen,
		"avail_after":    l.AvailAfter,
		"frozen_after":   l.FrozenAfter,
		"biz_type":       l.BizType,
		"biz_ref_id":     l.BizRefId,
		"ts":             l.TsUnixMs,
	}
}


