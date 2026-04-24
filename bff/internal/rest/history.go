package rest

import (
	"net/http"
	"strconv"
	"strings"

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
	resp, err := s.history.ListOrders(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(resp.Orders))
	for _, o := range resp.Orders {
		out = append(out, orderToJSON(o))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"orders":      out,
		"next_cursor": resp.NextCursor,
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
	resp, err := s.history.ListTrades(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(resp.Trades))
	for _, t := range resp.Trades {
		out = append(out, tradeToJSON(t))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"trades":      out,
		"next_cursor": resp.NextCursor,
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
	resp, err := s.history.ListAccountLogs(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(resp.Logs))
	for _, l := range resp.Logs {
		out = append(out, accountLogToJSON(l))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"logs":        out,
		"next_cursor": resp.NextCursor,
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

// ---------------------------------------------------------------------------
// GET /v1/transfers  (history / projection)
// GET /v1/transfers/{transfer_id}  (single row)
//
// These are historical views. For in-flight saga polling use GET
// /v1/transfer/{transfer_id} which reads asset-service.transfer_ledger
// directly (ADR-0057 M4).
// ---------------------------------------------------------------------------

func (s *Server) handleListTransfers(w http.ResponseWriter, r *http.Request) {
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
	scope, err := parseTransferScope(q.Get("scope"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	states := splitCSVParam(q.Get("state"))
	req := &historypb.ListTransfersRequest{
		UserId:  userID,
		FromBiz: q.Get("from_biz"),
		ToBiz:   q.Get("to_biz"),
		Asset:   q.Get("asset"),
		Scope:   scope,
		States:  states,
		SinceMs: parseInt64Query(q.Get("since_ms")),
		UntilMs: parseInt64Query(q.Get("until_ms")),
		Cursor:  q.Get("cursor"),
		Limit:   parseInt32Query(q.Get("limit")),
	}
	resp, err := s.history.ListTransfers(r.Context(), req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(resp.Transfers))
	for _, t := range resp.Transfers {
		out = append(out, historyTransferToJSON(t))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"transfers":   out,
		"next_cursor": resp.NextCursor,
	})
}

func (s *Server) handleGetHistoryTransfer(w http.ResponseWriter, r *http.Request) {
	if s.history == nil {
		writeError(w, http.StatusServiceUnavailable, "history service not configured")
		return
	}
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	id := r.PathValue("transfer_id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "transfer_id required")
		return
	}
	resp, err := s.history.GetTransfer(r.Context(), &historypb.GetTransferRequest{
		UserId:     userID,
		TransferId: id,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, historyTransferToJSON(resp.Transfer))
}

func parseTransferScope(s string) (historypb.TransferScope, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "all":
		return historypb.TransferScope_TRANSFER_SCOPE_ALL, nil
	case "in_flight", "in-flight", "inflight", "pending":
		return historypb.TransferScope_TRANSFER_SCOPE_IN_FLIGHT, nil
	case "terminal", "done", "final":
		return historypb.TransferScope_TRANSFER_SCOPE_TERMINAL, nil
	}
	return historypb.TransferScope_TRANSFER_SCOPE_UNSPECIFIED, badRequest("scope", s)
}

func splitCSVParam(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func historyTransferToJSON(t *historypb.Transfer) map[string]any {
	return map[string]any{
		"transfer_id":        t.TransferId,
		"user_id":            t.UserId,
		"from_biz":           t.FromBiz,
		"to_biz":             t.ToBiz,
		"asset":              t.Asset,
		"amount":             t.Amount,
		"state":              t.State,
		"reject_reason":      t.RejectReason,
		"created_at_unix_ms": t.CreatedAtUnixMs,
		"updated_at_unix_ms": t.UpdatedAtUnixMs,
	}
}

