package rest

// perp_product.go is the ADR-0078 user-plane REST surface: amend (§2),
// batch place/cancel + cancel-all (§3), pre-check (§4), close-all (§5).
// Admin-plane ops (force adjust / block trade) are deliberately NOT routed
// here — they live behind admin-gateway only.

import (
	"net/http"

	"connectrpc.com/connect"

	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
)

type perpAmendBody struct {
	OrderID  uint64 `json:"order_id"`
	NewPrice string `json:"new_price"`
	NewQty   string `json:"new_qty"` // new TOTAL intent qty (ADR-0078 §2)
}

// handlePerpAmendOrder POST /v1/perp/order/amend.
func (s *Server) handlePerpAmendOrder(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpAmendBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.perp.AmendOrder(r.Context(), connect.NewRequest(&perprpc.AmendOrderRequest{
		UserId: userID, OrderId: body.OrderID,
		NewPrice: body.NewPrice, NewQty: body.NewQty,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":            resp.Msg.Accepted,
		"reject_reason":       resp.Msg.RejectReason,
		"old_order_id":        resp.Msg.OldOrderId,
		"new_order_id":        resp.Msg.NewOrderId,
		"received_ts_unix_ms": resp.Msg.ReceivedTsUnixMs,
	})
}

type perpBatchPlaceBody struct {
	BatchID string               `json:"batch_id,omitempty"`
	Items   []perpPlaceOrderBody `json:"items"`
}

// handlePerpBatchPlace POST /v1/perp/orders/batch.
func (s *Server) handlePerpBatchPlace(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpBatchPlaceBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	req := &perprpc.BatchPlaceOrdersRequest{UserId: userID, BatchId: body.BatchID}
	for i := range body.Items {
		item, err := perpPlaceOrderRequest(userID, &body.Items[i])
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		req.Items = append(req.Items, item)
	}
	resp, err := s.perp.BatchPlaceOrders(r.Context(), connect.NewRequest(req))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	items := make([]map[string]any, 0, len(resp.Msg.Items))
	for _, it := range resp.Msg.Items {
		items = append(items, map[string]any{
			"order_id":        it.OrderId,
			"client_order_id": it.ClientOrderId,
			"accepted":        it.Accepted,
			"reject_reason":   it.RejectReason,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"batch_id": resp.Msg.BatchId,
		"items":    items,
	})
}

type perpBatchCancelBody struct {
	BatchID  string   `json:"batch_id,omitempty"`
	OrderIDs []uint64 `json:"order_ids"`
}

// handlePerpBatchCancel POST /v1/perp/orders/batch-cancel.
func (s *Server) handlePerpBatchCancel(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpBatchCancelBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.perp.BatchCancelOrders(r.Context(), connect.NewRequest(&perprpc.BatchCancelOrdersRequest{
		UserId: userID, BatchId: body.BatchID, OrderIds: body.OrderIDs,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"batch_id": resp.Msg.BatchId,
		"items":    cancelItemsJSON(resp.Msg.Items),
	})
}

// handlePerpCancelAll DELETE /v1/perp/orders?symbol= (mirrors the spot
// DELETE /v1/orders shape).
func (s *Server) handlePerpCancelAll(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	resp, err := s.perp.CancelAllOrders(r.Context(), connect.NewRequest(&perprpc.CancelAllOrdersRequest{
		UserId: userID, Symbol: r.URL.Query().Get("symbol"),
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": cancelItemsJSON(resp.Msg.Items),
	})
}

func cancelItemsJSON(items []*perprpc.CancelOrderResponse) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		out = append(out, map[string]any{
			"order_id":      it.OrderId,
			"accepted":      it.Accepted,
			"reject_reason": it.RejectReason,
		})
	}
	return out
}

// handlePerpPreCheckOrder POST /v1/perp/order/pre-check — the ADR-0078 §4
// dry-run estimate (no reservation, no lock-in).
func (s *Server) handlePerpPreCheckOrder(w http.ResponseWriter, r *http.Request) {
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
	order, err := perpPlaceOrderRequest(userID, &body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.perp.PreCheckOrder(r.Context(), connect.NewRequest(&perprpc.PreCheckOrderRequest{Order: order}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"would_accept":            resp.Msg.WouldAccept,
		"reject_reason":           resp.Msg.RejectReason,
		"required_initial_margin": resp.Msg.RequiredInitialMargin,
		"fee_buffer":              resp.Msg.FeeBuffer,
		"effective_leverage":      resp.Msg.EffectiveLeverage,
		"margin_mode":             perpMarginModeToString(resp.Msg.MarginMode),
		"risk_id":                 resp.Msg.RiskId,
		"max_open_qty":            resp.Msg.MaxOpenQty,
		"config_version":          resp.Msg.ConfigVersion,
	})
}

type perpCloseAllBody struct {
	Symbol      string `json:"symbol,omitempty"` // empty = all symbols in scope
	SlippageBps uint32 `json:"slippage_bps"`     // required (0, 10000]: ADR-0083 collar
	ClientOpID  string `json:"client_op_id"`     // idempotency key (required)
}

// handlePerpCloseAll POST /v1/perp/close-all — conservative two-phase
// close-all (ADR-0078 §5). A repeat with the same client_op_id returns the
// run's current phase + legs.
func (s *Server) handlePerpCloseAll(w http.ResponseWriter, r *http.Request) {
	if !s.requirePerp(w) {
		return
	}
	userID, ok := perpUserID(w, r)
	if !ok {
		return
	}
	var body perpCloseAllBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.perp.CloseAllPositions(r.Context(), connect.NewRequest(&perprpc.CloseAllPositionsRequest{
		UserId: userID, Symbol: body.Symbol,
		SlippageBps: body.SlippageBps, ClientOpId: body.ClientOpID,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	legs := make([]map[string]any, 0, len(resp.Msg.Legs))
	for _, leg := range resp.Msg.Legs {
		legs = append(legs, map[string]any{
			"symbol":        leg.Symbol,
			"position_idx":  leg.PositionIdx,
			"order_id":      leg.OrderId,
			"qty":           leg.Qty,
			"reject_reason": leg.RejectReason,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":      resp.Msg.Accepted,
		"reject_reason": resp.Msg.RejectReason,
		"close_all_id":  resp.Msg.CloseAllId,
		"phase":         closeAllPhaseLabel(resp.Msg.Phase),
		"legs":          legs,
	})
}

func closeAllPhaseLabel(p perprpc.CloseAllPhase) string {
	switch p {
	case perprpc.CloseAllPhase_CLOSE_ALL_PHASE_CANCELING:
		return "canceling"
	case perprpc.CloseAllPhase_CLOSE_ALL_PHASE_PLACING:
		return "placing"
	case perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE:
		return "done"
	case perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE_WITH_ERRORS:
		return "done_with_errors"
	default:
		return "unspecified"
	}
}
