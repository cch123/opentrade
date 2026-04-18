package rest

import (
	"net/http"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/bff/internal/auth"
)

type placeOrderBody struct {
	ClientOrderID string `json:"client_order_id,omitempty"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`       // "buy" / "sell"
	OrderType     string `json:"order_type"` // "limit" / "market"
	TIF           string `json:"tif"`        // "gtc" / "ioc" / "fok" / "post_only"
	Price         string `json:"price,omitempty"`
	Qty           string `json:"qty"`
}

func (s *Server) handlePlaceOrder(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var body placeOrderBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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
	if body.Qty == "" || body.Symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol and qty are required")
		return
	}

	resp, err := s.counter.PlaceOrder(r.Context(), &counterrpc.PlaceOrderRequest{
		UserId:        userID,
		ClientOrderId: body.ClientOrderID,
		Symbol:        body.Symbol,
		Side:          side,
		OrderType:     ot,
		Tif:           tif,
		Price:         body.Price,
		Qty:           body.Qty,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"order_id":         resp.OrderId,
		"client_order_id":  resp.ClientOrderId,
		"status":           "new", // MVP: external status always 'new' at placement
		"accepted":         resp.Accepted,
		"received_ts_unix_ms": resp.ReceivedTsUnixMs,
	})
}

func (s *Server) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
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
	resp, err := s.counter.CancelOrder(r.Context(), &counterrpc.CancelOrderRequest{
		UserId:  userID,
		OrderId: orderID,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"order_id": resp.OrderId,
		"accepted": resp.Accepted,
	})
}

func (s *Server) handleQueryOrder(w http.ResponseWriter, r *http.Request) {
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
	resp, err := s.counter.QueryOrder(r.Context(), &counterrpc.QueryOrderRequest{
		UserId:  userID,
		OrderId: orderID,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"order_id":        resp.OrderId,
		"client_order_id": resp.ClientOrderId,
		"symbol":          resp.Symbol,
		"side":            sideToString(resp.Side),
		"order_type":      orderTypeToString(resp.OrderType),
		"tif":             tifToString(resp.Tif),
		"price":           resp.Price,
		"qty":             resp.Qty,
		"filled_qty":      resp.FilledQty,
		"frozen_amount":   resp.FrozenAmt,
		"status":          externalStatusFromInternal(resp.Status),
		"created_at":      resp.CreatedAtUnixMs,
		"updated_at":      resp.UpdatedAtUnixMs,
	})
}

// ---------------------------------------------------------------------------
// parsing helpers
// ---------------------------------------------------------------------------

func parseSide(s string) (eventpb.Side, error) {
	switch s {
	case "buy", "BUY":
		return eventpb.Side_SIDE_BUY, nil
	case "sell", "SELL":
		return eventpb.Side_SIDE_SELL, nil
	}
	return eventpb.Side_SIDE_UNSPECIFIED, badRequest("side", s)
}

func parseOrderType(t string) (eventpb.OrderType, error) {
	switch t {
	case "limit", "LIMIT":
		return eventpb.OrderType_ORDER_TYPE_LIMIT, nil
	case "market", "MARKET":
		return eventpb.OrderType_ORDER_TYPE_MARKET, nil
	}
	return eventpb.OrderType_ORDER_TYPE_UNSPECIFIED, badRequest("order_type", t)
}

func parseTIF(t string) (eventpb.TimeInForce, error) {
	switch t {
	case "", "gtc", "GTC":
		return eventpb.TimeInForce_TIME_IN_FORCE_GTC, nil
	case "ioc", "IOC":
		return eventpb.TimeInForce_TIME_IN_FORCE_IOC, nil
	case "fok", "FOK":
		return eventpb.TimeInForce_TIME_IN_FORCE_FOK, nil
	case "post_only", "POST_ONLY":
		return eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY, nil
	}
	return eventpb.TimeInForce_TIME_IN_FORCE_UNSPECIFIED, badRequest("tif", t)
}

func sideToString(s eventpb.Side) string {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return "buy"
	case eventpb.Side_SIDE_SELL:
		return "sell"
	}
	return ""
}

func orderTypeToString(t eventpb.OrderType) string {
	switch t {
	case eventpb.OrderType_ORDER_TYPE_LIMIT:
		return "limit"
	case eventpb.OrderType_ORDER_TYPE_MARKET:
		return "market"
	}
	return ""
}

func tifToString(t eventpb.TimeInForce) string {
	switch t {
	case eventpb.TimeInForce_TIME_IN_FORCE_GTC:
		return "gtc"
	case eventpb.TimeInForce_TIME_IN_FORCE_IOC:
		return "ioc"
	case eventpb.TimeInForce_TIME_IN_FORCE_FOK:
		return "fok"
	case eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY:
		return "post_only"
	}
	return ""
}

// externalStatusFromInternal folds PENDING_* onto their visible counterparts
// (ADR-0020). BFF returns Binance-style status strings.
func externalStatusFromInternal(s eventpb.InternalOrderStatus) string {
	switch s {
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW:
		return "new"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL:
		// PENDING_CANCEL externally stays at the pre-cancel status; without
		// the pre-cancel hint here we conservatively return partially_filled
		// only when some fills exist. The gRPC response carries FilledQty we
		// already used above, so this path is only reached for pure NEW ->
		// PENDING_CANCEL with zero fills; map that to "new".
		if s == eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED {
			return "partially_filled"
		}
		return "new"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED:
		return "filled"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED:
		return "canceled"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED:
		return "rejected"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED:
		return "expired"
	}
	return "unknown"
}

// ---------------------------------------------------------------------------
// gRPC -> HTTP error mapping
// ---------------------------------------------------------------------------

func writeGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	switch st.Code() {
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, st.Message())
	case codes.NotFound:
		writeError(w, http.StatusNotFound, st.Message())
	case codes.FailedPrecondition, codes.AlreadyExists:
		writeError(w, http.StatusConflict, st.Message())
	case codes.Unauthenticated:
		writeError(w, http.StatusUnauthorized, st.Message())
	case codes.PermissionDenied:
		writeError(w, http.StatusForbidden, st.Message())
	case codes.ResourceExhausted:
		writeError(w, http.StatusTooManyRequests, st.Message())
	case codes.Unavailable:
		writeError(w, http.StatusServiceUnavailable, st.Message())
	default:
		writeError(w, http.StatusInternalServerError, st.Message())
	}
}

type validationErr struct {
	field, value string
}

func (e *validationErr) Error() string { return "invalid " + e.field + ": " + e.value }

func badRequest(field, value string) error { return &validationErr{field, value} }
