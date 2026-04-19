package rest

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/auth"
)

// applySlippage adjusts lastPrice by slippageBps for the given side:
//
//	buy  → price × (1 + bps/10000)  (willing to pay up to this much)
//	sell → price × (1 - bps/10000)  (willing to sell down to this much)
//
// Rounded to 8 decimals, plenty for every listed symbol today. Returns
// the decimal string so caller can forward it verbatim.
func applySlippage(lastPrice string, bps int, side eventpb.Side) (string, error) {
	last, err := decimal.NewFromString(lastPrice)
	if err != nil {
		return "", fmt.Errorf("invalid last_price %q: %w", lastPrice, err)
	}
	if last.Sign() <= 0 {
		return "", fmt.Errorf("last_price must be > 0")
	}
	if bps <= 0 || bps > 10_000 {
		return "", fmt.Errorf("slippage_bps must be in (0, 10000]")
	}
	delta := last.Mul(decimal.NewFromInt(int64(bps))).Div(decimal.NewFromInt(10_000))
	var out decimal.Decimal
	switch side {
	case eventpb.Side_SIDE_BUY:
		out = last.Add(delta)
	case eventpb.Side_SIDE_SELL:
		out = last.Sub(delta)
	default:
		return "", fmt.Errorf("invalid side for slippage")
	}
	if out.Sign() <= 0 {
		return "", fmt.Errorf("protected price went non-positive")
	}
	return out.Truncate(8).String(), nil
}

// bestEffortMidPrice returns (bestBid+bestAsk)/2 from BFF's market cache as
// a decimal string, or "" when a mid-price cannot be derived (no cache, no
// snapshot yet, or one side empty). Counter treats "" as "reference price
// unavailable" and falls back to the ADR-0053 M3 behaviour of skipping
// MARKET-by-base precision (safer than over-rejecting during BFF cold
// start).
func (s *Server) bestEffortMidPrice(symbol string) string {
	if s.market == nil {
		return ""
	}
	snap := s.market.DepthSnapshot(symbol)
	if snap == nil || len(snap.Bids) == 0 || len(snap.Asks) == 0 {
		return ""
	}
	bid, err := decimal.NewFromString(snap.Bids[0].Price)
	if err != nil || bid.Sign() <= 0 {
		return ""
	}
	ask, err := decimal.NewFromString(snap.Asks[0].Price)
	if err != nil || ask.Sign() <= 0 {
		return ""
	}
	return bid.Add(ask).Div(decimal.NewFromInt(2)).Truncate(12).String()
}

type placeOrderBody struct {
	ClientOrderID string `json:"client_order_id,omitempty"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`       // "buy" / "sell"
	OrderType     string `json:"order_type"` // "limit" / "market"
	TIF           string `json:"tif"`        // "gtc" / "ioc" / "fok" / "post_only"; ignored for market
	Price         string `json:"price,omitempty"`
	Qty           string `json:"qty,omitempty"`        // base qty; empty for market buy with quote_qty
	QuoteQty      string `json:"quote_qty,omitempty"`  // market buy quote budget (BN quoteOrderQty, ADR-0035)

	// Optional slippage protection for market orders (ADR-0035 §路径 B).
	// When SlippageBps > 0 the client MUST also supply LastPrice; BFF
	// rewrites the order to LIMIT+IOC with price = LastPrice × (1 ± slippage)
	// before forwarding to counter. Not a server-side concern beyond BFF.
	LastPrice   string `json:"last_price,omitempty"`
	SlippageBps int    `json:"slippage_bps,omitempty"`
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
	if body.Symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol is required")
		return
	}

	// Translate MARKET + slippage_bps into LIMIT + IOC with a protective
	// price (ADR-0035 §路径 B). When slippage_bps is 0/absent we forward
	// the MARKET straight through for the server-side native path.
	if ot == eventpb.OrderType_ORDER_TYPE_MARKET && body.SlippageBps > 0 {
		if body.LastPrice == "" {
			writeError(w, http.StatusBadRequest,
				"slippage_bps requires last_price from the client (ADR-0035)")
			return
		}
		if body.QuoteQty != "" {
			writeError(w, http.StatusBadRequest,
				"slippage_bps with quote_qty is ambiguous; drop one or the other (ADR-0035)")
			return
		}
		protectedPrice, perr := applySlippage(body.LastPrice, body.SlippageBps, side)
		if perr != nil {
			writeError(w, http.StatusBadRequest, perr.Error())
			return
		}
		body.Price = protectedPrice
		body.TIF = "ioc"
		ot = eventpb.OrderType_ORDER_TYPE_LIMIT
	}

	// Validate the shape now that translation (if any) settled.
	switch ot {
	case eventpb.OrderType_ORDER_TYPE_LIMIT:
		if body.Qty == "" || body.Price == "" {
			writeError(w, http.StatusBadRequest, "limit orders require qty and price")
			return
		}
	case eventpb.OrderType_ORDER_TYPE_MARKET:
		if side == eventpb.Side_SIDE_BUY {
			if body.QuoteQty == "" {
				writeError(w, http.StatusBadRequest,
					"market buy requires quote_qty (ADR-0035); for qty-based market buy, "+
						"translate client-side via last_price + slippage_bps")
				return
			}
			if body.Qty != "" {
				writeError(w, http.StatusBadRequest,
					"market buy: pass either qty (with slippage_bps + last_price) or quote_qty, not both")
				return
			}
		} else {
			if body.Qty == "" {
				writeError(w, http.StatusBadRequest, "market sell requires qty")
				return
			}
		}
	}

	tif, err := parseTIF(body.TIF)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// ADR-0053 M3.b: best-effort mid-price for counter-side MARKET-by-base
	// precision validation. Only relevant when a MARKET-sell or MARKET-buy-
	// by-qty path reaches counter (i.e. not the LIMIT+IOC slippage-
	// protected rewrite, which already carries a hard Price). We compute it
	// unconditionally; counter ignores reference_price for LIMIT /
	// MarketBuyByQuote anyway.
	referencePrice := s.bestEffortMidPrice(body.Symbol)

	resp, err := s.counter.PlaceOrder(r.Context(), &counterrpc.PlaceOrderRequest{
		UserId:         userID,
		ClientOrderId:  body.ClientOrderID,
		Symbol:         body.Symbol,
		Side:           side,
		OrderType:      ot,
		Tif:            tif,
		Price:          body.Price,
		Qty:            body.Qty,
		QuoteQty:       body.QuoteQty,
		ReferencePrice: referencePrice,
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
		// internal_status surfaces the 8-state machine (ADR-0020) so callers
		// that need to distinguish e.g. PENDING_CANCEL from NEW — which the
		// coarse external status folds together — can act on the real state.
		"internal_status": internalStatusString(resp.Status),
		"created_at":      resp.CreatedAtUnixMs,
		"updated_at":      resp.UpdatedAtUnixMs,
	})
}

// internalStatusString exposes the raw InternalOrderStatus as a snake-case
// string. Used by the dev UI to filter orders whose cancel is in flight
// (PENDING_CANCEL) from the open list without conflating them with live
// cancelable NEW orders.
func internalStatusString(s eventpb.InternalOrderStatus) string {
	switch s {
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW:
		return "pending_new"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW:
		return "new"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED:
		return "partially_filled"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED:
		return "filled"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL:
		return "pending_cancel"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED:
		return "canceled"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED:
		return "rejected"
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED:
		return "expired"
	}
	return "unspecified"
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
