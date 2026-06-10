package rest

import (
	"errors"
	"net/http"
	"strconv"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/auth"
)

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
	snap := s.market.OrderBook(symbol)
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
	Qty           string `json:"qty,omitempty"`       // base qty; empty for market buy with quote_qty
	QuoteQty      string `json:"quote_qty,omitempty"` // market buy quote budget (BN quoteOrderQty, ADR-0035)

	// ADR-0083 native protected market order: BFF forwards both fields
	// verbatim; Match derives the collar from the opposite best price at
	// execution time. SlippageBps in (0, 10000], market orders only.
	// QuoteCap is required for (and only valid on) a protected market buy
	// by base qty — Counter freezes exactly that amount. Clients that want
	// protection relative to the price they SAW (the retired ADR-0035 路径
	// B semantics) submit a LIMIT IOC with a self-computed price instead.
	SlippageBps int    `json:"slippage_bps,omitempty"`
	QuoteCap    string `json:"quote_cap,omitempty"`
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

	// ADR-0083: slippage protection is native — the collar is derived by
	// Match from its own book at execution time; BFF only shape-checks.
	if body.SlippageBps != 0 {
		if body.SlippageBps < 0 || body.SlippageBps > 10_000 {
			writeError(w, http.StatusBadRequest, "slippage_bps must be in (0, 10000] (ADR-0083)")
			return
		}
		if ot != eventpb.OrderType_ORDER_TYPE_MARKET {
			writeError(w, http.StatusBadRequest,
				"slippage_bps is only valid on market orders (ADR-0083); for a limit-priced "+
					"protection submit LIMIT IOC with your own price")
			return
		}
	}

	switch ot {
	case eventpb.OrderType_ORDER_TYPE_LIMIT:
		if body.Qty == "" || body.Price == "" {
			writeError(w, http.StatusBadRequest, "limit orders require qty and price")
			return
		}
		if body.QuoteCap != "" {
			writeError(w, http.StatusBadRequest, "quote_cap is only valid on a protected market buy (ADR-0083)")
			return
		}
	case eventpb.OrderType_ORDER_TYPE_MARKET:
		if side == eventpb.Side_SIDE_BUY {
			switch {
			case body.QuoteQty != "":
				if body.Qty != "" {
					writeError(w, http.StatusBadRequest,
						"market buy: pass either qty (protected, ADR-0083) or quote_qty, not both")
					return
				}
				if body.QuoteCap != "" {
					writeError(w, http.StatusBadRequest,
						"market buy by quote_qty must not carry quote_cap — the budget is the cap (ADR-0083)")
					return
				}
			case body.Qty != "":
				// Market buy by base qty exists only as an ADR-0083 protected
				// order: Counter freezes quote_cap, Match bounds execution by
				// min(collar, quote_cap/qty).
				if body.SlippageBps <= 0 || body.QuoteCap == "" {
					writeError(w, http.StatusBadRequest,
						"market buy by qty requires slippage_bps + quote_cap (ADR-0083); "+
							"or submit quote_qty for a budget-driven market buy (ADR-0035)")
					return
				}
			default:
				writeError(w, http.StatusBadRequest,
					"market buy requires quote_qty (ADR-0035) or qty + slippage_bps + quote_cap (ADR-0083)")
				return
			}
		} else {
			if body.Qty == "" {
				writeError(w, http.StatusBadRequest, "market sell requires qty")
				return
			}
			if body.QuoteCap != "" {
				writeError(w, http.StatusBadRequest,
					"quote_cap is only valid on a protected market buy — sells freeze base qty (ADR-0083)")
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
	// by-qty path reaches counter. We compute it unconditionally; counter
	// ignores reference_price for LIMIT / MarketBuyByQuote anyway.
	referencePrice := s.bestEffortMidPrice(body.Symbol)

	resp, err := s.counter.PlaceOrder(r.Context(), connect.NewRequest(&counterrpc.PlaceOrderRequest{
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
		SlippageBps:    uint32(body.SlippageBps),
		QuoteCap:       body.QuoteCap,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"order_id":            resp.Msg.OrderId,
		"client_order_id":     resp.Msg.ClientOrderId,
		"status":              "new", // MVP: external status always 'new' at placement
		"accepted":            resp.Msg.Accepted,
		"received_ts_unix_ms": resp.Msg.ReceivedTsUnixMs,
	})
}

// handleCancelMyOrders bulk-cancels the caller's live orders on their
// Counter shard. Optional `?symbol=X` narrows the scope to one symbol;
// absent query means "every open order for this user". Returns the
// per-shard counts (no fan-out — a user lives on a single shard).
func (s *Server) handleCancelMyOrders(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	symbol := r.URL.Query().Get("symbol")
	resp, err := s.counter.CancelMyOrders(r.Context(), connect.NewRequest(&counterrpc.CancelMyOrdersRequest{
		UserId: userID,
		Symbol: symbol,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cancelled": resp.Msg.Cancelled,
		"skipped":   resp.Msg.Skipped,
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
	resp, err := s.counter.CancelOrder(r.Context(), connect.NewRequest(&counterrpc.CancelOrderRequest{
		UserId:  userID,
		OrderId: orderID,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"order_id": resp.Msg.OrderId,
		"accepted": resp.Msg.Accepted,
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
	resp, err := s.counter.QueryOrder(r.Context(), connect.NewRequest(&counterrpc.QueryOrderRequest{
		UserId:  userID,
		OrderId: orderID,
	}))
	if err != nil {
		writeConnectError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"order_id":        resp.Msg.OrderId,
		"client_order_id": resp.Msg.ClientOrderId,
		"symbol":          resp.Msg.Symbol,
		"side":            sideToString(resp.Msg.Side),
		"order_type":      orderTypeToString(resp.Msg.OrderType),
		"tif":             tifToString(resp.Msg.Tif),
		"price":           resp.Msg.Price,
		"qty":             resp.Msg.Qty,
		"filled_qty":      resp.Msg.FilledQty,
		"frozen_amount":   resp.Msg.FrozenAmt,
		"status":          externalStatusFromInternal(resp.Msg.Status),
		// internal_status surfaces the 8-state machine (ADR-0020) so callers
		// that need to distinguish e.g. PENDING_CANCEL from NEW — which the
		// coarse external status folds together — can act on the real state.
		"internal_status": internalStatusString(resp.Msg.Status),
		"created_at":      resp.Msg.CreatedAtUnixMs,
		"updated_at":      resp.Msg.UpdatedAtUnixMs,
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
// Connect -> HTTP error mapping
// ---------------------------------------------------------------------------

func writeConnectError(w http.ResponseWriter, err error) {
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	switch cerr.Code() {
	case connect.CodeInvalidArgument:
		writeError(w, http.StatusBadRequest, cerr.Message())
	case connect.CodeNotFound:
		writeError(w, http.StatusNotFound, cerr.Message())
	case connect.CodeFailedPrecondition, connect.CodeAlreadyExists:
		writeError(w, http.StatusConflict, cerr.Message())
	case connect.CodeUnauthenticated:
		writeError(w, http.StatusUnauthorized, cerr.Message())
	case connect.CodePermissionDenied:
		writeError(w, http.StatusForbidden, cerr.Message())
	case connect.CodeResourceExhausted:
		writeError(w, http.StatusTooManyRequests, cerr.Message())
	case connect.CodeUnavailable:
		writeError(w, http.StatusServiceUnavailable, cerr.Message())
	default:
		writeError(w, http.StatusInternalServerError, cerr.Message())
	}
}

type validationErr struct {
	field, value string
}

func (e *validationErr) Error() string { return "invalid " + e.field + ": " + e.value }

func badRequest(field, value string) error { return &validationErr{field, value} }
