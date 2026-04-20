package rest

import (
	"net/http"
	"strconv"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// GET /v1/depth/{symbol} — returns the latest OrderBook Full frame BFF
// has seen on market-data (ADR-0055). Clients call this after a disconnect
// to resync their local book before resubscribing to the live WS stream
// (ADR-0038).
func (s *Server) handleDepthSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.market == nil {
		writeError(w, http.StatusServiceUnavailable, "market-data cache not configured")
		return
	}
	symbol := r.PathValue("symbol")
	if symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol required")
		return
	}
	snap := s.market.OrderBook(symbol)
	if snap == nil {
		writeError(w, http.StatusNotFound, "no snapshot yet for symbol")
		return
	}
	// protojson would be heavier; the wire form matches the WS payload
	// (OrderBookLevel is plain strings), so stock json.Encode is fine here.
	writeJSON(w, http.StatusOK, map[string]any{
		"symbol":       snap.Symbol,
		"match_seq_id": snap.MatchSeqID,
		"bids":         snap.Bids,
		"asks":         snap.Asks,
	})
}

// GET /v1/klines/{symbol}?interval=1m&limit=N — returns the most recently
// closed bars BFF has seen from market-data. Defaults: interval=1m,
// limit=200; max limit = Cache.KlineBuffer (server-side cap).
func (s *Server) handleKlinesRecent(w http.ResponseWriter, r *http.Request) {
	if s.market == nil {
		writeError(w, http.StatusServiceUnavailable, "market-data cache not configured")
		return
	}
	symbol := r.PathValue("symbol")
	if symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol required")
		return
	}
	iv, err := parseKlineInterval(r.URL.Query().Get("interval"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit := 200
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = n
	}
	bars := s.market.RecentKlines(symbol, iv, limit)
	out := make([]map[string]any, 0, len(bars))
	for _, k := range bars {
		out = append(out, map[string]any{
			"symbol":        k.Symbol,
			"interval":      klineIntervalLabel(k.Interval),
			"open_time_ms":  k.OpenTimeMs,
			"close_time_ms": k.CloseTimeMs,
			"open":          k.Open,
			"high":          k.High,
			"low":           k.Low,
			"close":         k.Close,
			"volume":        k.Volume,
			"quote_volume":  k.QuoteVolume,
			"trade_count":   k.TradeCount,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"symbol":   symbol,
		"interval": klineIntervalLabel(iv),
		"klines":   out,
	})
}

func parseKlineInterval(s string) (eventpb.KlineInterval, error) {
	switch s {
	case "", "1m":
		return eventpb.KlineInterval_KLINE_INTERVAL_1M, nil
	case "5m":
		return eventpb.KlineInterval_KLINE_INTERVAL_5M, nil
	case "15m":
		return eventpb.KlineInterval_KLINE_INTERVAL_15M, nil
	case "1h":
		return eventpb.KlineInterval_KLINE_INTERVAL_1H, nil
	case "1d":
		return eventpb.KlineInterval_KLINE_INTERVAL_1D, nil
	}
	return eventpb.KlineInterval_KLINE_INTERVAL_UNSPECIFIED, badRequest("interval", s)
}

func klineIntervalLabel(i eventpb.KlineInterval) string {
	switch i {
	case eventpb.KlineInterval_KLINE_INTERVAL_1M:
		return "1m"
	case eventpb.KlineInterval_KLINE_INTERVAL_5M:
		return "5m"
	case eventpb.KlineInterval_KLINE_INTERVAL_15M:
		return "15m"
	case eventpb.KlineInterval_KLINE_INTERVAL_1H:
		return "1h"
	case eventpb.KlineInterval_KLINE_INTERVAL_1D:
		return "1d"
	}
	return ""
}
