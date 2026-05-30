package index

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/dec"
)

const reconnectDelay = 5 * time.Second

// RunExternalSources starts best-effort public market-data adapters for
// non-self sources. The composite algorithm owns safety through max-age and
// quorum, so an adapter may fail or reconnect without mutating any global
// health state beyond its latest quote aging out.
func RunExternalSources(ctx context.Context, book *SourceBook, sources []SourceConfig, logger *zap.Logger) {
	for _, src := range sources {
		if src.Self {
			continue
		}
		venue, symbol, ok := splitSourceName(src.Name)
		if !ok {
			logger.Warn("index source name must be venue:symbol", zap.String("source", src.Name))
			continue
		}
		switch venue {
		case "binance":
			go runReconnecting(ctx, src.Name, logger, func(ctx context.Context) error {
				return runBinance(ctx, book, src.Name, symbol)
			})
		case "okx":
			go runReconnecting(ctx, src.Name, logger, func(ctx context.Context) error {
				return runOKX(ctx, book, src.Name, symbol)
			})
		case "huobi", "htx":
			go runReconnecting(ctx, src.Name, logger, func(ctx context.Context) error {
				return runHuobi(ctx, book, src.Name, symbol)
			})
		default:
			logger.Warn("unsupported external index source venue",
				zap.String("source", src.Name), zap.String("venue", venue))
		}
	}
}

func runReconnecting(ctx context.Context, name string, logger *zap.Logger, run func(context.Context) error) {
	for ctx.Err() == nil {
		if err := run(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("index source disconnected; reconnecting",
				zap.String("source", name), zap.Error(err), zap.Duration("delay", reconnectDelay))
		}
		timer := time.NewTimer(reconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func splitSourceName(name string) (venue, symbol string, ok bool) {
	parts := strings.SplitN(name, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return strings.ToLower(parts[0]), parts[1], true
}

func runBinance(ctx context.Context, book *SourceBook, name, symbol string) error {
	// Binance raw streams require lowercase symbols and expose best bid/ask on
	// <symbol>@bookTicker. This adapter computes a mid so the composite layer
	// remains exchange-agnostic.
	url := "wss://stream.binance.com:9443/ws/" + strings.ToLower(symbol) + "@bookTicker"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if string(payload) == "ping" {
			_ = conn.Write(ctx, websocket.MessageText, []byte("pong"))
			continue
		}
		var msg struct {
			Bid string `json:"b"`
			Ask string `json:"a"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		upsertMid(book, name, msg.Bid, msg.Ask, time.Now().UnixMilli())
	}
}

func runOKX(ctx context.Context, book *SourceBook, name, symbol string) error {
	conn, _, err := websocket.Dial(ctx, "wss://ws.okx.com:8443/ws/v5/public", nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")
	sub := map[string]any{
		"op":   "subscribe",
		"args": []map[string]string{{"channel": "tickers", "instId": symbol}},
	}
	body, _ := json.Marshal(sub)
	if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
		return err
	}
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var msg struct {
			Data []struct {
				Bid string `json:"bidPx"`
				Ask string `json:"askPx"`
				Ts  string `json:"ts"`
			} `json:"data"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil || len(msg.Data) == 0 {
			continue
		}
		ts := parseMs(msg.Data[0].Ts)
		upsertMid(book, name, msg.Data[0].Bid, msg.Data[0].Ask, ts)
	}
}

func runHuobi(ctx context.Context, book *SourceBook, name, symbol string) error {
	conn, _, err := websocket.Dial(ctx, "wss://api.huobi.pro/ws", nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")
	sub := map[string]string{"sub": "market." + strings.ToLower(symbol) + ".bbo", "id": name}
	body, _ := json.Marshal(sub)
	if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
		return err
	}
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		payload, err = maybeGunzip(payload)
		if err != nil {
			continue
		}
		var msg struct {
			Ping int64 `json:"ping"`
			Ts   int64 `json:"ts"`
			Tick struct {
				Bid []json.RawMessage `json:"bid"`
				Ask []json.RawMessage `json:"ask"`
			} `json:"tick"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		if msg.Ping > 0 {
			pong, _ := json.Marshal(map[string]int64{"pong": msg.Ping})
			_ = conn.Write(ctx, websocket.MessageText, pong)
			continue
		}
		bid, okBid := firstDecimal(msg.Tick.Bid)
		ask, okAsk := firstDecimal(msg.Tick.Ask)
		if !okBid || !okAsk {
			continue
		}
		upsertMid(book, name, bid.String(), ask.String(), msg.Ts)
	}
}

func upsertMid(book *SourceBook, name, bidRaw, askRaw string, tsMs int64) {
	bid, errBid := dec.Parse(bidRaw)
	ask, errAsk := dec.Parse(askRaw)
	if errBid != nil || errAsk != nil || bid.Sign() <= 0 || ask.Sign() <= 0 {
		return
	}
	if tsMs <= 0 {
		tsMs = time.Now().UnixMilli()
	}
	book.Upsert(name, bid.Add(ask).Div(dec.FromInt(2)), tsMs)
}

func parseMs(s string) int64 {
	if s == "" {
		return time.Now().UnixMilli()
	}
	var v int64
	if _, err := fmt.Sscan(s, &v); err != nil || v <= 0 {
		return time.Now().UnixMilli()
	}
	return v
}

func maybeGunzip(payload []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return payload, nil
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func firstDecimal(raw []json.RawMessage) (dec.Decimal, bool) {
	if len(raw) == 0 {
		return zero, false
	}
	var s string
	if err := json.Unmarshal(raw[0], &s); err == nil {
		d, err := dec.Parse(s)
		return d, err == nil
	}
	var n json.Number
	if err := json.Unmarshal(raw[0], &n); err == nil {
		d, err := dec.Parse(n.String())
		return d, err == nil
	}
	return zero, false
}
