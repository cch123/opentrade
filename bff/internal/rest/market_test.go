package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/bff/internal/marketcache"
)

func newServerWithMarket(cache *marketcache.Cache) *Server {
	return NewServer(Config{
		UserRateLimit: 1000, UserRateWindow: time.Second,
		IPRateLimit: 1000, IPRateWindow: time.Second,
	}, &fakeCounter{}, nil, cache, nil, nil, zap.NewNop())
}

func TestDepthSnapshot_503WhenNoCache(t *testing.T) {
	srv := newServerWithMarket(nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/depth/BTC-USDT", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, body = %s", rr.Code, rr.Body.String())
	}
}

func TestDepthSnapshot_404BeforeFirstWrite(t *testing.T) {
	cache := marketcache.New(marketcache.Config{})
	srv := newServerWithMarket(cache)
	req := httptest.NewRequest(http.MethodGet, "/v1/depth/BTC-USDT", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("code = %d", rr.Code)
	}
}

func TestDepthSnapshot_ReturnsLatest(t *testing.T) {
	cache := marketcache.New(marketcache.Config{})
	cache.PutOrderBookFull("BTC-USDT", 42, &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{{Price: "99", Qty: "1"}},
		Asks: []*eventpb.OrderBookLevel{{Price: "101", Qty: "2"}},
	})
	srv := newServerWithMarket(cache)
	req := httptest.NewRequest(http.MethodGet, "/v1/depth/BTC-USDT", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["symbol"] != "BTC-USDT" {
		t.Errorf("symbol: %v", got["symbol"])
	}
	bids, _ := got["bids"].([]any)
	if len(bids) != 1 {
		t.Errorf("bids: %+v", got["bids"])
	}
}

func TestKlinesRecent_ReturnsBuffer(t *testing.T) {
	cache := marketcache.New(marketcache.Config{KlineBuffer: 5})
	for i := int64(0); i < 3; i++ {
		cache.AppendKlineClosed("BTC-USDT", &eventpb.KlineClosed{Kline: &eventpb.Kline{
			Symbol:     "BTC-USDT",
			Interval:   eventpb.KlineInterval_KLINE_INTERVAL_1M,
			OpenTimeMs: 60_000 * i,
		}})
	}
	srv := newServerWithMarket(cache)
	req := httptest.NewRequest(http.MethodGet, "/v1/klines/BTC-USDT?interval=1m&limit=10", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &got)
	if got["interval"] != "1m" {
		t.Errorf("interval: %v", got["interval"])
	}
	klines, _ := got["klines"].([]any)
	if len(klines) != 3 {
		t.Errorf("klines len: %d", len(klines))
	}
}

func TestKlinesRecent_DefaultsTo1m(t *testing.T) {
	cache := marketcache.New(marketcache.Config{})
	cache.AppendKlineClosed("ETH-USDT", &eventpb.KlineClosed{Kline: &eventpb.Kline{
		Symbol: "ETH-USDT", Interval: eventpb.KlineInterval_KLINE_INTERVAL_1M, OpenTimeMs: 1,
	}})
	srv := newServerWithMarket(cache)
	req := httptest.NewRequest(http.MethodGet, "/v1/klines/ETH-USDT", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &got)
	if got["interval"] != "1m" {
		t.Errorf("default interval: %v", got["interval"])
	}
}

func TestKlinesRecent_BadInterval(t *testing.T) {
	cache := marketcache.New(marketcache.Config{})
	srv := newServerWithMarket(cache)
	req := httptest.NewRequest(http.MethodGet, "/v1/klines/BTC-USDT?interval=2m", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
}

func TestKlinesRecent_NegativeLimit(t *testing.T) {
	cache := marketcache.New(marketcache.Config{})
	srv := newServerWithMarket(cache)
	req := httptest.NewRequest(http.MethodGet, "/v1/klines/BTC-USDT?limit=-5", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
}
