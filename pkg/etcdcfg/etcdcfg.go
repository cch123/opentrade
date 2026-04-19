// Package etcdcfg reads match-shard symbol assignments out of etcd and
// delivers live updates to the runtime.
//
// Key layout (ADR-0009 / ADR-0030):
//
//	/cex/match/symbols/<SYMBOL>   JSON: {"shard":"match-0","trading":true,"version":"v1"}
//
// A Source yields the initial snapshot plus a Watch channel of future
// changes. Production uses EtcdSource backed by a clientv3.Client; tests
// use MemorySource to avoid spinning up etcd.
package etcdcfg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xargin/opentrade/pkg/dec"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// DefaultPrefix is the etcd key prefix under which match symbol configs live.
const DefaultPrefix = "/cex/match/symbols/"

// SymbolConfig is the JSON value stored at each symbol key.
//
// Precision fields (ADR-0053) are additive and zero-value = compatibility
// mode: the match / counter runtimes skip all precision validation when
// Tiers is empty, preserving pre-ADR-0053 behaviour. M2 of the migration
// backfills Tiers; M3 flips the per-symbol strict flag.
type SymbolConfig struct {
	// Shard identifies the match instance that owns this symbol. Operators
	// set this to the instance's --shard-id (e.g. "match-0"). The match
	// runtime compares to its own shard id when filtering.
	Shard string `json:"shard"`
	// Trading reports whether the symbol accepts new orders. False means
	// "symbol present in config but temporarily suspended" — see ADR-0030
	// migration runbook.
	Trading bool `json:"trading"`
	// Version is a free-form tag for canary rollouts; ignored by the
	// runtime but surfaced in logs.
	Version string `json:"version,omitempty"`

	// BaseAsset / QuoteAsset identify the traded pair's constituent assets
	// (e.g. "BTC" / "USDT"). Empty = compatibility mode, inferred by callers
	// where needed. Populated by admin-gateway during M2 backfill.
	BaseAsset  string `json:"base_asset,omitempty"`
	QuoteAsset string `json:"quote_asset,omitempty"`

	// PrecisionVersion monotonically increases on every precision change so
	// downstream consumers can distinguish orders admitted pre- vs post-
	// switch. Zero = compatibility mode (no precision regime).
	PrecisionVersion uint64 `json:"precision_version,omitempty"`

	// Tiers describes the precision ranges keyed by mid-price at order
	// admission. Must pass ValidateTiers. Empty = compatibility mode.
	Tiers []PrecisionTier `json:"tiers,omitempty"`

	// ScheduledChange, when non-nil, describes the next pending tier
	// migration (ADR-0053 § 6 rollout protocol). Match / counter consult it
	// to schedule the atomic switch at EffectiveAt.
	ScheduledChange *PrecisionChange `json:"scheduled_change,omitempty"`
}

// Owned reports whether this symbol should run on shardID right now.
func (c SymbolConfig) Owned(shardID string) bool {
	return c.Shard == shardID && c.Trading
}

// HasPrecision reports whether the config carries any tier information at
// all. Callers use this as the "compatibility mode" predicate: no tiers =
// skip precision validation entirely.
func (c SymbolConfig) HasPrecision() bool {
	return len(c.Tiers) > 0
}

// PrecisionTier describes one precision range within a symbol's Tiers list.
// Field names follow ADR-0053's glossary (direct English, not Binance-style
// filter nesting) — see docs/adr/0053 § Glossary for the industry mapping.
//
// Validation lives in ValidateTiers / ValidateTierEvolution. Zero values
// on optional fields (Min/Max guardrails, QuoteStepSize, MinQuoteAmount)
// mean "no check for this axis".
type PrecisionTier struct {
	// PriceFrom / PriceTo describe the half-open range [PriceFrom, PriceTo)
	// over which this tier applies. PriceTo == 0 means "+∞" and is legal
	// only on the last tier. First tier's PriceFrom must be 0.
	PriceFrom dec.Decimal `json:"price_from"`
	PriceTo   dec.Decimal `json:"price_to"`

	// TickSize: minimum price increment. Limit-order Price must be a
	// multiple of TickSize.
	TickSize dec.Decimal `json:"tick_size"`

	// StepSize: minimum base-asset qty increment (limit orders + MARKET
	// sell + MARKET buy by base). Qty must be a multiple of StepSize.
	StepSize dec.Decimal `json:"step_size"`

	// QuoteStepSize: minimum quote-asset qty increment (MARKET buy by
	// quote, ADR-0035). QuoteQty must be a multiple of QuoteStepSize.
	// Zero = no quote-step check.
	QuoteStepSize dec.Decimal `json:"quote_step_size,omitempty"`

	// MinQty / MaxQty: base-qty lower / upper guardrails. Zero = no
	// check for that side.
	MinQty dec.Decimal `json:"min_qty,omitempty"`
	MaxQty dec.Decimal `json:"max_qty,omitempty"`

	// MinQuoteQty: quote-qty lower bound for MARKET buy by quote. Zero =
	// no check.
	MinQuoteQty dec.Decimal `json:"min_quote_qty,omitempty"`

	// MinQuoteAmount: minimum order amount (price × qty). Equivalent to
	// Binance MIN_NOTIONAL / Bybit minOrderAmt — see ADR-0053 Glossary.
	// Zero = no check.
	MinQuoteAmount dec.Decimal `json:"min_quote_amount,omitempty"`

	// MarketMinQty / MarketMaxQty: MARKET-order-specific base-qty
	// guardrails (Binance MARKET_LOT_SIZE). Zero = fall back to
	// MinQty / MaxQty respectively.
	MarketMinQty dec.Decimal `json:"market_min_qty,omitempty"`
	MarketMaxQty dec.Decimal `json:"market_max_qty,omitempty"`

	// EnforceMinQuoteAmountOnMarket: when true, MARKET orders must also
	// clear MinQuoteAmount (estimated via rolling average price supplied
	// by caller). Binance MIN_NOTIONAL.applyToMarket equivalent.
	EnforceMinQuoteAmountOnMarket bool `json:"enforce_min_quote_amount_on_market,omitempty"`

	// AvgPriceMins: rolling-window minutes for estimating MARKET amount
	// when EnforceMinQuoteAmountOnMarket is true. Consumed by the quote
	// service; zero means "use server default" (5 minutes).
	AvgPriceMins uint32 `json:"avg_price_mins,omitempty"`
}

// PrecisionChange describes a scheduled tier migration (ADR-0053 § 6).
// admin-gateway stages it via PUT /admin/symbols/{symbol}/precision;
// match / counter watch etcd for it and execute the swap at EffectiveAt.
type PrecisionChange struct {
	// EffectiveAt is the absolute UTC moment the new tiers take over.
	EffectiveAt time.Time `json:"effective_at"`

	// NewTiers is the tier list to install. Must pass ValidateTiers on
	// its own and ValidateTierEvolution(old.Tiers, NewTiers).
	NewTiers []PrecisionTier `json:"new_tiers"`

	// NewPrecisionVersion must equal the pre-change PrecisionVersion + 1;
	// acts as an idempotency key during rollout.
	NewPrecisionVersion uint64 `json:"new_precision_version"`

	// Reason is free-form text persisted to admin audit logs (ADR-0052).
	Reason string `json:"reason,omitempty"`
}

// EventType distinguishes Put from Delete.
type EventType uint8

const (
	EventPut    EventType = 1
	EventDelete EventType = 2
)

// Event is a single change seen by a Watch.
type Event struct {
	Type   EventType
	Symbol string
	Config SymbolConfig // zero for Delete
}

// Source is the minimum surface the match runtime uses to talk to etcd
// (or a test fake).
type Source interface {
	// List returns a snapshot of (symbol → config) plus the etcd revision
	// at which it was observed. Watch should be called with fromRevision =
	// revision + 1 so no change is lost.
	List(ctx context.Context) (configs map[string]SymbolConfig, revision int64, err error)

	// Watch returns a stream of events starting from fromRevision. The
	// channel closes when ctx is cancelled or the underlying watch ends.
	Watch(ctx context.Context, fromRevision int64) (<-chan Event, error)

	// Close releases resources. Must be called when done.
	Close() error
}

// Writer is the subset of Source used by admin-plane CRUD (ADR-0052). Only
// EtcdSource / MemorySource implement it today; callers that don't need
// writes can keep depending on Source.
type Writer interface {
	// Put upserts cfg under symbol. Returns the revision after the write.
	Put(ctx context.Context, symbol string, cfg SymbolConfig) (revision int64, err error)

	// Delete removes symbol. Returns (false, revAtCallTime, nil) when the
	// key did not exist.
	Delete(ctx context.Context, symbol string) (existed bool, revision int64, err error)
}

// ErrInvalidSymbol is returned when a symbol name fails basic validation
// (empty, contains '/' which would break the etcd key layout, or NUL byte).
var ErrInvalidSymbol = errors.New("etcdcfg: invalid symbol")

// ValidateSymbol applies the same check Put / Delete use so BFF admin
// handlers can surface a clean 400.
func ValidateSymbol(symbol string) error {
	if symbol == "" {
		return ErrInvalidSymbol
	}
	for i := 0; i < len(symbol); i++ {
		c := symbol[i]
		if c == '/' || c == 0 || c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			return ErrInvalidSymbol
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// JSON codec (exposed for tests / integration tooling)
// -----------------------------------------------------------------------------

// SymbolFromKey extracts the symbol from an etcd key using prefix.
func SymbolFromKey(key, prefix string) string {
	if !strings.HasPrefix(key, prefix) {
		return ""
	}
	return key[len(prefix):]
}

// DecodeValue parses a JSON byte slice into a SymbolConfig.
func DecodeValue(raw []byte) (SymbolConfig, error) {
	var out SymbolConfig
	if len(raw) == 0 {
		return out, errors.New("etcdcfg: empty value")
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("etcdcfg: decode value: %w", err)
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// EtcdSource
// -----------------------------------------------------------------------------

// EtcdConfig configures an EtcdSource.
type EtcdConfig struct {
	Endpoints   []string
	DialTimeout time.Duration // default 5s
	Prefix      string        // default DefaultPrefix
	// Client is an optional pre-built client (used by tests or when the
	// caller manages the lifecycle). When set, Endpoints/DialTimeout are
	// ignored.
	Client *clientv3.Client
}

// EtcdSource is a real etcd-backed Source.
type EtcdSource struct {
	client    *clientv3.Client
	prefix    string
	ownClient bool // true when we created the client and should Close it
}

// NewEtcdSource dials etcd (or wraps an existing client) and returns a Source.
func NewEtcdSource(cfg EtcdConfig) (*EtcdSource, error) {
	if cfg.Prefix == "" {
		cfg.Prefix = DefaultPrefix
	}
	if cfg.Client != nil {
		return &EtcdSource{client: cfg.Client, prefix: cfg.Prefix, ownClient: false}, nil
	}
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("etcdcfg: endpoints required")
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("etcdcfg: dial: %w", err)
	}
	return &EtcdSource{client: cli, prefix: cfg.Prefix, ownClient: true}, nil
}

// Close releases the underlying client if we created it.
func (s *EtcdSource) Close() error {
	if s.ownClient {
		return s.client.Close()
	}
	return nil
}

// List fetches the current snapshot under the configured prefix.
func (s *EtcdSource) List(ctx context.Context) (map[string]SymbolConfig, int64, error) {
	resp, err := s.client.Get(ctx, s.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, fmt.Errorf("etcdcfg: list: %w", err)
	}
	out := make(map[string]SymbolConfig, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		symbol := SymbolFromKey(string(kv.Key), s.prefix)
		if symbol == "" {
			continue
		}
		cfg, err := DecodeValue(kv.Value)
		if err != nil {
			return nil, 0, fmt.Errorf("etcdcfg: key %s: %w", kv.Key, err)
		}
		out[symbol] = cfg
	}
	return out, resp.Header.Revision, nil
}

// Put upserts cfg under symbol (ADR-0052).
func (s *EtcdSource) Put(ctx context.Context, symbol string, cfg SymbolConfig) (int64, error) {
	if err := ValidateSymbol(symbol); err != nil {
		return 0, err
	}
	value, err := json.Marshal(cfg)
	if err != nil {
		return 0, fmt.Errorf("etcdcfg: encode: %w", err)
	}
	resp, err := s.client.Put(ctx, s.prefix+symbol, string(value))
	if err != nil {
		return 0, fmt.Errorf("etcdcfg: put: %w", err)
	}
	return resp.Header.Revision, nil
}

// Delete removes symbol. Returns existed=false when the key was absent.
func (s *EtcdSource) Delete(ctx context.Context, symbol string) (bool, int64, error) {
	if err := ValidateSymbol(symbol); err != nil {
		return false, 0, err
	}
	resp, err := s.client.Delete(ctx, s.prefix+symbol)
	if err != nil {
		return false, 0, fmt.Errorf("etcdcfg: delete: %w", err)
	}
	return resp.Deleted > 0, resp.Header.Revision, nil
}

// Watch streams changes starting at fromRevision.
func (s *EtcdSource) Watch(ctx context.Context, fromRevision int64) (<-chan Event, error) {
	wc := s.client.Watch(ctx, s.prefix, clientv3.WithPrefix(), clientv3.WithRev(fromRevision))
	out := make(chan Event, 16)
	go func() {
		defer close(out)
		for wr := range wc {
			if err := wr.Err(); err != nil {
				// Upstream watch errors are terminal for this channel; the
				// caller is expected to notice a closed channel and restart.
				return
			}
			for _, ev := range wr.Events {
				symbol := SymbolFromKey(string(ev.Kv.Key), s.prefix)
				if symbol == "" {
					continue
				}
				switch ev.Type {
				case clientv3.EventTypePut:
					cfg, err := DecodeValue(ev.Kv.Value)
					if err != nil {
						continue
					}
					select {
					case out <- Event{Type: EventPut, Symbol: symbol, Config: cfg}:
					case <-ctx.Done():
						return
					}
				case clientv3.EventTypeDelete:
					select {
					case out <- Event{Type: EventDelete, Symbol: symbol}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, nil
}
