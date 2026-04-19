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

	clientv3 "go.etcd.io/etcd/client/v3"
)

// DefaultPrefix is the etcd key prefix under which match symbol configs live.
const DefaultPrefix = "/cex/match/symbols/"

// SymbolConfig is the JSON value stored at each symbol key.
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
}

// Owned reports whether this symbol should run on shardID right now.
func (c SymbolConfig) Owned(shardID string) bool {
	return c.Shard == shardID && c.Trading
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
