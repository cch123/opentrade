package holder

import (
	"errors"
	"sync"
)

// ErrUnknownBizLine is returned by Registry.Get when biz_line has no
// registered Client.
var ErrUnknownBizLine = errors.New("holder: unknown biz_line")

// Registry maps biz_line strings ("funding" / "spot" / ...) to their
// Client. Population happens at startup via main.go (see M3b main.go
// wiring); the saga driver looks up from both from_biz and to_biz on
// every Transfer.
type Registry struct {
	mu      sync.RWMutex
	holders map[string]Client
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{holders: make(map[string]Client)}
}

// Register binds biz_line to client. Panics if client is nil so
// configuration errors surface at startup.
func (r *Registry) Register(bizLine string, client Client) {
	if client == nil {
		panic("holder: Register(" + bizLine + ", nil)")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.holders[bizLine] = client
}

// Get returns the Client for biz_line or ErrUnknownBizLine.
func (r *Registry) Get(bizLine string) (Client, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.holders[bizLine]
	if !ok {
		return nil, ErrUnknownBizLine
	}
	return c, nil
}

// Known returns the sorted list of biz_lines configured. Intended for
// startup logging so operators can see the saga routing table at a
// glance.
func (r *Registry) Known() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.holders))
	for k := range r.holders {
		out = append(out, k)
	}
	return out
}
