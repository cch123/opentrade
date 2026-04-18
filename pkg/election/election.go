// Package election is a thin wrapper around etcd concurrency.Election that
// matches the ADR-0002 lease-based leadership model used by Counter and Match
// shards.
//
// Lifecycle:
//
//	e, _ := election.New(Config{Endpoints, Path, Value})
//	defer e.Close()
//	// In a goroutine:
//	for {
//	    if err := e.Campaign(ctx); err != nil { return err }
//	    runAsPrimary(ctx, e.LostCh())     // blocks until lease lost / ctx cancelled
//	    _ = e.Resign(ctx)                 // voluntary step-down
//	}
//
// Campaign blocks until the caller becomes leader. LostCh fires when the
// session expires (backup node becomes primary elsewhere). Observe streams
// the current leader Value to backups.
package election

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Config configures an Election.
type Config struct {
	// Client is an already-connected etcd client. When non-nil, Endpoints
	// and DialTimeout are ignored and Close will not close this client.
	Client *clientv3.Client

	// Endpoints is used when Client is nil; election dials its own client.
	Endpoints   []string
	DialTimeout time.Duration // default 5s

	// LeaseTTL is the session TTL in seconds. Default 10. Leader must
	// heartbeat within this window or another candidate takes over.
	LeaseTTL int

	// Path is the etcd key prefix used for this election — every
	// candidate with the same Path competes for the same leadership.
	// e.g. "/cex/counter/shard-0".
	Path string

	// Value is written under the leader key when this candidate wins.
	// Typically the instance id (e.g. "counter-0-a"). Must be non-empty.
	Value string
}

// Election is a single-instance leadership coordinator.
type Election struct {
	cfg       Config
	client    *clientv3.Client
	ownClient bool

	session  *concurrency.Session
	election *concurrency.Election

	// lostCh is closed when the session expires (we lost leadership). A
	// fresh Election creates a new session and lostCh on each Campaign.
	lostCh chan struct{}
}

// New validates cfg, dials etcd if needed, and creates a session. It does
// not campaign yet — call Campaign in a goroutine.
func New(cfg Config) (*Election, error) {
	if cfg.Path == "" {
		return nil, errors.New("election: Path required")
	}
	if cfg.Value == "" {
		return nil, errors.New("election: Value required")
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 10
	}

	e := &Election{cfg: cfg}

	if cfg.Client != nil {
		e.client = cfg.Client
	} else {
		if len(cfg.Endpoints) == 0 {
			return nil, errors.New("election: Endpoints or Client required")
		}
		if cfg.DialTimeout <= 0 {
			cfg.DialTimeout = 5 * time.Second
		}
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   cfg.Endpoints,
			DialTimeout: cfg.DialTimeout,
		})
		if err != nil {
			return nil, fmt.Errorf("election: dial: %w", err)
		}
		e.client = cli
		e.ownClient = true
	}

	if err := e.newSession(); err != nil {
		if e.ownClient {
			_ = e.client.Close()
		}
		return nil, err
	}
	return e, nil
}

func (e *Election) newSession() error {
	session, err := concurrency.NewSession(e.client, concurrency.WithTTL(e.cfg.LeaseTTL))
	if err != nil {
		return fmt.Errorf("election: new session: %w", err)
	}
	e.session = session
	e.election = concurrency.NewElection(session, e.cfg.Path)
	lost := make(chan struct{})
	e.lostCh = lost
	go func() {
		<-session.Done()
		close(lost)
	}()
	return nil
}

// Campaign blocks until this candidate becomes leader or ctx is cancelled.
// If the session was lost before the call, a fresh one is created. Safe to
// call multiple times across leadership cycles.
func (e *Election) Campaign(ctx context.Context) error {
	// If the current session is already gone, start a new one before
	// campaigning again.
	select {
	case <-e.session.Done():
		if err := e.newSession(); err != nil {
			return err
		}
	default:
	}
	return e.election.Campaign(ctx, e.cfg.Value)
}

// Resign relinquishes leadership. Call it during a graceful shutdown from
// the primary role so the backup takes over immediately instead of waiting
// for the lease to expire.
func (e *Election) Resign(ctx context.Context) error {
	return e.election.Resign(ctx)
}

// LostCh fires when the underlying lease has expired. Primaries should
// select on this in their main loop alongside their own shutdown signal.
func (e *Election) LostCh() <-chan struct{} { return e.lostCh }

// Observe streams the current leader Value whenever it changes. Backups
// can read from it to know who is primary. The channel closes when ctx is
// cancelled.
func (e *Election) Observe(ctx context.Context) <-chan string {
	raw := e.election.Observe(ctx)
	out := make(chan string, 1)
	go func() {
		defer close(out)
		for resp := range raw {
			for _, kv := range resp.Kvs {
				select {
				case out <- string(kv.Value):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Leader returns the current leader value, or "" when there is none.
func (e *Election) Leader(ctx context.Context) (string, error) {
	resp, err := e.election.Leader(ctx)
	if err != nil {
		if errors.Is(err, concurrency.ErrElectionNoLeader) {
			return "", nil
		}
		return "", err
	}
	for _, kv := range resp.Kvs {
		return string(kv.Value), nil
	}
	return "", nil
}

// Close terminates the session (releasing leadership if held) and, if the
// client was created by this Election, closes the client too.
func (e *Election) Close() error {
	if e.session != nil {
		_ = e.session.Close()
	}
	if e.ownClient {
		return e.client.Close()
	}
	return nil
}
