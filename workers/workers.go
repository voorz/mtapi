// Package workers manages one live MT5 session per account.
//
// A Worker IS the account's session: it dials once, keeps the
// connection alive (the underlying client re-authenticates itself
// after drops, and the worker re-dials when a session dies for
// good), and serves queries straight from that live session. No
// query ever logs in again. Workers are fully independent: one
// account's outage, bad password or restart never touches another.
//
// The package is mechanism only. When workers start or stop, how
// often they are queried and what the answers mean is the caller's
// policy.
package workers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/voorz/mtapi"
)

// Key identifies one account: its login on one server.
type Key struct {
	Login  uint64
	Server string
}

// KeyOf derives the pool key for a client configuration.
func KeyOf(cfg mtapi.Config) Key {
	return Key{Login: cfg.Login, Server: cfg.Server}
}

// State is the worker's connection state, as last observed by the
// worker itself: it changes when the worker dials, when a query
// succeeds or fails, and when the session reconnects.
type State int

const (
	// StateConnecting: the first dial (or a restart) is in flight.
	StateConnecting State = iota
	// StateConnected: the session is live.
	StateConnected
	// StateReconnecting: the session dropped or died; the worker is
	// re-establishing it. Queries block (bounded by their context),
	// mirroring the client's own behaviour during an outage.
	StateReconnecting
	// StateAuthFailed: the server rejected the credentials. Terminal
	// until Restart: retrying a bad password only risks a lockout.
	StateAuthFailed
	// StateClosed: the worker was closed and released its session.
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	case StateAuthFailed:
		return "auth_failed"
	case StateClosed:
		return "closed"
	}
	return "unknown"
}

// Status is a point-in-time snapshot of one worker, for display and
// monitoring.
type Status struct {
	Key            Key
	State          State
	Since          time.Time // when the current state was entered
	ConnectedSince time.Time // start of the current connected period, zero unless connected
	LastError      string    // most recent failure, empty while healthy
	Reconnects     int       // successful reconnects since the worker started
}

// session is the part of mtapi.Client a worker relies on. It exists
// so tests can substitute a fake; production always wraps the real
// client.
type session interface {
	Account(ctx context.Context) (mtapi.Account, error)
	Positions(ctx context.Context) ([]mtapi.Position, error)
	Orders(ctx context.Context) ([]mtapi.Order, error)
	Deals(ctx context.Context, from, to time.Time) ([]mtapi.Deal, error)
	OnReconnect(fn func())
	Close() error
}

// dialFunc creates one session. The default dials a real client.
type dialFunc func(ctx context.Context, cfg mtapi.Config) (session, *mtapi.Client, error)

func realDial(ctx context.Context, cfg mtapi.Config) (session, *mtapi.Client, error) {
	c, err := mtapi.Dial(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return c, c, nil
}

// Worker owns one account's live session.
type Worker struct {
	key Key
	cfg mtapi.Config

	mu             sync.Mutex
	changed        chan struct{} // broadcast: closed and replaced on every state change
	state          State
	since          time.Time
	connectedSince time.Time
	lastErr        string
	reconnects     int
	client         session
	realClient     *mtapi.Client
	authFailed     bool
	closed         bool

	firstDone chan struct{} // closed on the first dial outcome
	firstErr  error

	ctx    context.Context // worker lifetime; cancelled by Close
	cancel context.CancelFunc
	kick   chan struct{} // asks the maintenance loop to (re)dial now
	dial   dialFunc
}

// Status returns the worker's current status snapshot.
func (w *Worker) Status() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Status{
		Key:            w.key,
		State:          w.state,
		Since:          w.since,
		ConnectedSince: w.connectedSince,
		LastError:      w.lastErr,
		Reconnects:     w.reconnects,
	}
}

// Key returns the account identity this worker serves.
func (w *Worker) Key() Key { return w.key }

// Client returns the underlying live client, or nil while no session
// is established. Callers that only need account data should prefer
// the Worker's own query methods, which keep the status honest.
func (w *Worker) Client() *mtapi.Client {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.realClient
}

// Account returns the account state from the live session.
func (w *Worker) Account(ctx context.Context) (mtapi.Account, error) {
	var out mtapi.Account
	err := w.query(ctx, func(c session) error {
		v, err := c.Account(ctx)
		out = v
		return err
	})
	return out, err
}

// Positions returns the open positions from the live session.
func (w *Worker) Positions(ctx context.Context) ([]mtapi.Position, error) {
	var out []mtapi.Position
	err := w.query(ctx, func(c session) error {
		v, err := c.Positions(ctx)
		out = v
		return err
	})
	return out, err
}

// Orders returns the pending orders from the live session.
func (w *Worker) Orders(ctx context.Context) ([]mtapi.Order, error) {
	var out []mtapi.Order
	err := w.query(ctx, func(c session) error {
		v, err := c.Orders(ctx)
		out = v
		return err
	})
	return out, err
}

// Deals returns the deal history from the live session.
func (w *Worker) Deals(ctx context.Context, from, to time.Time) ([]mtapi.Deal, error) {
	var out []mtapi.Deal
	err := w.query(ctx, func(c session) error {
		v, err := c.Deals(ctx, from, to)
		out = v
		return err
	})
	return out, err
}

// Restart drops the current session and dials again with the stored
// configuration. It is the recovery path out of StateAuthFailed
// (after the caller has fixed the credentials out of band it still
// uses the stored ones; to change credentials, stop the worker and
// start it again with a new configuration).
func (w *Worker) Restart() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	old := w.client
	w.client = nil
	w.realClient = nil
	w.authFailed = false
	w.setStateLocked(StateConnecting, "")
	w.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	w.signalKick()
}

// Close releases the session and stops the worker. Idempotent; the
// worker serves no queries afterwards.
func (w *Worker) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	c := w.client
	w.client = nil
	w.realClient = nil
	w.setStateLocked(StateClosed, w.lastErr)
	w.mu.Unlock()
	w.cancel()
	if c != nil {
		return c.Close()
	}
	return nil
}

// --- internals ---

func (w *Worker) setStateLocked(st State, errText string) {
	w.state = st
	w.since = time.Now()
	w.lastErr = errText
	if st == StateConnected {
		w.connectedSince = w.since
	} else {
		w.connectedSince = time.Time{}
	}
	close(w.changed)
	w.changed = make(chan struct{})
}

func (w *Worker) signalKick() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// start launches the maintenance loop and waits for the first dial
// outcome (or ctx expiry). The worker keeps retrying in the
// background after a failed first dial; the returned error is the
// first failure observed.
func (w *Worker) start(ctx context.Context) error {
	go w.maintain()
	select {
	case <-w.firstDone:
		return w.firstErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// maintain keeps a live session in place: it dials whenever there is
// no client, with a bounded backoff, until the worker is closed or
// the credentials are rejected.
func (w *Worker) maintain() {
	backoff := time.Second
	for {
		w.mu.Lock()
		need := !w.closed && !w.authFailed && w.client == nil
		w.mu.Unlock()
		if !need {
			select {
			case <-w.ctx.Done():
				return
			case <-w.kick:
				continue
			}
		}

		sess, real, err := w.dial(w.ctx, w.cfg)
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			if sess != nil {
				_ = sess.Close()
			}
			return
		}
		if err == nil {
			w.client = sess
			w.realClient = real
			w.setStateLocked(StateConnected, "")
			w.mu.Unlock()
			sess.OnReconnect(w.onReconnect)
			w.finishFirst(nil)
			backoff = time.Second
			continue
		}
		// Dial failed.
		auth := errors.Is(err, mtapi.ErrAuth)
		if auth {
			w.authFailed = true
			w.setStateLocked(StateAuthFailed, err.Error())
		} else {
			w.setStateLocked(StateReconnecting, err.Error())
		}
		w.mu.Unlock()
		w.finishFirst(err)
		if auth {
			continue // wait for Restart or Close via the need-check above
		}
		select {
		case <-w.ctx.Done():
			return
		case <-w.kick: // a Restart asked for an immediate redial
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (w *Worker) finishFirst(err error) {
	select {
	case <-w.firstDone:
	default:
		w.firstErr = err
		close(w.firstDone)
	}
}

func (w *Worker) onReconnect() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.client == nil {
		return
	}
	w.reconnects++
	w.setStateLocked(StateConnected, "")
}

// awaitClient waits until a live session exists, honouring ctx, and
// fails fast in the terminal states.
func (w *Worker) awaitClient(ctx context.Context) (session, error) {
	for {
		w.mu.Lock()
		if w.client != nil {
			c := w.client
			w.mu.Unlock()
			return c, nil
		}
		if w.closed {
			w.mu.Unlock()
			return nil, mtapi.ErrClosed
		}
		if w.authFailed {
			err := w.lastErr
			w.mu.Unlock()
			return nil, fmt.Errorf("%w: %s", mtapi.ErrAuth, err)
		}
		ch := w.changed
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}

// query runs fn against the live session and folds the outcome into
// the worker's status.
func (w *Worker) query(ctx context.Context, fn func(session) error) error {
	c, err := w.awaitClient(ctx)
	if err != nil {
		return err
	}
	err = fn(c)
	w.observe(c, err)
	return err
}

// observe records a query outcome. An auth failure is terminal. A
// closed client means the session died for good: drop it and let
// the maintenance loop re-dial. Anything else is a transient outage
// the client itself is already recovering from.
func (w *Worker) observe(c session, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.client != c {
		return // stale session or already closed; nothing to learn
	}
	switch {
	case err == nil:
		if w.state != StateConnected {
			w.setStateLocked(StateConnected, "")
		} else {
			w.lastErr = ""
		}
	case errors.Is(err, mtapi.ErrAuth):
		w.authFailed = true
		w.client = nil
		w.realClient = nil
		w.setStateLocked(StateAuthFailed, err.Error())
		go func() { _ = c.Close() }()
	case errors.Is(err, mtapi.ErrClosed):
		w.client = nil
		w.realClient = nil
		w.setStateLocked(StateReconnecting, err.Error())
		w.signalKick()
	default:
		w.setStateLocked(StateReconnecting, err.Error())
	}
}

// Pool is a registry of workers, one per account.
type Pool struct {
	mu      sync.Mutex
	workers map[Key]*Worker
	dial    dialFunc
}

// NewPool returns an empty pool.
func NewPool() *Pool {
	return &Pool{workers: make(map[Key]*Worker), dial: realDial}
}

// GetOrStart returns the worker for the configuration's account,
// starting it if necessary. Starting is idempotent: concurrent or
// repeated calls for the same login and server return the same
// worker and dial at most once; a call that finds the worker
// already present returns it immediately, without waiting. The configuration's AutoReconnect
// is always enabled — a worker is a live session by definition.
//
// The call waits for the first dial outcome and reports its error,
// if any; the worker itself keeps retrying transient failures in
// the background, and an auth failure parks it in StateAuthFailed
// for the caller to see (fix the credentials, then Restart it or
// stop and start it again).
func (p *Pool) GetOrStart(ctx context.Context, cfg mtapi.Config) (*Worker, error) {
	cfg.AutoReconnect = true
	key := KeyOf(cfg)

	p.mu.Lock()
	if w, ok := p.workers[key]; ok {
		p.mu.Unlock()
		return w, nil
	}
	wctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		key:       key,
		cfg:       cfg,
		changed:   make(chan struct{}),
		state:     StateConnecting,
		since:     time.Now(),
		firstDone: make(chan struct{}),
		ctx:       wctx,
		cancel:    cancel,
		kick:      make(chan struct{}, 1),
		dial:      p.dial,
	}
	p.workers[key] = w
	p.mu.Unlock()

	if err := w.start(ctx); err != nil {
		return w, err
	}
	return w, nil
}

// Get returns the worker for a key, if one exists.
func (p *Pool) Get(key Key) (*Worker, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.workers[key]
	return w, ok
}

// Stop closes and removes the worker for a key. It reports whether
// one existed.
func (p *Pool) Stop(key Key) bool {
	p.mu.Lock()
	w, ok := p.workers[key]
	if ok {
		delete(p.workers, key)
	}
	p.mu.Unlock()
	if !ok {
		return false
	}
	_ = w.Close()
	return true
}

// StopAll closes and removes every worker.
func (p *Pool) StopAll() {
	p.mu.Lock()
	all := p.workers
	p.workers = make(map[Key]*Worker)
	p.mu.Unlock()
	for _, w := range all {
		_ = w.Close()
	}
}

// Len reports how many workers the pool holds.
func (p *Pool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

// Statuses snapshots every worker, ordered by server then login so
// listings are stable.
func (p *Pool) Statuses() []Status {
	p.mu.Lock()
	ws := make([]*Worker, 0, len(p.workers))
	for _, w := range p.workers {
		ws = append(ws, w)
	}
	p.mu.Unlock()
	out := make([]Status, 0, len(ws))
	for _, w := range ws {
		out = append(out, w.Status())
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key.Server != out[j].Key.Server {
			return out[i].Key.Server < out[j].Key.Server
		}
		return out[i].Key.Login < out[j].Key.Login
	})
	return out
}
