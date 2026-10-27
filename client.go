package mtapi

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/voorz/mtapi/internal/cryptox"
	"github.com/voorz/mtapi/internal/wire"
	"github.com/voorz/mtapi/internal/ws"
)

// Config configures a Client connection.
type Config struct {
	// Login is the MT5 account number.
	Login uint64
	// Password is the account password. An investor (read-only)
	// password works for everything except trading.
	Password string
	// Server is the server name, e.g. "Exness-MT5Trial17". It is
	// resolved to an access point through broker search unless Host
	// is set.
	Server string
	// Host optionally pins the access point ("ip:port", or a bare IP
	// for port 443), skipping server resolution entirely.
	Host string
	// Build is the terminal protocol build. Default 5830.
	Build int
	// Timeout bounds each request/response exchange. Default 10s.
	Timeout time.Duration
	// TLSConfig overrides the TLS configuration used for the access
	// server connection. See ws.Options for why the default does not
	// verify the access server's certificate.
	TLSConfig *tls.Config
	// HTTPClient is used for server resolution. Default
	// http.DefaultClient.
	HTTPClient *http.Client
	// Execution selects the protocol trade action used for market
	// orders (open and close). Default ExecutionAuto: the client
	// learns per symbol from the server's responses. See
	// ExecutionMode.
	Execution ExecutionMode
	// AutoReconnect keeps the session alive across connection
	// drops: when the transport fails, the client re-dials,
	// re-authenticates, reloads the account and symbol list, and
	// restores subscriptions, with backoff. Read operations block
	// during the outage and resume on the new session; a trade
	// interrupted mid-flight fails with ErrConnection and is never
	// silently resubmitted (reconcile via Positions/Deals).
	// Default false: a drop closes the client, as in the
	// reference.
	AutoReconnect bool
}

func (cfg Config) withDefaults() Config {
	if cfg.Build == 0 {
		cfg.Build = 5830
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	return cfg
}

// Client is one authenticated MT5 account session. All methods are
// safe for concurrent use; requests that share a protocol command are
// serialized internally because protocol responses carry no request
// identifier (the reference implementation keys its pending table by
// command id the same way).
//
// After Close the client is done: create a new one with Dial. With
// Config.AutoReconnect a dropped transport is recovered in place
// instead (see the field's documentation).
type Client struct {
	cfg Config

	// Session state. A session is one transport connection plus
	// its key; it is replaced wholesale on reconnect. sessChanged
	// is closed and recreated on every session transition to wake
	// waiters.
	sessMu      sync.RWMutex
	sess        *session
	sessChanged chan struct{}
	fatalErr    error // permanent reconnect failure (e.g. auth)
	closedFlag  bool  // set by Close under sessMu; blocks new sessions
	nextSessID  uint64
	kick        chan struct{} // reconnect requests, buffered 1
	supWg       sync.WaitGroup
	loopsWg     sync.WaitGroup // recv+heartbeat loops of all sessions

	baseCtx    context.Context
	baseCancel context.CancelFunc

	sendMu sync.Mutex // serializes frame writes, as the reference does

	slotsMu sync.Mutex
	slots   map[uint16]chan struct{} // per-command in-flight semaphore

	pendingMu sync.Mutex
	pending   map[uint16]chan *wire.Response

	tradeMu     sync.Mutex // serializes trade operations
	tradeEvents chan TradeEvent

	stateMu     sync.RWMutex
	account     Account
	hasAccount  bool
	symbols     map[string]*Symbol
	symbolByID  map[int64]string
	quotes      map[string]Quote
	posCache    map[int64]Position
	orderCache  map[int64]Order
	execLearned map[string]uint32 // symbol -> learned market trade action
	subscribed  map[string]bool   // symbol names with a live subscription
	specData    map[int64][]byte
	onQuote     func(Quote)
	onTrade     func(TradeEvent)
	onSpec      func(SymbolSpec)
	onReconnect func()

	done      chan struct{}
	closeOnce sync.Once
}

// session is one established transport: a connection, its session
// key, and the loops serving it. dead is closed when the receive
// loop exits.
type session struct {
	id   uint64
	conn *ws.Conn
	key  []byte
	dead chan struct{}
}

// Dial resolves the server, connects, performs the auth handshake and
// login, starts the receive and heartbeat loops, and loads the account
// and symbol list. If any step fails the connection is torn down and
// an error is returned.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if cfg.Login == 0 {
		return nil, errors.New("mtapi: Config.Login is required")
	}
	if cfg.Password == "" {
		return nil, errors.New("mtapi: Config.Password is required")
	}
	if cfg.Server == "" && cfg.Host == "" {
		return nil, errors.New("mtapi: Config.Server or Config.Host is required")
	}

	c := &Client{
		cfg:         cfg,
		sessChanged: make(chan struct{}),
		kick:        make(chan struct{}, 1),
		slots:       make(map[uint16]chan struct{}),
		pending:     make(map[uint16]chan *wire.Response),
		tradeEvents: make(chan TradeEvent, 64),
		symbols:     make(map[string]*Symbol),
		symbolByID:  make(map[int64]string),
		quotes:      make(map[string]Quote),
		posCache:    make(map[int64]Position),
		orderCache:  make(map[int64]Order),
		execLearned: make(map[string]uint32),
		subscribed:  make(map[string]bool),
		specData:    make(map[int64][]byte),
		done:        make(chan struct{}),
	}
	c.baseCtx, c.baseCancel = context.WithCancel(context.Background())

	sess, err := c.establish(ctx)
	if err != nil {
		c.baseCancel()
		return nil, err
	}
	c.installSession(sess)

	if cfg.AutoReconnect {
		c.supWg.Add(1)
		go c.reconnectSupervisor()
	}

	if _, err := c.Account(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("mtapi: load account: %w", err)
	}
	if _, err := c.Symbols(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("mtapi: load symbols: %w", err)
	}
	return c, nil
}

// establish dials a fresh transport and performs the auth handshake
// and login on it. The returned session is not installed yet.
func (c *Client) establish(ctx context.Context) (*session, error) {
	addr, err := c.cfg.resolveAddr(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := ws.Dial(ctx, addr, "/terminal", ws.Options{
		TLSConfig: c.cfg.TLSConfig,
		Origin:    "https://" + addr,
	})
	if err != nil {
		return nil, connError(err)
	}
	c.sessMu.Lock()
	c.nextSessID++
	sess := &session{id: c.nextSessID, conn: conn, dead: make(chan struct{})}
	c.sessMu.Unlock()

	if err := c.authOn(ctx, sess); err != nil {
		conn.Close()
		return nil, err
	}
	if err := c.loginOn(ctx, sess); err != nil {
		conn.Close()
		return nil, err
	}
	return sess, nil
}

// installSession makes sess the live session and starts its
// receive and heartbeat loops. It refuses when the client is
// already closed (the caller then owns and closes the session).
func (c *Client) installSession(sess *session) bool {
	c.sessMu.Lock()
	if c.closedFlag {
		c.sessMu.Unlock()
		return false
	}
	c.sess = sess
	close(c.sessChanged)
	c.sessChanged = make(chan struct{})
	c.sessMu.Unlock()

	c.loopsWg.Add(2)
	go c.recvLoop(sess)
	go c.heartbeatLoop(sess)
	return true
}

// currentSession returns the live session, or nil while down.
func (c *Client) currentSession() *session {
	c.sessMu.RLock()
	defer c.sessMu.RUnlock()
	return c.sess
}

// waitSession returns the live session, waiting for a reconnect
// when none is up. Without AutoReconnect a dead session is final
// (the client is closing or closed) and ErrClosed is returned.
func (c *Client) waitSession(ctx context.Context) (*session, error) {
	for {
		c.sessMu.RLock()
		sess, changed, fatal := c.sess, c.sessChanged, c.fatalErr
		c.sessMu.RUnlock()
		if sess != nil {
			return sess, nil
		}
		if fatal != nil {
			return nil, fatal
		}
		if !c.cfg.AutoReconnect {
			return nil, ErrClosed
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return nil, ErrClosed
		}
	}
}

// resolveAddr determines the "host:port" of the access point.
func (cfg Config) resolveAddr(ctx context.Context) (string, error) {
	if cfg.Host != "" {
		if _, _, err := net.SplitHostPort(cfg.Host); err != nil {
			return net.JoinHostPort(cfg.Host, "443"), nil
		}
		return cfg.Host, nil
	}
	ips, err := FindServerIPs(ctx, cfg.Server, cfg.HTTPClient)
	if err != nil {
		return "", err
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("%w: %q has no access points", ErrServerNotFound, cfg.Server)
	}
	addr := ips[0]
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "443")
	}
	return addr, nil
}

// Login returns the account number this client is logged in as.
func (c *Client) Login() uint64 { return c.cfg.Login }

// Server returns the configured server name.
func (c *Client) Server() string { return c.cfg.Server }

// Close tears down the client, stopping any reconnect activity.
// It is safe to call more than once.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.sessMu.Lock()
		c.closedFlag = true
		sess := c.sess
		c.sessMu.Unlock()
		close(c.done)
		c.baseCancel()
		if sess != nil {
			sess.conn.Close()
		}
	})
	c.supWg.Wait()
	c.loopsWg.Wait()
	return nil
}

func (c *Client) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------

// authOn performs the command-0 handshake on sess: a 64-byte zero
// payload encrypted with the static key; the response body carries
// the session key at offset 66 (after a 2-byte sub-tag and a
// 64-byte session token).
func (c *Client) authOn(ctx context.Context, sess *session) error {
	var rb [2]byte
	if _, err := rand.Read(rb[:]); err != nil {
		return err
	}
	plain := wire.BuildCommand(rb[0], rb[1], wire.CmdAuth, make([]byte, 64))
	enc, err := cryptox.AESEncrypt(cryptox.StaticKey, plain)
	if err != nil {
		return err
	}
	if err := sess.conn.Send(wire.PackWire(enc)); err != nil {
		return connError(err)
	}
	resp, err := c.recvOneOn(ctx, sess, cryptox.StaticKey)
	if err != nil {
		return err
	}
	if resp == nil || resp.ResCode != 0 {
		code := -1
		if resp != nil {
			code = int(resp.ResCode)
		}
		return authError("auth handshake failed: response code %d", code)
	}
	if len(resp.Body) < 66+32 {
		return authError("auth response too short: %d bytes", len(resp.Body))
	}
	sess.key = append([]byte(nil), resp.Body[66:]...)
	return nil
}

// loginOn performs the command-28 login on sess with the 912-byte
// payload: password (UTF-16LE) at offset 4, server name at 480 with
// its character count at 476, login (uint64) at 736.
func (c *Client) loginOn(ctx context.Context, sess *session) error {
	payload := make([]byte, 912)
	pw := wire.EncodeUTF16LE(c.cfg.Password)
	copy(payload[4:476], pw)
	name := wire.EncodeUTF16LE(c.cfg.Server)
	binary.LittleEndian.PutUint32(payload[476:480], uint32(len(name)/2))
	copy(payload[480:736], name)
	binary.LittleEndian.PutUint64(payload[736:744], c.cfg.Login)

	if err := c.sendEncryptedOn(sess, wire.CmdLogin, payload); err != nil {
		return err
	}
	resp, err := c.recvOneOn(ctx, sess, sess.key)
	if err != nil {
		return err
	}
	if resp == nil || resp.CmdID != wire.CmdLogin {
		return authError("login failed")
	}
	return nil
}

// recvOneOn reads and decodes a single message outside the receive
// loop (used by the handshake phases). The context is bounded by
// the configured request timeout.
func (c *Client) recvOneOn(ctx context.Context, sess *session, key []byte) (*wire.Response, error) {
	rctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	msg, err := sess.conn.Recv(rctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: waiting for handshake response", ErrTimeout)
		}
		return nil, connError(err)
	}
	return decodeMessage(msg, key), nil
}

// decodeMessage strips the 8-byte wire header, decrypts the payload
// and parses the response envelope. It returns nil for undecodable
// messages. Padding errors are tolerated exactly as the reference
// tolerates them (the raw plaintext is used); other decryption
// failures drop the message.
func decodeMessage(msg, key []byte) *wire.Response {
	if len(msg) <= 8 {
		return nil
	}
	pt, err := cryptox.AESDecrypt(key, msg[8:])
	if err != nil && !errors.Is(err, cryptox.ErrInvalidPadding) {
		return nil
	}
	return wire.ParseResponse(pt)
}

// ---------------------------------------------------------------------------
// Session plumbing
// ---------------------------------------------------------------------------

// sendEncryptedOn builds, encrypts (session key) and sends one
// command on a specific session.
func (c *Client) sendEncryptedOn(sess *session, cmdID uint16, payload []byte) error {
	var rb [2]byte
	if _, err := rand.Read(rb[:]); err != nil {
		return err
	}
	plain := wire.BuildCommand(rb[0], rb[1], cmdID, payload)
	enc, err := cryptox.AESEncrypt(sess.key, plain)
	if err != nil {
		return err
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	select {
	case <-sess.dead:
		return connError(errors.New("session is down"))
	default:
	}
	if err := sess.conn.Send(wire.PackWire(enc)); err != nil {
		return connError(err)
	}
	return nil
}

// sendEncrypted sends one command on the live session, waiting for
// a reconnect when the client is configured for it.
func (c *Client) sendEncrypted(ctx context.Context, cmdID uint16, payload []byte) error {
	sess, err := c.waitSession(ctx)
	if err != nil {
		return err
	}
	return c.sendEncryptedOn(sess, cmdID, payload)
}

// sendAndWait sends a command and waits for the response with the
// expected command id. Requests are serialized per command id.
// Every command routed here is an idempotent read or subscription,
// so with AutoReconnect an attempt interrupted by a drop is
// retried on the new session until the context ends.
func (c *Client) sendAndWait(ctx context.Context, cmdID uint16, payload []byte, expected uint16) (*wire.Response, error) {
	release, err := c.acquireSlot(ctx, expected)
	if err != nil {
		return nil, err
	}
	defer release()

	for {
		resp, retry, err := c.sendAndWaitOnce(ctx, cmdID, payload, expected)
		if err != nil {
			return nil, err
		}
		if !retry {
			return resp, nil
		}
	}
}

// sendAndWaitOnce is one attempt of sendAndWait. retry is true when
// the attempt was interrupted by a session drop and the caller may
// resend on a new session.
func (c *Client) sendAndWaitOnce(ctx context.Context, cmdID uint16, payload []byte, expected uint16) (resp *wire.Response, retry bool, err error) {
	sess, err := c.waitSession(ctx)
	if err != nil {
		return nil, false, err
	}

	ch := make(chan *wire.Response, 1)
	c.pendingMu.Lock()
	c.pending[expected] = ch
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, expected)
		c.pendingMu.Unlock()
	}()

	if err := c.sendEncryptedOn(sess, cmdID, payload); err != nil {
		if c.cfg.AutoReconnect && !c.isDone() {
			return nil, true, nil
		}
		return nil, false, err
	}

	rctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	select {
	case resp := <-ch:
		return resp, false, nil
	case <-sess.dead:
		if c.cfg.AutoReconnect && !c.isDone() {
			return nil, true, nil
		}
		return nil, false, ErrClosed
	case <-rctx.Done():
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		return nil, false, fmt.Errorf("%w: command %d", ErrTimeout, cmdID)
	case <-c.done:
		return nil, false, ErrClosed
	}
}

// acquireSlot takes the per-command in-flight semaphore.
func (c *Client) acquireSlot(ctx context.Context, cmd uint16) (func(), error) {
	c.slotsMu.Lock()
	sem, ok := c.slots[cmd]
	if !ok {
		sem = make(chan struct{}, 1)
		c.slots[cmd] = sem
	}
	c.slotsMu.Unlock()

	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrClosed
	}
}

// recvLoop dispatches incoming messages for one session: pending
// requests first, then the push handlers (quotes, trade events,
// symbol specs). All other commands are notifications the
// reference also ignores. A fatal receive error ends the session.
func (c *Client) recvLoop(sess *session) {
	defer c.loopsWg.Done()
	defer c.onSessionDeath(sess)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		msg, err := sess.conn.Recv(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && !c.isDone() {
				continue
			}
			return
		}
		resp := decodeMessage(msg, sess.key)
		if resp == nil {
			continue
		}
		if c.dispatchPending(resp) {
			continue
		}
		switch resp.CmdID {
		case wire.CmdQuotes:
			c.handleQuote(resp.Body)
		case wire.CmdTradeEvent:
			c.handleTradeEvent(resp.Body)
		case wire.CmdSymbolSpec:
			c.handleSpec(resp.Body)
		default:
			// Account updates, system notices, position updates and
			// heartbeat echoes carry no state this client tracks.
		}
	}
}

// onSessionDeath retires a session whose receive loop has exited:
// it wakes the session's waiters and either asks the supervisor for
// a reconnect (AutoReconnect) or shuts the client down.
func (c *Client) onSessionDeath(sess *session) {
	close(sess.dead)
	sess.conn.Close()

	c.sessMu.Lock()
	wasLive := c.sess == sess
	if wasLive {
		c.sess = nil
		close(c.sessChanged)
		c.sessChanged = make(chan struct{})
	}
	c.sessMu.Unlock()

	if c.isDone() || !wasLive {
		return
	}
	if c.cfg.AutoReconnect {
		select {
		case c.kick <- struct{}{}:
		default:
		}
	} else {
		c.shutdown()
	}
}

// dispatchPending delivers resp to a waiting request, if any.
func (c *Client) dispatchPending(resp *wire.Response) bool {
	c.pendingMu.Lock()
	ch, ok := c.pending[resp.CmdID]
	if ok {
		delete(c.pending, resp.CmdID)
	}
	c.pendingMu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- resp:
	default:
	}
	return true
}

// heartbeatLoop sends the command-51 heartbeat every 3 seconds
// for one session, the interval the reference uses.
func (c *Client) heartbeatLoop(sess *session) {
	defer c.loopsWg.Done()
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-sess.dead:
			return
		case <-t.C:
			if err := c.sendEncryptedOn(sess, wire.CmdHeartbeat, nil); err != nil {
				return
			}
		}
	}
}

// shutdown closes the client after a fatal receive error when
// AutoReconnect is off.
func (c *Client) shutdown() {
	c.closeOnce.Do(func() {
		c.sessMu.Lock()
		c.closedFlag = true
		sess := c.sess
		c.sessMu.Unlock()
		close(c.done)
		c.baseCancel()
		if sess != nil {
			sess.conn.Close()
		}
	})
}

// reconnectSupervisor re-establishes the session after a drop,
// with backoff (1s doubling to a 30s cap), until the client is
// closed. A handshake or login failure is permanent: retrying
// cannot fix credentials, so it is recorded as the client's fatal
// error and surfaced to waiting callers.
func (c *Client) reconnectSupervisor() {
	defer c.supWg.Done()
	backoff := time.Second
	for {
		select {
		case <-c.done:
			return
		case <-c.kick:
		}

		for {
			if c.isDone() {
				return
			}
			attemptCtx, cancel := context.WithCancel(c.baseCtx)
			sess, err := c.establish(attemptCtx)
			if err == nil {
				if !c.installSession(sess) {
					sess.conn.Close()
					cancel()
					return
				}
				err = c.restoreSession(attemptCtx)
				if err == nil {
					cancel()
					backoff = time.Second
					c.fireOnReconnect()
					break
				}
				// The fresh session failed to restore: retire it
				// and treat the attempt as failed.
				c.retireSession(sess)
				sess.conn.Close()
			}
			cancel()
			if err != nil && errors.Is(err, ErrAuth) {
				c.sessMu.Lock()
				c.fatalErr = err
				close(c.sessChanged)
				c.sessChanged = make(chan struct{})
				c.sessMu.Unlock()
				return
			}
			select {
			case <-time.After(backoff):
			case <-c.done:
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}
}

// retireSession removes sess as the live session (used when a
// freshly established session fails its restore step).
func (c *Client) retireSession(sess *session) {
	c.sessMu.Lock()
	if c.sess == sess {
		c.sess = nil
		close(c.sessChanged)
		c.sessChanged = make(chan struct{})
	}
	c.sessMu.Unlock()
}

// restoreSession rebuilds the volatile state on a fresh session:
// stale quotes and trade-fed caches are dropped, the account and
// symbol list are reloaded, and subscriptions are re-sent (symbol
// ids may differ on the new session, so names are re-resolved).
func (c *Client) restoreSession(ctx context.Context) error {
	c.stateMu.Lock()
	c.quotes = make(map[string]Quote)
	c.posCache = make(map[int64]Position)
	c.orderCache = make(map[int64]Order)
	names := make([]string, 0, len(c.subscribed))
	for name := range c.subscribed {
		names = append(names, name)
	}
	c.stateMu.Unlock()

	if _, err := c.Account(ctx); err != nil {
		return err
	}
	if _, err := c.Symbols(ctx); err != nil {
		return err
	}
	if len(names) > 0 {
		if err := c.Subscribe(ctx, names...); err != nil {
			return err
		}
	}
	return nil
}

// fireOnReconnect invokes the registered reconnect callback.
func (c *Client) fireOnReconnect() {
	c.stateMu.RLock()
	cb := c.onReconnect
	c.stateMu.RUnlock()
	if cb != nil {
		callSafely(cb)
	}
}

// OnReconnect registers a callback invoked after a session has
// been re-established and subscriptions restored (AutoReconnect
// only). Callbacks must not block. Pass nil to clear.
func (c *Client) OnReconnect(fn func()) {
	c.stateMu.Lock()
	c.onReconnect = fn
	c.stateMu.Unlock()
}

// OnQuote registers the callback for quote pushes. Callbacks run on
// the receive goroutine and must not block. Pass nil to clear.
func (c *Client) OnQuote(fn func(Quote)) {
	c.stateMu.Lock()
	c.onQuote = fn
	c.stateMu.Unlock()
}

// OnTrade registers the callback for trade event pushes. Callbacks
// run on the receive goroutine and must not block. Pass nil to clear.
func (c *Client) OnTrade(fn func(TradeEvent)) {
	c.stateMu.Lock()
	c.onTrade = fn
	c.stateMu.Unlock()
}

// OnSpec registers the callback for symbol specification pushes.
// Callbacks run on the receive goroutine and must not block. Pass nil
// to clear.
func (c *Client) OnSpec(fn func(SymbolSpec)) {
	c.stateMu.Lock()
	c.onSpec = fn
	c.stateMu.Unlock()
}

// callSafely invokes a user callback, swallowing panics as the
// reference swallows callback exceptions.
func callSafely(fn func()) {
	defer func() { _ = recover() }()
	fn()
}
