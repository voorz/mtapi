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
// A Client is single-use: after Close, create a new one with Dial.
type Client struct {
	cfg  Config
	conn *ws.Conn

	sessionKey []byte

	sendMu sync.Mutex // serializes frame writes, as the reference does

	slotsMu sync.Mutex
	slots   map[uint16]chan struct{} // per-command in-flight semaphore

	pendingMu sync.Mutex
	pending   map[uint16]chan *wire.Response

	tradeMu     sync.Mutex // serializes trade operations
	tradeEvents chan TradeEvent

	stateMu    sync.RWMutex
	account    Account
	hasAccount bool
	symbols    map[string]*Symbol
	symbolByID map[int64]string
	quotes     map[string]Quote
	posCache   map[int64]Position
	orderCache map[int64]Order
	specData   map[int64][]byte
	onQuote    func(Quote)
	onTrade    func(TradeEvent)
	onSpec     func(SymbolSpec)

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
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

	addr, err := cfg.resolveAddr(ctx)
	if err != nil {
		return nil, err
	}

	c := &Client{
		cfg:         cfg,
		slots:       make(map[uint16]chan struct{}),
		pending:     make(map[uint16]chan *wire.Response),
		tradeEvents: make(chan TradeEvent, 64),
		symbols:     make(map[string]*Symbol),
		symbolByID:  make(map[int64]string),
		quotes:      make(map[string]Quote),
		posCache:    make(map[int64]Position),
		orderCache:  make(map[int64]Order),
		specData:    make(map[int64][]byte),
		done:        make(chan struct{}),
	}

	conn, err := ws.Dial(ctx, addr, "/terminal", ws.Options{
		TLSConfig: cfg.TLSConfig,
		Origin:    "https://" + addr,
	})
	if err != nil {
		return nil, connError(err)
	}
	c.conn = conn

	if err := c.auth(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	if err := c.login(ctx); err != nil {
		conn.Close()
		return nil, err
	}

	c.wg.Add(2)
	go c.recvLoop()
	go c.heartbeatLoop()

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

// Close tears down the session. It is safe to call more than once.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.conn != nil {
			c.conn.Close()
		}
	})
	c.wg.Wait()
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

// auth performs the command-0 handshake: a 64-byte zero payload
// encrypted with the static key; the response body carries the
// session key at offset 66 (after a 2-byte sub-tag and a 64-byte
// session token).
func (c *Client) auth(ctx context.Context) error {
	var rb [2]byte
	if _, err := rand.Read(rb[:]); err != nil {
		return err
	}
	plain := wire.BuildCommand(rb[0], rb[1], wire.CmdAuth, make([]byte, 64))
	enc, err := cryptox.AESEncrypt(cryptox.StaticKey, plain)
	if err != nil {
		return err
	}
	if err := c.conn.Send(wire.PackWire(enc)); err != nil {
		return connError(err)
	}
	resp, err := c.recvOne(ctx, cryptox.StaticKey)
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
	c.sessionKey = append([]byte(nil), resp.Body[66:]...)
	return nil
}

// login performs the command-28 login with the 912-byte payload:
// password (UTF-16LE) at offset 4, server name at 480 with its
// character count at 476, login (uint64) at 736.
func (c *Client) login(ctx context.Context) error {
	payload := make([]byte, 912)
	pw := wire.EncodeUTF16LE(c.cfg.Password)
	copy(payload[4:476], pw)
	name := wire.EncodeUTF16LE(c.cfg.Server)
	binary.LittleEndian.PutUint32(payload[476:480], uint32(len(name)/2))
	copy(payload[480:736], name)
	binary.LittleEndian.PutUint64(payload[736:744], c.cfg.Login)

	if err := c.sendEncrypted(wire.CmdLogin, payload); err != nil {
		return err
	}
	resp, err := c.recvOne(ctx, c.sessionKey)
	if err != nil {
		return err
	}
	if resp == nil || resp.CmdID != wire.CmdLogin {
		return authError("login failed")
	}
	return nil
}

// recvOne reads and decodes a single message outside the receive loop
// (used by the handshake phases). The context is bounded by the
// configured request timeout.
func (c *Client) recvOne(ctx context.Context, key []byte) (*wire.Response, error) {
	rctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	msg, err := c.conn.Recv(rctx)
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

// sendEncrypted builds, encrypts (session key) and sends one command.
func (c *Client) sendEncrypted(cmdID uint16, payload []byte) error {
	var rb [2]byte
	if _, err := rand.Read(rb[:]); err != nil {
		return err
	}
	plain := wire.BuildCommand(rb[0], rb[1], cmdID, payload)
	enc, err := cryptox.AESEncrypt(c.sessionKey, plain)
	if err != nil {
		return err
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.isDone() {
		return ErrClosed
	}
	if err := c.conn.Send(wire.PackWire(enc)); err != nil {
		return connError(err)
	}
	return nil
}

// sendAndWait sends a command and waits for the response with the
// expected command id. Requests are serialized per command id.
func (c *Client) sendAndWait(ctx context.Context, cmdID uint16, payload []byte, expected uint16) (*wire.Response, error) {
	release, err := c.acquireSlot(ctx, expected)
	if err != nil {
		return nil, err
	}
	defer release()

	ch := make(chan *wire.Response, 1)
	c.pendingMu.Lock()
	c.pending[expected] = ch
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, expected)
		c.pendingMu.Unlock()
	}()

	if err := c.sendEncrypted(cmdID, payload); err != nil {
		return nil, err
	}

	rctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	select {
	case resp := <-ch:
		return resp, nil
	case <-rctx.Done():
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: command %d", ErrTimeout, cmdID)
	case <-c.done:
		return nil, ErrClosed
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

// recvLoop dispatches incoming messages: pending requests first, then
// the push handlers (quotes, trade events, symbol specs). All other
// commands are notifications the reference also ignores.
func (c *Client) recvLoop() {
	defer c.wg.Done()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		msg, err := c.conn.Recv(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && !c.isDone() {
				continue
			}
			c.shutdown()
			return
		}
		resp := decodeMessage(msg, c.sessionKey)
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

// heartbeatLoop sends the command-51 heartbeat every 3 seconds, the
// interval the reference uses.
func (c *Client) heartbeatLoop() {
	defer c.wg.Done()
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			if err := c.sendEncrypted(wire.CmdHeartbeat, nil); err != nil {
				return
			}
		}
	}
}

// shutdown closes the client after a fatal receive error.
func (c *Client) shutdown() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.conn != nil {
			c.conn.Close()
		}
	})
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
