// Package ws is a minimal WebSocket client (RFC 6455, client side)
// implementing exactly what the MT5 WebSocket protocol needs: a TLS
// connection to wss://host:port/terminal, binary messages, and
// ping/pong keepalive frames. It exists so the library stays free of
// external dependencies.
//
// Behaviour mirrors the reference implementation's use of the Python
// websockets library: no automatic pings (the protocol has its own
// heartbeat command), automatic pongs in reply to server pings, and
// one complete message delivered per Recv call.
package ws

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// maxMessageSize caps one assembled message. The largest legitimate
// protocol message (the compressed symbol list) is far below this.
const maxMessageSize = 1 << 28

// ErrClosed is returned by Send and Recv after the connection breaks
// or Close is called.
var ErrClosed = errors.New("ws: connection closed")

// Options controls Dial.
type Options struct {
	// TLSConfig overrides the TLS configuration. When nil, a config
	// with InsecureSkipVerify is used: MT5 access servers are reached
	// by IP address and present certificates that do not verify
	// against the IP, exactly as the reference client handles them
	// (ssl.CERT_NONE with hostname checking disabled).
	TLSConfig *tls.Config
	// Origin is sent as the Origin header; the protocol expects
	// "https://host:port".
	Origin string
	// Timeout bounds the TCP+TLS+handshake phase. Default 15s.
	Timeout time.Duration
}

// Conn is one WebSocket connection. Send is safe for concurrent use;
// Recv must be called from a single goroutine.
type Conn struct {
	conn net.Conn
	br   *bufio.Reader

	wmu    sync.Mutex
	closed bool
	cmu    sync.Mutex
}

// Dial connects to wss://addr/path and performs the WebSocket
// handshake.
func Dial(ctx context.Context, addr, path string, opts Options) (*Conn, error) {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	tlsConf := opts.TLSConfig
	if tlsConf == nil {
		// The protocol's reference client does not verify access
		// server certificates; see Options.TLSConfig.
		tlsConf = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // protocol requirement, see doc
	} else {
		tlsConf = tlsConf.Clone()
	}
	if tlsConf.ServerName == "" {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			tlsConf.ServerName = host
		}
	}

	d := &net.Dialer{Timeout: timeout}
	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ws: dial %s: %w", addr, err)
	}
	tlsConn := tls.Client(raw, tlsConf)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, fmt.Errorf("ws: TLS handshake with %s: %w", addr, err)
	}

	c := &Conn{conn: tlsConn, br: bufio.NewReader(tlsConn)}
	if err := c.handshake(ctx, addr, path, opts.Origin, timeout); err != nil {
		tlsConn.Close()
		return nil, err
	}
	return c, nil
}

func (c *Conn) handshake(ctx context.Context, addr, path, origin string, timeout time.Duration) error {
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	var sb strings.Builder
	fmt.Fprintf(&sb, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(&sb, "Host: %s\r\n", addr)
	sb.WriteString("Upgrade: websocket\r\n")
	sb.WriteString("Connection: Upgrade\r\n")
	fmt.Fprintf(&sb, "Sec-WebSocket-Key: %s\r\n", key)
	sb.WriteString("Sec-WebSocket-Version: 13\r\n")
	if origin != "" {
		fmt.Fprintf(&sb, "Origin: %s\r\n", origin)
	}
	sb.WriteString("\r\n")

	deadline := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	c.conn.SetDeadline(deadline)
	defer c.conn.SetDeadline(time.Time{})
	if _, err := io.WriteString(c.conn, sb.String()); err != nil {
		return fmt.Errorf("ws: send handshake: %w", err)
	}

	status, headers, err := readHTTPHead(c.br)
	if err != nil {
		return fmt.Errorf("ws: read handshake response: %w", err)
	}
	if status != 101 {
		return fmt.Errorf("ws: handshake rejected with HTTP status %d", status)
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	wantAccept := base64.StdEncoding.EncodeToString(sum[:])
	if got := headers["sec-websocket-accept"]; got != wantAccept {
		return fmt.Errorf("ws: bad Sec-WebSocket-Accept %q", got)
	}
	return nil
}

// readHTTPHead reads an HTTP response head from br, returning the
// status code and a lower-cased header map.
func readHTTPHead(br *bufio.Reader) (int, map[string]string, error) {
	statusLine, err := readLine(br)
	if err != nil {
		return 0, nil, err
	}
	var status int
	if _, err := fmt.Sscanf(statusLine, "HTTP/1.1 %d", &status); err != nil {
		return 0, nil, fmt.Errorf("malformed status line %q", statusLine)
	}
	headers := make(map[string]string)
	for {
		line, err := readLine(br)
		if err != nil {
			return 0, nil, err
		}
		if line == "" {
			return status, headers, nil
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			headers[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
}

func readLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// Send writes data as one binary message.
func (c *Conn) Send(data []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.isClosed() {
		return ErrClosed
	}
	return c.writeFrame(0x2, data)
}

// Recv returns the next complete message, reassembling fragmented
// messages and answering pings along the way. The context deadline
// bounds the wait; on expiry the returned error wraps ctx.Err().
func (c *Conn) Recv(ctx context.Context) ([]byte, error) {
	if dl, ok := ctx.Deadline(); ok {
		c.conn.SetReadDeadline(dl)
		defer c.conn.SetReadDeadline(time.Time{})
	}
	var msg []byte
	for {
		fin, opcode, payload, err := c.readFrame()
		if err != nil {
			c.markClosed()
			if isTimeout(err) && ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		switch opcode {
		case 0x9: // ping -> pong
			c.wmu.Lock()
			_ = c.writeFrame(0xA, payload)
			c.wmu.Unlock()
		case 0xA: // pong: ignore
		case 0x8: // close
			c.markClosed()
			return nil, ErrClosed
		case 0x0, 0x1, 0x2: // continuation, text, binary
			msg = append(msg, payload...)
			if len(msg) > maxMessageSize {
				c.markClosed()
				return nil, errors.New("ws: message exceeds size limit")
			}
			if fin {
				return msg, nil
			}
		default:
			return nil, fmt.Errorf("ws: unexpected opcode %d", opcode)
		}
	}
}

// Close sends a close frame and tears down the connection. It is safe
// to call more than once.
func (c *Conn) Close() error {
	c.markClosed()
	c.wmu.Lock()
	_ = c.writeFrame(0x8, nil)
	c.wmu.Unlock()
	return c.conn.Close()
}

func (c *Conn) isClosed() bool {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.closed
}

func (c *Conn) markClosed() {
	c.cmu.Lock()
	c.closed = true
	c.cmu.Unlock()
}

// writeFrame writes one FIN-set frame with a client mask.
func (c *Conn) writeFrame(opcode byte, payload []byte) error {
	var hdr [14]byte
	hdr[0] = 0x80 | opcode
	n := len(payload)
	hdrLen := 2
	switch {
	case n < 126:
		hdr[1] = 0x80 | byte(n)
	case n < 65536:
		hdr[1] = 0x80 | 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(n))
		hdrLen = 4
	default:
		hdr[1] = 0x80 | 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(n))
		hdrLen = 10
	}
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	copy(hdr[hdrLen:hdrLen+4], mask[:])
	hdrLen += 4
	if _, err := c.conn.Write(hdr[:hdrLen]); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	masked := make([]byte, n)
	for i, b := range payload {
		masked[i] = b ^ mask[i&3]
	}
	_, err := c.conn.Write(masked)
	return err
}

// readFrame reads one frame, returning its FIN flag, opcode and
// (unmasked) payload.
func (c *Conn) readFrame() (bool, byte, []byte, error) {
	var h [2]byte
	if _, err := io.ReadFull(c.br, h[:]); err != nil {
		return false, 0, nil, err
	}
	fin := h[0]&0x80 != 0
	opcode := h[0] & 0x0F
	masked := h[1]&0x80 != 0
	length := uint64(h[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if length > maxMessageSize {
		return false, 0, nil, errors.New("ws: frame exceeds size limit")
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.br, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i&3]
		}
	}
	return fin, opcode, payload, nil
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
