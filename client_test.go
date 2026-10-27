package mtapi_test

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/voorz/mtapi"
	"github.com/voorz/mtapi/internal/cryptox"
	"github.com/voorz/mtapi/internal/wire"
)

// fakeMT5 is an in-process MT5 access server speaking just enough of
// the protocol to exercise Dial and the read APIs end to end.
type fakeMT5 struct {
	t           *testing.T
	sessionKey  []byte
	mu          sync.Mutex
	gotLogin    uint64
	gotPassword string
	subscribed  []uint32
	posOpen     bool
	pendOrder   bool
}

func (f *fakeMT5) respond(cmdID uint16, body []byte) []byte {
	plain := make([]byte, 5+len(body))
	binary.LittleEndian.PutUint16(plain[0:2], cmdID)
	binary.LittleEndian.PutUint16(plain[2:4], cmdID)
	plain[4] = 0
	copy(plain[5:], body)
	key := f.sessionKey
	if cmdID == wire.CmdAuth {
		key = cryptox.StaticKey
	}
	enc, err := cryptox.AESEncrypt(key, plain)
	if err != nil {
		f.t.Errorf("encrypt response: %v", err)
		return nil
	}
	return wire.PackWire(enc)
}

func (f *fakeMT5) accountBody() []byte {
	body := make([]byte, 816)
	binary.LittleEndian.PutUint32(body[1:5], 12345) // login
	binary.LittleEndian.PutUint64(body[9:17], math.Float64bits(1000.5))
	binary.LittleEndian.PutUint64(body[17:25], math.Float64bits(1001.5))
	copy(body[25:89], wire.EncodeUTF16LE("USD"))
	binary.LittleEndian.PutUint16(body[353:355], 500) // leverage
	return body
}

func (f *fakeMT5) symbolsBody() []byte {
	var rec bytes.Buffer
	binary.Write(&rec, binary.LittleEndian, uint32(1))
	mh := make([]byte, 526)
	copy(mh[0:64], wire.EncodeUTF16LE("EURUSD"))
	binary.LittleEndian.PutUint32(mh[192:196], 5)  // digits
	binary.LittleEndian.PutUint32(mh[196:200], 42) // symbol id
	rec.Write(mh)
	var zbuf bytes.Buffer
	zw := zlib.NewWriter(&zbuf)
	zw.Write(rec.Bytes())
	zw.Close()
	return append([]byte{0, 0, 0, 0}, zbuf.Bytes()...)
}

func (f *fakeMT5) quoteBody() []byte {
	body := make([]byte, wire.QuoteSchema.Size())
	binary.LittleEndian.PutUint32(body[0:4], 42)
	binary.LittleEndian.PutUint64(body[12:20], math.Float64bits(108523))
	binary.LittleEndian.PutUint64(body[20:28], math.Float64bits(108545))
	return body
}

func (f *fakeMT5) handle(conn io.ReadWriter, r *bufio.Reader) {
	for {
		_, payload, err := srvReadFrame(r)
		if err != nil {
			return
		}
		// One wire frame per message: strip header, decrypt, dispatch.
		resp := f.dispatch(payload)
		for _, msg := range resp {
			if err := srvWriteFrame(conn, true, 0x2, msg); err != nil {
				return
			}
		}
	}
}

func (f *fakeMT5) dispatch(frame []byte) [][]byte {
	if len(frame) <= 8 {
		return nil
	}
	key := cryptox.StaticKey
	plain, err := cryptox.AESDecrypt(key, frame[8:])
	if err != nil && !isPadding(err) {
		return nil
	}
	if len(plain) < 4 {
		return nil
	}
	cmd := binary.LittleEndian.Uint16(plain[2:4])
	if cmd != wire.CmdAuth {
		plain, err = cryptox.AESDecrypt(f.sessionKey, frame[8:])
		if err != nil && !isPadding(err) {
			return nil
		}
		if len(plain) < 4 {
			return nil
		}
		cmd = binary.LittleEndian.Uint16(plain[2:4])
	}

	switch cmd {
	case wire.CmdAuth:
		body := make([]byte, 66, 98)
		body = append(body, f.sessionKey...)
		return [][]byte{f.respond(cmd, body)}
	case wire.CmdLogin:
		payload := plain[4:]
		f.mu.Lock()
		f.gotLogin = binary.LittleEndian.Uint64(payload[736:744])
		f.gotPassword = wire.DecodeUTF16LE(payload[4:476])
		f.mu.Unlock()
		return [][]byte{f.respond(cmd, make([]byte, 168))}
	case wire.CmdAccount:
		return [][]byte{f.respond(cmd, f.accountBody())}
	case wire.CmdSymbols:
		return [][]byte{f.respond(cmd, f.symbolsBody())}
	case wire.CmdSubscribe:
		n := binary.LittleEndian.Uint32(plain[4:8])
		f.mu.Lock()
		for i := uint32(0); i < n; i++ {
			f.subscribed = append(f.subscribed, binary.LittleEndian.Uint32(plain[8+4*i:12+4*i]))
		}
		f.mu.Unlock()
		return [][]byte{f.respond(wire.CmdQuotes, f.quoteBody())}
	case wire.CmdTrade:
		return f.handleTrade(plain[4:])
	case wire.CmdPositions:
		return [][]byte{f.respond(cmd, f.positionsBody())}
	case wire.CmdDeals:
		return [][]byte{f.respond(cmd, make([]byte, 4))}
	case wire.CmdRates:
		body := make([]byte, 48)
		binary.LittleEndian.PutUint32(body[0:4], 1700000000)
		binary.LittleEndian.PutUint64(body[4:12], math.Float64bits(1.085))
		return [][]byte{f.respond(cmd, body)}
	case wire.CmdHeartbeat:
		return nil
	}
	return nil
}

// handleTrade answers a command-12 request. Most actions get both a
// bare command-12 response and a detailed command-19 event; modify
// actions get only the command-12 response, exercising the client's
// fallback path.
func (f *fakeMT5) handleTrade(op []byte) [][]byte {
	action := binary.LittleEndian.Uint32(op[4:8])
	tradePos := int64(binary.LittleEndian.Uint64(op[228:236]))
	tradeOrder := int64(binary.LittleEndian.Uint64(op[84:92]))
	volume := binary.LittleEndian.Uint64(op[72:80])

	respBody := make([]byte, 4)
	binary.LittleEndian.PutUint32(respBody, 10009)
	cmd12 := f.respond(wire.CmdTrade, respBody)

	event := func(order, deal int64) []byte {
		body := make([]byte, 380)
		copy(body[4:252], op)
		ap := 252
		binary.LittleEndian.PutUint32(body[ap:ap+4], 10009)
		binary.LittleEndian.PutUint64(body[ap+4:ap+12], uint64(deal))
		binary.LittleEndian.PutUint64(body[ap+12:ap+20], uint64(order))
		binary.LittleEndian.PutUint64(body[ap+20:ap+28], volume)
		binary.LittleEndian.PutUint64(body[ap+28:ap+36], math.Float64bits(1.0853))
		copy(body[ap+64:ap+128], wire.EncodeUTF16LE("ok"))
		return f.respond(wire.CmdTradeEvent, body)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	switch action {
	case wire.TradeMarket:
		if tradePos == 0 {
			f.posOpen = true
			return [][]byte{cmd12, event(900001, 800001)}
		}
		f.posOpen = false
		return [][]byte{cmd12, event(900001, 800002)}
	case wire.TradePending:
		f.pendOrder = true
		return [][]byte{cmd12, event(900002, 0)}
	case wire.TradeCancel:
		f.pendOrder = false
		return [][]byte{cmd12, event(tradeOrder, 0)}
	case wire.TradeModify, wire.TradeModifyOrder:
		return [][]byte{cmd12}
	}
	return [][]byte{cmd12}
}

// positionsBody builds a command-4 body from the fake's trade state.
func (f *fakeMT5) positionsBody() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	var body bytes.Buffer
	posCount := uint32(0)
	if f.posOpen {
		posCount = 1
	}
	binary.Write(&body, binary.LittleEndian, posCount)
	if f.posOpen {
		rec := make([]byte, 344)
		binary.LittleEndian.PutUint64(rec[0:8], 900001)
		binary.LittleEndian.PutUint32(rec[16:20], 1700000000)
		copy(rec[24:88], wire.EncodeUTF16LE("EURUSD"))
		binary.LittleEndian.PutUint32(rec[88:92], 0) // buy
		binary.LittleEndian.PutUint64(rec[92:100], math.Float64bits(1.0853))
		binary.LittleEndian.PutUint64(rec[124:132], 1000000) // 0.01 lots
		body.Write(rec)
	}
	ordCount := uint32(0)
	if f.pendOrder {
		ordCount = 1
	}
	binary.Write(&body, binary.LittleEndian, ordCount)
	if f.pendOrder {
		rec := make([]byte, 356)
		binary.LittleEndian.PutUint64(rec[0:8], 900002)
		copy(rec[72:136], wire.EncodeUTF16LE("EURUSD"))
		binary.LittleEndian.PutUint32(rec[148:152], 2) // buy limit
		binary.LittleEndian.PutUint32(rec[204:208], 1000000)
		binary.LittleEndian.PutUint64(rec[336:344], math.Float64bits(1.08))
		body.Write(rec)
	}
	return body.Bytes()
}

func isPadding(err error) bool {
	return err != nil && (err.Error() == "cryptox: invalid PKCS#7 padding")
}

// --- server-side frame helpers (tests only) ---

func srvReadFrame(r *bufio.Reader) (byte, []byte, error) {
	h := make([]byte, 2)
	if _, err := io.ReadFull(r, h); err != nil {
		return 0, nil, err
	}
	opcode := h[0] & 0x0F
	masked := h[1]&0x80 != 0
	length := uint64(h[1] & 0x7F)
	if length == 126 {
		ext := make([]byte, 2)
		io.ReadFull(r, ext)
		length = uint64(binary.BigEndian.Uint16(ext))
	} else if length == 127 {
		ext := make([]byte, 8)
		io.ReadFull(r, ext)
		length = binary.BigEndian.Uint64(ext)
	}
	var mask [4]byte
	if masked {
		io.ReadFull(r, mask[:])
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i&3]
		}
	}
	return opcode, payload, nil
}

func srvWriteFrame(w io.Writer, fin bool, opcode byte, payload []byte) error {
	b0 := opcode
	if fin {
		b0 |= 0x80
	}
	hdr := []byte{b0}
	switch {
	case len(payload) < 126:
		hdr = append(hdr, byte(len(payload)))
	case len(payload) < 65536:
		hdr = append(hdr, 126, 0, 0)
		binary.BigEndian.PutUint16(hdr[2:4], uint16(len(payload)))
	default:
		hdr = append(hdr, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(hdr[2:10], uint64(len(payload)))
	}
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func startFakeMT5(t *testing.T) (*fakeMT5, string) {
	f := &fakeMT5{t: t, sessionKey: bytes.Repeat([]byte{0x42}, 32)}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Sec-WebSocket-Key")
		sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		hj := w.(http.Hijacker)
		conn, rw, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
			base64.StdEncoding.EncodeToString(sum[:]) + "\r\n\r\n")
		rw.Flush()
		f.handle(conn, rw.Reader)
	})
	srv := httptest.NewTLSServer(handler)
	t.Cleanup(srv.Close)
	return f, strings.TrimPrefix(srv.URL, "https://")
}

func TestDialEndToEnd(t *testing.T) {
	fake, addr := startFakeMT5(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := mtapi.Dial(ctx, mtapi.Config{
		Login:    12345,
		Password: "pw123",
		Server:   "Fake-MT5",
		Host:     addr,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	fake.mu.Lock()
	if fake.gotLogin != 12345 || fake.gotPassword != "pw123" {
		t.Fatalf("server saw login=%d password=%q", fake.gotLogin, fake.gotPassword)
	}
	fake.mu.Unlock()

	acct, err := client.Account(ctx)
	if err != nil {
		t.Fatalf("Account: %v", err)
	}
	if acct.Login != 12345 || acct.Balance != 1000.5 || acct.Equity != 1001.5 || acct.Currency != "USD" || acct.Leverage != 500 {
		t.Fatalf("account = %+v", acct)
	}

	symbols, err := client.Symbols(ctx)
	if err != nil {
		t.Fatalf("Symbols: %v", err)
	}
	sym, ok := symbols["EURUSD"]
	if !ok || sym.ID != 42 || sym.Digits != 5 {
		t.Fatalf("symbols = %+v", symbols)
	}

	quote, err := client.SubscribeQuote(ctx, "EURUSD")
	if err != nil {
		t.Fatalf("SubscribeQuote: %v", err)
	}
	if quote.Bid != 1.08523 || quote.Ask != 1.08545 {
		t.Fatalf("quote = %+v", quote)
	}

	positions, err := client.Positions(ctx)
	if err != nil || len(positions) != 0 {
		t.Fatalf("positions = %v, %v", positions, err)
	}
	deals, err := client.Deals(ctx, time.Time{}, time.Time{})
	if err != nil || len(deals) != 0 {
		t.Fatalf("deals = %v, %v", deals, err)
	}
	candles, err := client.Candles(ctx, "EURUSD", mtapi.TFM1, 10)
	if err != nil || len(candles) != 1 || candles[0].Open != 1.085 {
		t.Fatalf("candles = %+v, %v", candles, err)
	}
}

func TestDialBadPasswordShape(t *testing.T) {
	// A wrong-shaped config must fail before any network activity.
	_, err := mtapi.Dial(context.Background(), mtapi.Config{Server: "X"})
	if err == nil {
		t.Fatal("expected error for missing login/password")
	}
}
