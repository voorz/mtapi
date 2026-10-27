package ws

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// --- minimal server-side WebSocket used only by this test ---

func serverReadFrame(r *bufio.Reader) (byte, []byte, error) {
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

func serverWriteFrame(w io.Writer, fin bool, opcode byte, payload []byte) error {
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

func upgradeHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Sec-WebSocket-Key")
		sum := sha1.Sum([]byte(key + wsGUID))
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("no hijacker")
			return
		}
		conn, rw, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer conn.Close()
		_, err = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + base64.StdEncoding.EncodeToString(sum[:]) + "\r\n\r\n")
		if err != nil {
			t.Errorf("write handshake: %v", err)
			return
		}
		rw.Flush()
		reader := rw.Reader

		// 1. Echo one message.
		_, payload, err := serverReadFrame(reader)
		if err != nil {
			t.Errorf("server read echo: %v", err)
			return
		}
		if err := serverWriteFrame(conn, true, 0x2, payload); err != nil {
			t.Errorf("server echo: %v", err)
			return
		}
		// 2. Send a fragmented message.
		serverWriteFrame(conn, false, 0x2, []byte("frag-"))
		serverWriteFrame(conn, true, 0x0, []byte("ok"))
		// 3. Ping, expect a pong, then send a final message.
		serverWriteFrame(conn, true, 0x9, []byte("x"))
		op, _, err := serverReadFrame(reader)
		if err != nil || op != 0xA {
			t.Errorf("server expected pong, got op=%d err=%v", op, err)
			return
		}
		serverWriteFrame(conn, true, 0x2, []byte("after-pong"))
		time.Sleep(200 * time.Millisecond)
	}
}

func TestDialEchoFragmentPing(t *testing.T) {
	srv := httptest.NewTLSServer(upgradeHandler(t))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "https://")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr, "/terminal", Options{Origin: "https://" + addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if err := c.Send([]byte("hello mt5")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	msg, err := c.Recv(ctx)
	if err != nil || string(msg) != "hello mt5" {
		t.Fatalf("echo = %q, %v", msg, err)
	}
	msg, err = c.Recv(ctx)
	if err != nil || string(msg) != "frag-ok" {
		t.Fatalf("fragment = %q, %v", msg, err)
	}
	msg, err = c.Recv(ctx)
	if err != nil || string(msg) != "after-pong" {
		t.Fatalf("after pong = %q, %v", msg, err)
	}
}

func TestRecvTimeout(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Sec-WebSocket-Key")
		sum := sha1.Sum([]byte(key + wsGUID))
		hj := w.(http.Hijacker)
		conn, rw, _ := hj.Hijack()
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
			base64.StdEncoding.EncodeToString(sum[:]) + "\r\n\r\n")
		rw.Flush()
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "https://")

	c, err := Dial(context.Background(), addr, "/terminal", Options{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := c.Recv(ctx); err == nil {
		t.Fatal("expected timeout error")
	}
}
