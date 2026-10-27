package mtapi

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// stubDirs points all three directory endpoints at the given
// handlers (nil = HTTP 502) and counts requests per endpoint.
func stubDirs(t *testing.T, mq, io5, mirror http.HandlerFunc) (counts *[3]atomic.Int32) {
	t.Helper()
	counts = &[3]atomic.Int32{}
	oldMQ, oldSearch, oldIO := searchMQURL, searchURL, searchMT5IO
	t.Cleanup(func() { searchMQURL, searchURL, searchMT5IO = oldMQ, oldSearch, oldIO })

	mk := func(i int, h http.HandlerFunc) string {
		if h == nil {
			h = func(w http.ResponseWriter, r *http.Request) { http.Error(w, "down", http.StatusBadGateway) }
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			counts[i].Add(1)
			h(w, r)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	searchMQURL = mk(0, mq)
	searchMT5IO = mk(1, io5)
	searchURL = mk(2, mirror)
	return counts
}

func serveJSON(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }
}

const ioDirectoryJSON = `[{"companyName":"Exness","results":[{"name":"Exness-MT5Trial17","access":["13.213.81.113:443"]}]}]`

func TestSearchServersChain(t *testing.T) {
	ctx := context.Background()

	// MQ empty -> mtapi.io serves.
	stubDirs(t, serveJSON(`{"result":[]}`), serveJSON(ioDirectoryJSON), nil)
	companies, err := SearchServers(ctx, "Exness", nil)
	if err != nil || len(companies) != 1 || companies[0].Results[0].Name != "Exness-MT5Trial17" {
		t.Fatalf("fall to io: companies=%v err=%v", companies, err)
	}

	// MQ down -> io down -> mirror serves.
	stubDirs(t, nil, nil, serveJSON(sampleDirectoryJSON))
	companies, err = SearchServers(ctx, "Exness", nil)
	if err != nil || len(companies) != 1 || len(companies[0].Results) != 2 {
		t.Fatalf("fall to mirror: companies=%v err=%v", companies, err)
	}

	// No matches anywhere -> (nil, nil), not an error.
	stubDirs(t, serveJSON(`{"result":[]}`), serveJSON(`[]`), serveJSON(`{"result":[]}`))
	companies, err = SearchServers(ctx, "ZZZ", nil)
	if err != nil || companies != nil {
		t.Fatalf("no match: companies=%v err=%v", companies, err)
	}

	// Every directory down -> error.
	stubDirs(t, nil, nil, nil)
	if _, err := SearchServers(ctx, "Exness", nil); err == nil {
		t.Fatal("all down: expected error")
	}
}

func TestDirectoryCache(t *testing.T) {
	counts := stubDirs(t, serveJSON(sampleDirectoryJSON), nil, nil)
	ctx := context.Background()
	cache := NewDirectoryCache(time.Hour)

	// Two searches (different case) -> one directory round.
	for _, kw := range []string{"Exness", "exness"} {
		companies, err := cache.SearchServers(ctx, kw, nil)
		if err != nil || len(companies) != 1 {
			t.Fatalf("search %q: %v %+v", kw, err, companies)
		}
	}
	if got := counts[0].Load(); got != 1 {
		t.Fatalf("MQ requests = %d, want 1", got)
	}

	// Two resolutions -> still one round (name key).
	for i := 0; i < 2; i++ {
		ips, err := cache.FindServerIPs(ctx, "Exness-MT5Trial17", nil)
		if err != nil || len(ips) != 1 || ips[0] != "13.213.81.113:443" {
			t.Fatalf("resolve %d: %v %v", i, ips, err)
		}
	}
	if got := counts[0].Load(); got != 2 {
		t.Fatalf("MQ requests = %d, want 2 (1 search + 1 resolve)", got)
	}

	// Clear forces a refetch.
	cache.Clear()
	if _, err := cache.FindServerIPs(ctx, "Exness-MT5Trial17", nil); err != nil {
		t.Fatal(err)
	}
	if got := counts[0].Load(); got != 3 {
		t.Fatalf("MQ requests = %d, want 3 after Clear", got)
	}

	// TTL expiry forces a refetch.
	short := NewDirectoryCache(50 * time.Millisecond)
	if _, err := short.FindServerIPs(ctx, "Exness-MT5Trial17", nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(90 * time.Millisecond)
	if _, err := short.FindServerIPs(ctx, "Exness-MT5Trial17", nil); err != nil {
		t.Fatal(err)
	}
	if got := counts[0].Load(); got != 5 {
		t.Fatalf("MQ requests = %d, want 5 after TTL expiry", got)
	}
}

// A cached access point that refuses the connection is evicted and
// re-resolved once, so Dial reaches the live endpoint without
// waiting for the TTL.
func TestDialEvictsStaleDirectoryEntry(t *testing.T) {
	// Live endpoint: accepts the WebSocket upgrade, then hangs up
	// (the MT5 handshake behind it is out of scope here; reaching
	// it is the assertion).
	var liveHits atomic.Int32
	live := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveHits.Add(1)
		key := r.Header.Get("Sec-WebSocket-Key")
		sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		hj := w.(http.Hijacker)
		conn, rw, err := hj.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
			base64.StdEncoding.EncodeToString(sum[:]) + "\r\n\r\n")
		rw.Flush()
	}))
	defer live.Close()
	liveAddr := strings.TrimPrefix(live.URL, "https://")

	// Directory: first lookup hands out a dead access point, later
	// lookups the live one.
	var dirHits atomic.Int32
	mq := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := dirHits.Add(1)
		access := liveAddr
		if n == 1 {
			access = "127.0.0.1:1"
		}
		fmt.Fprintf(w, `{"result":[{"company":"Exness","results":[{"name":"Exness-MT5Trial17","access":[%q],"is_demo":1}]}]}`, access)
	})
	stubDirs(t, mq, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := Dial(ctx, Config{
		Login: 12345, Password: "pw123", Server: "Exness-MT5Trial17",
		Directory: NewDirectoryCache(time.Hour),
	})
	if err == nil {
		t.Fatal("Dial unexpectedly succeeded against a stub endpoint")
	}
	if got := dirHits.Load(); got != 2 {
		t.Fatalf("directory lookups = %d, want 2 (stale + re-resolve)", got)
	}
	if got := liveHits.Load(); got != 1 {
		t.Fatalf("live endpoint hits = %d, want 1", got)
	}
}
