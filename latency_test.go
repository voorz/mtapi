package mtapi

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func listenLocal(t *testing.T) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

func TestMeasureAccessLatency(t *testing.T) {
	live, closeFn := listenLocal(t)
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	results := MeasureAccessLatency(ctx, []string{live, "127.0.0.1:1", live})
	if len(results) != 3 {
		t.Fatalf("results = %d, want 3", len(results))
	}
	if results[0].Err != nil || results[0].Address != live {
		t.Fatalf("live point = %+v", results[0])
	}
	if results[1].Err == nil {
		t.Fatalf("dead point unexpectedly reachable: %+v", results[1])
	}
	if results[2].Err != nil {
		t.Fatalf("second live point = %+v", results[2])
	}
}

func TestMeasureLatencyViaDirectory(t *testing.T) {
	live, closeFn := listenLocal(t)
	defer closeFn()

	mq := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"result":[{"company":"Exness","results":[{"name":"Exness-MT5Trial17","access":[%q],"is_demo":1}]}]}`, live)
	})
	stubDirs(t, mq, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	results, err := MeasureLatency(ctx, "Exness-MT5Trial17", nil)
	if err != nil {
		t.Fatalf("MeasureLatency: %v", err)
	}
	if len(results) != 1 || results[0].Err != nil || results[0].Address != live {
		t.Fatalf("results = %+v", results)
	}

	// The cache-backed variant resolves identically.
	cache := NewDirectoryCache(time.Hour)
	results, err = cache.MeasureLatency(ctx, "Exness-MT5Trial17", nil)
	if err != nil || len(results) != 1 || results[0].Err != nil {
		t.Fatalf("cache MeasureLatency = %+v, %v", results, err)
	}

	// An unknown server is a resolution error, not per-point errors.
	if _, err := MeasureLatency(ctx, "NoSuchServer", nil); err == nil {
		t.Fatal("expected resolution error")
	}
}
