package mtapi

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// AccessLatency is the measured reachability of one access point:
// the TCP connect time, or the error that made it unreachable.
// This is the measurement the MT5 terminal shows next to server
// names. Ranking, display and selection policy belong to the
// application; the library only measures.
type AccessLatency struct {
	Address string // access point as dialed, "ip:port"
	Latency time.Duration
	Err     error // non-nil when the point could not be reached
}

// pingTimeout bounds one connect attempt when the caller's
// context carries no earlier deadline.
const pingTimeout = 5 * time.Second

// MeasureAccessLatency times a TCP connection to each given
// access point, concurrently, and returns one result per point in
// the order given. Addresses without a port get :443, as in
// resolution. An unreachable point is reported in its own Err,
// never as the call's error.
func MeasureAccessLatency(ctx context.Context, access []string) []AccessLatency {
	out := make([]AccessLatency, len(access))
	var wg sync.WaitGroup
	for i, addr := range access {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i] = pingAccess(ctx, addr)
		}()
	}
	wg.Wait()
	return out
}

func pingAccess(ctx context.Context, addr string) AccessLatency {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "443")
	}
	d := net.Dialer{Timeout: pingTimeout}
	start := time.Now()
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return AccessLatency{Address: addr, Err: err}
	}
	latency := time.Since(start)
	conn.Close()
	return AccessLatency{Address: addr, Latency: latency}
}

// MeasureLatency resolves a server name through the directory
// chain and measures every access point it lists. A resolution
// failure is the call's error; unreachable points are reported
// per point.
func MeasureLatency(ctx context.Context, server string, client *http.Client) ([]AccessLatency, error) {
	ips, err := FindServerIPs(ctx, server, client)
	if err != nil {
		return nil, err
	}
	return MeasureAccessLatency(ctx, ips), nil
}

// MeasureLatency resolves through the cache and measures every
// access point of the server.
func (d *DirectoryCache) MeasureLatency(ctx context.Context, server string, client *http.Client) ([]AccessLatency, error) {
	ips, err := d.FindServerIPs(ctx, server, client)
	if err != nil {
		return nil, err
	}
	return MeasureAccessLatency(ctx, ips), nil
}
