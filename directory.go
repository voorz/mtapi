package mtapi

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// SearchServers is the chained broker search for account-opening
// flows: it queries MetaQuotes' directory first, then
// mt5.mtapi.io, then the mirror, and returns the first result that
// lists at least one server. A keyword with no matches anywhere
// returns (nil, nil); an error is returned only when every
// directory failed.
func SearchServers(ctx context.Context, company string, client *http.Client) ([]CompanyResults, error) {
	var lastErr error
	for _, query := range []func(context.Context, string, *http.Client) ([]CompanyResults, error){SearchMQ, SearchMT5IO, Search} {
		companies, err := query(ctx, company, client)
		if err != nil {
			lastErr = err
			continue
		}
		if countServers(companies) > 0 {
			return companies, nil
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, nil
}

func countServers(companies []CompanyResults) int {
	n := 0
	for _, c := range companies {
		n += len(c.Results)
	}
	return n
}

// DirectoryCache is a TTL cache over the broker directory, of the
// kind every MT5 terminal keeps locally (its servers.dat): server
// lists change slowly, so repeated searches and logins should not
// re-query the directories every time. Entries are keyed by the
// lowercased company fragment or server name and are shared
// between goroutines; returned values must not be mutated.
//
// Wire one cache into dials with Config.Directory: Dial then
// resolves through the cache, and a cached access point that
// refuses the connection is evicted and re-resolved once, so a
// broker rotating its IPs heals itself at the next login. The
// cache is in-memory only; persistence is the application's
// choice.
type DirectoryCache struct {
	ttl time.Duration

	mu       sync.Mutex
	searches map[string]dirEntry[[]CompanyResults]
	servers  map[string]dirEntry[ServerInfo]
}

type dirEntry[T any] struct {
	val T
	at  time.Time
}

// NewDirectoryCache creates a cache whose entries live for ttl. A
// non-positive ttl selects 24 hours.
func NewDirectoryCache(ttl time.Duration) *DirectoryCache {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &DirectoryCache{
		ttl:      ttl,
		searches: make(map[string]dirEntry[[]CompanyResults]),
		servers:  make(map[string]dirEntry[ServerInfo]),
	}
}

func (d *DirectoryCache) fresh(at time.Time) bool {
	return time.Since(at) < d.ttl
}

// SearchServers is SearchServers served from the cache when a
// fresh entry exists.
func (d *DirectoryCache) SearchServers(ctx context.Context, company string, client *http.Client) ([]CompanyResults, error) {
	key := strings.ToLower(company)
	d.mu.Lock()
	if e, ok := d.searches[key]; ok && d.fresh(e.at) {
		d.mu.Unlock()
		return e.val, nil
	}
	d.mu.Unlock()

	companies, err := SearchServers(ctx, company, client)
	if err != nil {
		return nil, err
	}
	if countServers(companies) > 0 {
		d.mu.Lock()
		d.searches[key] = dirEntry[[]CompanyResults]{val: companies, at: time.Now()}
		d.mu.Unlock()
	}
	return companies, nil
}

// FindServer is FindServer served from the cache when a fresh
// entry exists.
func (d *DirectoryCache) FindServer(ctx context.Context, name string, client *http.Client) (*ServerInfo, error) {
	info, _, err := d.findServer(ctx, name, client)
	return info, err
}

func (d *DirectoryCache) findServer(ctx context.Context, name string, client *http.Client) (*ServerInfo, bool, error) {
	key := strings.ToLower(name)
	d.mu.Lock()
	if e, ok := d.servers[key]; ok && d.fresh(e.at) {
		d.mu.Unlock()
		info := e.val
		return &info, true, nil
	}
	d.mu.Unlock()

	info, err := FindServer(ctx, name, client)
	if err != nil {
		return nil, false, err
	}
	d.mu.Lock()
	d.servers[key] = dirEntry[ServerInfo]{val: *info, at: time.Now()}
	d.mu.Unlock()
	return info, false, nil
}

// FindServerIPs is FindServerIPs served from the cache when a
// fresh entry exists.
func (d *DirectoryCache) FindServerIPs(ctx context.Context, name string, client *http.Client) ([]string, error) {
	ips, _, err := d.resolveIPs(ctx, name, client)
	return ips, err
}

// resolveIPs additionally reports whether the resolution is
// managed by this cache (always true here): the dial path uses it
// to decide that evicting and re-resolving is possible when an
// access point refuses the connection.
func (d *DirectoryCache) resolveIPs(ctx context.Context, name string, client *http.Client) ([]string, bool, error) {
	info, _, err := d.findServer(ctx, name, client)
	if err != nil {
		return nil, false, err
	}
	return info.Access, true, nil
}

// invalidateServer drops one server's cached entry, so the next
// lookup re-queries the directories.
func (d *DirectoryCache) invalidateServer(name string) {
	d.mu.Lock()
	delete(d.servers, strings.ToLower(name))
	d.mu.Unlock()
}

// Clear drops every cached entry.
func (d *DirectoryCache) Clear() {
	d.mu.Lock()
	d.searches = make(map[string]dirEntry[[]CompanyResults])
	d.servers = make(map[string]dirEntry[ServerInfo])
	d.mu.Unlock()
}
