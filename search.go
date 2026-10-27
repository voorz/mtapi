package mtapi

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Broker search endpoints. Server names (e.g. "Exness-MT5Trial17") are
// resolved to access-point addresses before connecting: SearchMQ is
// MetaQuotes' own signed directory service; search.mtapi.io is the
// fallback mirror used by the reference implementation.
var (
	searchMQURL = "https://updates.metaquotes.net/public/mt5/network"
	searchURL   = "http://search.mtapi.io/Search"
)

// searchHMACKey signs SearchMQ requests (see computeSearchSignature).
var searchHMACKey = []byte{
	61, 123, 21, 22, 214, 234, 187, 52, 217, 214,
	99, 227, 98, 62, 27, 215, 251, 220, 174, 244,
	87, 59, 223, 53, 127, 168, 207, 11, 190, 173,
	146, 127,
}

const searchUserAgent = "MetaTrader 5 Terminal/5.5830 (Windows NT 10.0.22621; x64)"

// searchTimeout bounds one directory request when the caller's
// context carries no deadline of its own (the reference uses 10s).
const searchTimeout = 10 * time.Second

// CompanyResults groups the servers of one broker company, as
// returned by both directory endpoints.
type CompanyResults struct {
	Company string
	Results []ServerInfo
}

// flexBool decodes a JSON value that may be a bool or a 0/1 number.
type flexBool bool

func (b *flexBool) UnmarshalJSON(data []byte) error {
	s := strings.TrimSpace(string(data))
	*b = flexBool(s == "true" || s == "1")
	return nil
}

type searchResponse struct {
	Result []struct {
		Company string `json:"company"`
		Results []struct {
			Name   string   `json:"name"`
			Site   string   `json:"site"`
			Access []string `json:"access"`
			IsDemo flexBool `json:"is_demo"`
		} `json:"results"`
	} `json:"result"`
}

func (r *searchResponse) companies() []CompanyResults {
	out := make([]CompanyResults, 0, len(r.Result))
	for _, c := range r.Result {
		cr := CompanyResults{Company: c.Company}
		for _, s := range c.Results {
			cr.Results = append(cr.Results, ServerInfo{
				Company: c.Company,
				Name:    s.Name,
				Site:    s.Site,
				Access:  s.Access,
				IsDemo:  bool(s.IsDemo),
			})
		}
		out = append(out, cr)
	}
	return out
}

func searchClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

func searchCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, searchTimeout)
}

// computeSearchSignature signs a SearchMQ request body:
// hex(MD5(MD5(body) || HMAC key)).
func computeSearchSignature(body string) string {
	inner := md5.Sum([]byte(body))
	h := md5.New()
	h.Write(inner[:])
	h.Write(searchHMACKey)
	return hex.EncodeToString(h.Sum(nil))
}

// lcgTID derives the 16-character fingerprint id used by the SearchMQ
// cookie from a seed, following the terminal's generator: a 256-byte
// LCG stream is hashed with MD5 and byte 0 is replaced by the checksum
// of bytes 1..15.
func lcgTID(seed uint32) string {
	stream := make([]byte, 256)
	s := seed
	for i := range stream {
		s = (s*214013 + 2531011) & 0xFFFFFFFF
		stream[i] = byte((s >> 16) & 0xFF)
	}
	digest := md5.Sum(stream)
	var sum byte
	for j := 1; j < 16; j++ {
		sum += digest[j]
	}
	digest[0] = sum
	return strings.ToUpper(hex.EncodeToString(digest[:8]))
}

// searchCookie builds the "_fz_uniq" fingerprint cookie the SearchMQ
// endpoint expects.
func searchCookie(now time.Time) string {
	timestamp := now.Unix()
	ms := (now.UnixNano() / 1_000_000) & 0x1FFFFFF
	val := ((timestamp - 1420070400) | (ms << 32)) | 0x4200000000000000
	tid := lcgTID(uint32(now.UnixNano() & 0xFFFFFFFF))
	age := timestamp - 86400
	return fmt.Sprintf("_fz_uniq=%d;uniq=%d;age=%d;tid=%s", val, val, age, tid)
}

// SearchMQ queries MetaQuotes' directory for companies matching the
// given name fragment.
func SearchMQ(ctx context.Context, company string, client *http.Client) ([]CompanyResults, error) {
	ctx, cancel := searchCtx(ctx)
	defer cancel()

	body := "company=" + company + "&code=mt5"
	fullBody := body + "&signature=" + computeSearchSignature(body) + "&ver=2"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, searchMQURL, strings.NewReader(fullBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en")
	req.Header.Set("User-Agent", searchUserAgent)
	req.Header.Set("Cookie", searchCookie(time.Now()))

	resp, err := searchClient(client).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mtapi: SearchMQ returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	// The response may carry a non-JSON prefix; the payload starts at
	// the first '{'.
	start := strings.Index(string(raw), "{")
	if start < 0 {
		return nil, fmt.Errorf("mtapi: no JSON in SearchMQ response")
	}
	var parsed searchResponse
	if err := json.Unmarshal(raw[start:], &parsed); err != nil {
		return nil, fmt.Errorf("mtapi: decode SearchMQ response: %w", err)
	}
	return parsed.companies(), nil
}

// Search queries the fallback directory mirror (search.mtapi.io).
func Search(ctx context.Context, company string, client *http.Client) ([]CompanyResults, error) {
	ctx, cancel := searchCtx(ctx)
	defer cancel()

	u := searchURL + "?company=" + url.QueryEscape(company) + "&mt5=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", searchUserAgent)
	resp, err := searchClient(client).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mtapi: search returned HTTP %d", resp.StatusCode)
	}
	var parsed searchResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("mtapi: decode search response: %w", err)
	}
	return parsed.companies(), nil
}

// FindServer resolves a server name to its ServerInfo. It tries
// SearchMQ first and falls back to the mirror, matching names
// case-insensitively, as the reference does. The returned error wraps
// ErrServerNotFound when no directory lists the server.
func FindServer(ctx context.Context, name string, client *http.Client) (*ServerInfo, error) {
	var lastErr error
	for _, query := range []func(context.Context, string, *http.Client) ([]CompanyResults, error){SearchMQ, Search} {
		companies, err := query(ctx, name, client)
		if err != nil {
			lastErr = err
			continue
		}
		for _, c := range companies {
			for _, s := range c.Results {
				if strings.EqualFold(s.Name, name) {
					info := s
					return &info, nil
				}
			}
		}
	}
	if lastErr != nil {
		return nil, fmt.Errorf("%w: %q (directory lookup failed: %v)", ErrServerNotFound, name, lastErr)
	}
	return nil, fmt.Errorf("%w: %q", ErrServerNotFound, name)
}

// FindServerIPs resolves a server name to its access points
// ("ip:port" strings).
func FindServerIPs(ctx context.Context, name string, client *http.Client) ([]string, error) {
	info, err := FindServer(ctx, name, client)
	if err != nil {
		return nil, err
	}
	return info.Access, nil
}
