package mtapi

import (
	"context"
	"time"

	"github.com/voorz/mtapi/internal/wire"
)

// Account fetches the current account state (command 3) and caches
// it. The cached copy is also available via CachedAccount.
func (c *Client) Account(ctx context.Context) (Account, error) {
	resp, err := c.sendAndWait(ctx, wire.CmdAccount, nil, wire.CmdAccount)
	if err != nil {
		return Account{}, err
	}
	acct := parseAccount(resp.Body)
	c.stateMu.Lock()
	c.account = acct
	c.hasAccount = true
	c.stateMu.Unlock()
	return acct, nil
}

// CachedAccount returns the most recently fetched account without a
// network round trip. The second result reports whether an account
// has been loaded (Dial loads one, so a live client always has).
func (c *Client) CachedAccount() (Account, bool) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.account, c.hasAccount
}

// parseAccount parses the "fl" account record. Field indexes follow
// the reference parser exactly; free_margin is not in the record.
func parseAccount(body []byte) Account {
	vals, _ := wire.ParseSeries(body, wire.AccountSchema, 0)
	return Account{
		Login:    vals.Int(1),
		Balance:  vals.Float(3),
		Equity:   vals.Float(4),
		Currency: vals.Str(5),
		Group:    vals.Str(8),
		Leverage: int(vals.Int(9)),
		Server:   vals.Str(10),
		Profit:   vals.Float(16),
		Margin:   vals.Float(17),
	}
}

// timeFromUnix converts a protocol unix timestamp to a UTC time. Out
// of range values fall back to the current time, mirroring the
// reference's ts_to_dt.
func timeFromUnix(ts int64) time.Time {
	if ts < -62135596800 || ts > 253402300799 {
		return time.Now().UTC()
	}
	return time.Unix(ts, 0).UTC()
}
