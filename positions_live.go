package mtapi

import (
	"context"
	"encoding/binary"
	"strings"
	"time"

	"github.com/voorz/mtapi/internal/wire"
)

// Live position profits.
//
// Some broker clusters (observed on the Exness web-terminal line)
// answer command 4 on an established session with the position
// records as of login: the Profit fields never move again for the
// life of the session, and the server pushes no position updates
// (a 60-second frame census on a live session showed heartbeats
// only). A fresh login reads current values; a long-lived session
// silently serves a frozen snapshot. Other clusters (Doo) recompute
// server-side on every request and stay live. The reference client
// has the identical read model and freezes the same way (verified
// live, 2026-11-10).
//
// The genuine web terminal has the same server behaviour and stays
// live by computing floating profit locally from the streamed
// quotes. livePositions mirrors that: positions whose symbol has a
// streamed quote and a known contract size get their Profit
// recomputed from the quote; every other position keeps the
// server-reported value. Symbols of open positions are subscribed
// best-effort so quotes start flowing, and their contract sizes
// are fetched with command 18 (see fetchSpec).
//
// The recomputation is currency-gated: a price difference times a
// contract size is denominated in the symbol's profit currency,
// so the formula is only applied when that currency is the
// account currency (see profitCurrency). Positions in any other
// denomination keep the server value rather than risk a wrong
// conversion.

// specRetryInterval bounds how often a missing contract size is
// re-requested for one symbol.
const specRetryInterval = 60 * time.Second

// specRecordSize is the size of one symbol record in a command-18
// response, and the offsets of the two fields this library reads
// from it (verified by differential scan across XAUUSDm, EURUSDm,
// USDJPYm and XAGUSDm on a live server, 2026-11-10).
const (
	specRecordPointOffset        = 1396
	specRecordContractSizeOffset = 1456
	specRecordMinLength          = specRecordContractSizeOffset + 8
)

// livePositions refreshes the Profit fields of positions from the
// client's streamed quotes where possible, and arranges for the
// inputs (subscriptions, contract sizes) to be available on later
// reads.
func (c *Client) livePositions(ctx context.Context, positions []Position) []Position {
	if len(positions) == 0 {
		return positions
	}

	// Subscribe to position symbols without a live subscription so
	// quotes start flowing; this read still uses the server values
	// for them, later reads recompute.
	c.stateMu.RLock()
	missing := make([]string, 0, len(positions))
	seen := make(map[string]bool, len(positions))
	for _, p := range positions {
		if !c.subscribed[p.Symbol] && !seen[p.Symbol] {
			seen[p.Symbol] = true
			missing = append(missing, p.Symbol)
		}
	}
	c.stateMu.RUnlock()
	if len(missing) > 0 {
		subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = c.Subscribe(subCtx, missing...) // best effort by design
		cancel()
	}

	// Fetch contract sizes for position symbols that lack one
	// (throttled per symbol).
	needSpec := make([]string, 0, len(positions))
	seen = make(map[string]bool, len(positions))
	c.stateMu.RLock()
	for _, p := range positions {
		if seen[p.Symbol] {
			continue
		}
		seen[p.Symbol] = true
		if sym, ok := c.symbols[p.Symbol]; ok && sym.ContractSize <= 0 {
			if time.Since(c.specTried[p.Symbol]) >= specRetryInterval {
				needSpec = append(needSpec, p.Symbol)
			}
		}
	}
	c.stateMu.RUnlock()
	for _, name := range needSpec {
		c.stateMu.Lock()
		c.specTried[name] = time.Now()
		c.stateMu.Unlock()
		fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		c.fetchSpec(fetchCtx, name) // best effort by design
		cancel()
	}

	acct, ok := c.CachedAccount()
	if !ok || acct.Currency == "" {
		return positions
	}
	out := make([]Position, len(positions))
	copy(out, positions)
	for i := range out {
		p := &out[i]
		if profitCurrency(p.Symbol) != acct.Currency {
			continue
		}
		sym, ok := c.Symbol(p.Symbol)
		if !ok || sym.ContractSize <= 0 {
			continue
		}
		quote, ok := c.Quote(p.Symbol)
		if !ok {
			continue
		}
		if profit, ok := profitFromQuote(*p, quote, sym.ContractSize); ok {
			p.Profit = profit
		}
	}
	return out
}

// fetchSpec requests one symbol's full specification (command 18)
// and merges the contract size and point into the cached symbol.
// The response body is [count:uint32][record ...]; only the first
// record is read, and only when it carries the requested name.
func (c *Client) fetchSpec(ctx context.Context, name string) {
	c.stateMu.RLock()
	sym, ok := c.symbols[name]
	c.stateMu.RUnlock()
	if !ok {
		return
	}
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload[0:4], 1)
	binary.LittleEndian.PutUint32(payload[4:8], uint32(sym.ID))
	resp, err := c.sendAndWait(ctx, wire.CmdSpecInfo, payload, wire.CmdSpecInfo)
	if err != nil || len(resp.Body) < 4 {
		return
	}
	if binary.LittleEndian.Uint32(resp.Body[0:4]) == 0 {
		return
	}
	rec := resp.Body[4:]
	if len(rec) < specRecordMinLength {
		return
	}
	if recName := wire.DecodeUTF16LE(rec[0:64]); recName != "" && recName != name {
		return
	}
	contractSize := f64At(rec, specRecordContractSizeOffset)
	point := f64At(rec, specRecordPointOffset)
	c.stateMu.Lock()
	if s, ok := c.symbols[name]; ok {
		if contractSize > 0 {
			s.ContractSize = contractSize
		}
		if point > 0 {
			s.Point = point
		}
	}
	c.stateMu.Unlock()
}

// profitFromQuote computes a position's floating profit from a
// quote: a long position closes at the bid, a short at the ask,
// and the result is denominated in the symbol's profit currency.
func profitFromQuote(p Position, quote Quote, contractSize float64) (float64, bool) {
	if contractSize <= 0 {
		return 0, false
	}
	var diff float64
	switch p.Type {
	case TypeBuy:
		if quote.Bid <= 0 {
			return 0, false
		}
		diff = quote.Bid - p.Price
	case TypeSell:
		if quote.Ask <= 0 {
			return 0, false
		}
		diff = p.Price - quote.Ask
	default:
		return 0, false
	}
	return diff * p.Volume * contractSize, true
}

// profitCurrency derives a symbol's profit currency from its name:
// the trailing currency code of pairs and metals (EURUSD -> USD,
// USDJPY -> JPY), tolerating the decorations brokers append
// (XAUUSDm, XAUUSD+, GOLD#). It returns "" when no known currency
// code can be identified, which keeps the server-reported profit.
var knownCurrencies = map[string]bool{
	"USD": true, "EUR": true, "GBP": true, "JPY": true,
	"CHF": true, "AUD": true, "CAD": true, "NZD": true,
	"CNY": true, "HKD": true, "SGD": true, "SEK": true,
	"NOK": true, "PLN": true, "ZAR": true, "MXN": true,
	"TRY": true, "CNH": true,
}

func profitCurrency(symbol string) string {
	s := symbol
	// Strip trailing non-letters (XAUUSD+, GOLD#).
	for len(s) > 0 {
		last := s[len(s)-1]
		if (last >= 'A' && last <= 'Z') || (last >= 'a' && last <= 'z') {
			break
		}
		s = s[:len(s)-1]
	}
	// GOLD is quoted in USD on every cluster this library targets.
	if strings.EqualFold(s, "GOLD") {
		return "USD"
	}
	// Strip a single trailing lowercase broker suffix when what
	// remains ends in a currency code (XAUUSDm -> XAUUSD).
	if len(s) > 0 && s[len(s)-1] >= 'a' && s[len(s)-1] <= 'z' {
		if tail := s[len(s)-1-3 : len(s)-1]; knownCurrencies[strings.ToUpper(tail)] {
			s = s[:len(s)-1]
		}
	}
	if len(s) >= 3 {
		if tail := strings.ToUpper(s[len(s)-3:]); knownCurrencies[tail] {
			return tail
		}
	}
	return ""
}
