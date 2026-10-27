package mtapi

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/voorz/mtapi/internal/wire"
)

// Symbols fetches the full symbol list (command 34, compressed) and
// replaces the cached list. It returns a copy of the cache keyed by
// symbol name.
func (c *Client) Symbols(ctx context.Context) (map[string]Symbol, error) {
	resp, err := c.sendAndWait(ctx, wire.CmdSymbols, nil, wire.CmdSymbols)
	if err != nil {
		return nil, err
	}
	symbols, err := parseSymbols(resp.Body)
	if err != nil {
		return nil, err
	}
	c.stateMu.Lock()
	c.symbols = make(map[string]*Symbol, len(symbols))
	c.symbolByID = make(map[int64]string, len(symbols))
	out := make(map[string]Symbol, len(symbols))
	for name, sym := range symbols {
		s := sym
		c.symbols[name] = &s
		c.symbolByID[s.ID] = name
		out[name] = s
	}
	c.stateMu.Unlock()
	return out, nil
}

// Symbol returns the cached description of one symbol.
func (c *Client) Symbol(name string) (Symbol, bool) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	s, ok := c.symbols[name]
	if !ok {
		return Symbol{}, false
	}
	return *s, true
}

// parseSymbols decompresses and parses the command-34 body: a 4-byte
// header, a zlib stream holding a uint32 count followed by that many
// "Mh" records.
func parseSymbols(body []byte) (map[string]Symbol, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("mtapi: symbols response too short: %d bytes", len(body))
	}
	raw, err := inflate(body[4:])
	if err != nil {
		return nil, fmt.Errorf("mtapi: decompress symbols: %w", err)
	}
	if len(raw) < 4 {
		return nil, fmt.Errorf("mtapi: symbol list too short: %d bytes", len(raw))
	}
	count := binary.LittleEndian.Uint32(raw[0:4])
	off := 4
	size := wire.SymbolSchema.Size()
	out := make(map[string]Symbol, count)
	for i := uint32(0); i < count; i++ {
		if off+size > len(raw) {
			break
		}
		vals, next := wire.ParseSeries(raw, wire.SymbolSchema, off)
		off = next
		name := vals.Str(0)
		if name == "" {
			continue
		}
		out[name] = Symbol{
			Name:     name,
			ID:       vals.Int(3),
			Digits:   int(vals.Int(2)),
			CalcMode: int(vals.Int(5)),
		}
	}
	return out, nil
}

// inflate decompresses a zlib stream, falling back to raw DEFLATE as
// the reference does.
func inflate(data []byte) ([]byte, error) {
	if zr, err := zlib.NewReader(bytes.NewReader(data)); err == nil {
		defer zr.Close()
		if out, err := io.ReadAll(zr); err == nil {
			return out, nil
		}
	}
	fr := flate.NewReader(bytes.NewReader(data))
	defer fr.Close()
	return io.ReadAll(fr)
}

// Subscribe sends a quote subscription (command 7) for the given
// symbols. It does not wait for quotes; use Quote to read them as
// they arrive, or SubscribeQuote to wait for one symbol's first
// quote. Unknown symbol names are skipped, as in the reference.
func (c *Client) Subscribe(ctx context.Context, symbols ...string) error {
	c.stateMu.RLock()
	ids := make([]uint32, 0, len(symbols))
	names := make([]string, 0, len(symbols))
	for _, name := range symbols {
		if s, ok := c.symbols[name]; ok {
			ids = append(ids, uint32(s.ID))
			names = append(names, name)
		}
	}
	c.stateMu.RUnlock()
	if len(ids) == 0 {
		return nil
	}
	payload := make([]byte, 4+4*len(ids))
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(ids)))
	for i, id := range ids {
		binary.LittleEndian.PutUint32(payload[4+4*i:8+4*i], id)
	}
	if err := c.sendEncrypted(ctx, wire.CmdSubscribe, payload); err != nil {
		return err
	}
	// Remember the names so a reconnect can restore the
	// subscriptions on the new session (see restoreSession).
	c.stateMu.Lock()
	for _, name := range names {
		c.subscribed[name] = true
	}
	c.stateMu.Unlock()
	return nil
}

// SubscribeQuote subscribes to a symbol and waits for its first
// quote. The wait is bounded by the context and, failing that, the
// configured request timeout.
func (c *Client) SubscribeQuote(ctx context.Context, symbol string) (Quote, error) {
	if _, ok := c.Symbol(symbol); !ok {
		return Quote{}, fmt.Errorf("%w: %s", ErrSymbolNotFound, symbol)
	}
	if err := c.Subscribe(ctx, symbol); err != nil {
		return Quote{}, err
	}
	return c.waitQuote(ctx, symbol)
}

// SubscribeQuotes subscribes to several symbols in one command and
// waits until quotes for all of them have arrived (or the wait
// expires, in which case the quotes received so far are returned).
func (c *Client) SubscribeQuotes(ctx context.Context, symbols []string) (map[string]Quote, error) {
	if err := c.Subscribe(ctx, symbols...); err != nil {
		return nil, err
	}
	deadline, cancel := c.quoteWaitContext(ctx)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		got := make(map[string]Quote, len(symbols))
		for _, name := range symbols {
			if q, ok := c.Quote(name); ok {
				got[name] = q
			}
		}
		if len(got) == len(symbols) {
			return got, nil
		}
		select {
		case <-deadline.Done():
			return got, nil
		case <-tick.C:
		}
	}
}

// Quote returns the latest cached quote for a symbol.
func (c *Client) Quote(symbol string) (Quote, bool) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	q, ok := c.quotes[symbol]
	return q, ok
}

// waitQuote polls the quote cache until a quote for symbol arrives.
func (c *Client) waitQuote(ctx context.Context, symbol string) (Quote, error) {
	deadline, cancel := c.quoteWaitContext(ctx)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if q, ok := c.Quote(symbol); ok {
			return q, nil
		}
		select {
		case <-deadline.Done():
			if err := ctx.Err(); err != nil {
				return Quote{}, err
			}
			return Quote{}, fmt.Errorf("%w: no quote received for %s", ErrTimeout, symbol)
		case <-tick.C:
		}
	}
}

// quoteWaitContext derives the wait deadline: the caller's context if
// it has one, otherwise the configured request timeout.
func (c *Client) quoteWaitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.cfg.Timeout)
}

// handleQuote processes a command-8 push: a stream of 50-byte quote
// records. Prices on the wire are integer-valued floats scaled by
// 10^digits.
func (c *Client) handleQuote(body []byte) {
	size := wire.QuoteSchema.Size()
	for off := 0; off+size <= len(body); off += size {
		vals, _ := wire.ParseSeries(body, wire.QuoteSchema, off)
		if vals[3] == nil || vals[4] == nil {
			continue
		}
		symID := vals.Int(0)
		rawBid := int64(vals.Float(3))
		rawAsk := int64(vals.Float(4))

		c.stateMu.Lock()
		name, ok := c.symbolByID[symID]
		if !ok {
			c.stateMu.Unlock()
			continue
		}
		sym := c.symbols[name]
		scale := math.Pow10(sym.Digits)
		q := Quote{
			Symbol:   name,
			SymbolID: symID,
			Bid:      float64(rawBid) / scale,
			Ask:      float64(rawAsk) / scale,
			RawBid:   rawBid,
			RawAsk:   rawAsk,
			Time:     time.Now().UTC(),
		}
		c.quotes[name] = q
		cb := c.onQuote
		c.stateMu.Unlock()

		if cb != nil {
			callSafely(func() { cb(q) })
		}
	}
}

// handleSpec processes a command-17 push: the 1696-byte full symbol
// specification. Decoded fields are merged into the cached symbol.
func (c *Client) handleSpec(body []byte) {
	if len(body) < 1696 {
		return
	}
	spec := SymbolSpec{
		Name:         wire.DecodeUTF16LE(body[0:64]),
		TickValue:    f64At(body, 1376),
		TickSize:     f64At(body, 1384),
		ContractSize: f64At(body, 1392),
		Digits:       int(i32At(body, 1168)),
		Point:        f64At(body, 1172),
		Spread:       int(i32At(body, 1368)),
		CalcMode:     int(i32At(body, 1404)),
		Raw:          append([]byte(nil), body...),
	}

	c.stateMu.Lock()
	if sym, ok := c.symbols[spec.Name]; ok {
		sym.TickValue = spec.TickValue
		sym.TickSize = spec.TickSize
		sym.ContractSize = spec.ContractSize
		sym.Point = spec.Point
		sym.Spread = spec.Spread
	}
	symID := int64(binary.LittleEndian.Uint32(body[0:4]))
	c.specData[symID] = spec.Raw
	cb := c.onSpec
	c.stateMu.Unlock()

	if cb != nil {
		callSafely(func() { cb(spec) })
	}
}

// Candles fetches up to count recent candles (command 11) for a
// symbol and timeframe. The request window is sized from count using
// the reference's per-timeframe estimate.
func (c *Client) Candles(ctx context.Context, symbol string, tf Timeframe, count int) ([]Candle, error) {
	if count <= 0 {
		count = 100
	}
	now := time.Now().Unix()
	secPer := int64(wire.EstimateSecondsPerCandle(int(tf)))
	from := now - int64(count+10)*secPer

	payload := make([]byte, 74)
	name := wire.EncodeUTF16LE(symbol)
	copy(payload[0:64], name)
	binary.LittleEndian.PutUint16(payload[64:66], uint16(tf))
	binary.LittleEndian.PutUint32(payload[66:70], uint32(int32(from)))
	binary.LittleEndian.PutUint32(payload[70:74], uint32(int32(now)))

	resp, err := c.sendAndWait(ctx, wire.CmdRates, payload, wire.CmdRates)
	if err != nil {
		return nil, err
	}
	raw := wire.ParseCandles(resp.Body)
	out := make([]Candle, 0, len(raw))
	for _, rc := range raw {
		out = append(out, Candle{
			Time:       timeFromUnix(int64(rc.Timestamp)),
			Timestamp:  int64(rc.Timestamp),
			Open:       rc.Open,
			High:       rc.High,
			Low:        rc.Low,
			Close:      rc.Close,
			TickVolume: rc.TickVolume,
			Spread:     int(rc.Spread),
		})
	}
	return out, nil
}

func f64At(b []byte, off int) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(b[off : off+8]))
}

func i32At(b []byte, off int) int32 {
	return int32(binary.LittleEndian.Uint32(b[off : off+4]))
}
