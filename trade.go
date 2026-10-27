package mtapi

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/voorz/mtapi/internal/wire"
)

// defaultDeviation is the price deviation (in points) applied to
// market and pending orders when TradeOptions leaves it zero, as in
// the reference.
const defaultDeviation = 30

// tradeTimeout bounds one trade operation's wait for a result.
const tradeTimeout = 15 * time.Second

// tradeFallbackDelay is how long sendTrade waits for a detailed
// trade event before falling back to the bare command-12 retcode.
const tradeFallbackDelay = 3 * time.Second

// TradeOptions carries the optional parameters of an order placement.
type TradeOptions struct {
	SL      float64 // stop-loss price, 0 = none
	TP      float64 // take-profit price, 0 = none
	Comment string
	// Deviation is the maximum price deviation in points. Zero means
	// the default of 30.
	Deviation uint32
}

func (o TradeOptions) deviation() uint32 {
	if o.Deviation == 0 {
		return defaultDeviation
	}
	return o.Deviation
}

// ExecutionMode selects the protocol trade action used for market
// orders (open, close, partial close). MT5 symbols execute in
// market mode or in instant mode, and the protocol encodes that
// choice in the order itself (trade action MARKET vs INSTANT); the
// wrong one is rejected with retcode 10013. The reference
// implementation hardcodes MARKET and therefore cannot trade on
// instant-execution accounts at all (verified live: an Exness Pro
// account requires INSTANT for opens and closes).
type ExecutionMode int

const (
	// ExecutionAuto probes with the MARKET action and, when the
	// server rejects it as invalid (10013 — a pure validation
	// rejection that executes nothing), retries with INSTANT and
	// remembers the symbol's mode for the rest of the session.
	ExecutionAuto ExecutionMode = iota
	// ExecutionMarket always uses the MARKET action.
	ExecutionMarket
	// ExecutionInstant always uses the INSTANT action.
	ExecutionInstant
)

// maxRequoteRetries bounds how often a market operation is
// resubmitted with a fresh quote after a requote (10004).
const maxRequoteRetries = 3

// PositionRef supplies a position's identity fields directly,
// bypassing the position-list lookup. Close, partial close and
// modify operations accept one because the protocol's position list
// can be stale right after a trade (the reference implements the same
// bypass). All fields must be set for the bypass to engage.
type PositionRef struct {
	Symbol string
	Type   int // TypeBuy or TypeSell
	Volume float64
}

// OrderRef is the bypass counterpart for pending orders.
type OrderRef struct {
	Symbol string
	Type   int
	Volume float64
	Price  float64
}

// Buy opens a market buy position. The symbol must be subscribed
// first (Subscribe), as market orders are priced from the live quote.
func (c *Client) Buy(ctx context.Context, symbol string, volume float64, opts TradeOptions) (TradeResult, error) {
	return c.placeOrder(ctx, wire.TradeMarket, TypeBuy, wire.TypeBuy, symbol, volume, 0, 0, opts)
}

// Sell opens a market sell position.
func (c *Client) Sell(ctx context.Context, symbol string, volume float64, opts TradeOptions) (TradeResult, error) {
	return c.placeOrder(ctx, wire.TradeMarket, TypeSell, wire.TypeSell, symbol, volume, 0, 0, opts)
}

// BuyLimit places a buy limit pending order at price.
func (c *Client) BuyLimit(ctx context.Context, symbol string, volume, price float64, opts TradeOptions) (TradeResult, error) {
	return c.placeOrder(ctx, wire.TradePending, TypeBuy, wire.TypeBuyLimit, symbol, volume, price, 0, opts)
}

// SellLimit places a sell limit pending order at price.
func (c *Client) SellLimit(ctx context.Context, symbol string, volume, price float64, opts TradeOptions) (TradeResult, error) {
	return c.placeOrder(ctx, wire.TradePending, TypeSell, wire.TypeSellLimit, symbol, volume, price, 0, opts)
}

// BuyStop places a buy stop pending order at price.
func (c *Client) BuyStop(ctx context.Context, symbol string, volume, price float64, opts TradeOptions) (TradeResult, error) {
	return c.placeOrder(ctx, wire.TradePending, TypeBuy, wire.TypeBuyStop, symbol, volume, price, 0, opts)
}

// SellStop places a sell stop pending order at price.
func (c *Client) SellStop(ctx context.Context, symbol string, volume, price float64, opts TradeOptions) (TradeResult, error) {
	return c.placeOrder(ctx, wire.TradePending, TypeSell, wire.TypeSellStop, symbol, volume, price, 0, opts)
}

// BuyStopLimit places a buy stop-limit order: when the price reaches
// stopPrice, a limit order at limitPrice is placed. Protocol support
// for stop-limit orders is verified by the reference documentation,
// though the reference client exposes no method for it.
func (c *Client) BuyStopLimit(ctx context.Context, symbol string, volume, stopPrice, limitPrice float64, opts TradeOptions) (TradeResult, error) {
	return c.placeOrder(ctx, wire.TradePending, TypeBuy, wire.TypeBuyStopLimit, symbol, volume, limitPrice, stopPrice, opts)
}

// SellStopLimit places a sell stop-limit order.
func (c *Client) SellStopLimit(ctx context.Context, symbol string, volume, stopPrice, limitPrice float64, opts TradeOptions) (TradeResult, error) {
	return c.placeOrder(ctx, wire.TradePending, TypeSell, wire.TypeSellStopLimit, symbol, volume, limitPrice, stopPrice, opts)
}

// placeOrder implements the quote-priced order placements (market and
// pending). dirType is the order's direction (TypeBuy/TypeSell),
// opType is the trade_type written to the Op record.
func (c *Client) placeOrder(ctx context.Context, action uint32, dirType int, opType uint32, symbol string, volume, price, trigger float64, opts TradeOptions) (TradeResult, error) {
	sym, ok := c.Symbol(symbol)
	if !ok {
		return TradeResult{}, fmt.Errorf("%w: %s", ErrSymbolNotFound, symbol)
	}
	q, ok := c.Quote(symbol)
	if !ok {
		return TradeResult{}, fmt.Errorf("%w: %s", ErrNoQuote, symbol)
	}
	filling := uint32(wire.FillFOK)
	if action == wire.TradePending {
		filling = uint32(wire.FillReturn)
	}
	build := func(tradeAction uint32, price float64) wire.OpParams {
		return wire.OpParams{
			ActionID:       randU32(),
			TradeAction:    tradeAction,
			Symbol:         symbol,
			Volume:         wire.LotsToVolume(volume),
			Digits:         uint32(sym.Digits),
			TradeType:      opType,
			TypeFilling:    filling,
			TypeTime:       wire.TimeGTC,
			TypeFlags:      2,
			Price:          price,
			PriceTrigger:   trigger,
			SL:             opts.SL,
			TP:             opts.TP,
			PriceDeviation: opts.deviation(),
			Comment:        opts.Comment,
		}
	}

	var result TradeResult
	var err error
	if action == wire.TradePending {
		if price == 0 {
			price = dirPrice(q, dirType)
		}
		result, err = c.doTrade(ctx, build(action, price))
	} else {
		priceFor := func() (float64, bool) {
			qq, ok := c.Quote(symbol)
			if !ok {
				return 0, false
			}
			return dirPrice(qq, dirType), true
		}
		result, err = c.runMarketTrade(ctx, symbol, build, priceFor)
		if p, ok := priceFor(); ok {
			price = p // for the position/order cache below
		}
	}
	if err != nil {
		return TradeResult{}, err
	}

	// Cache the outcome so follow-up operations can find the position
	// or order even while the command-4 list is still stale.
	if result.Retcode == RetcodeDone && result.Order != 0 {
		c.stateMu.Lock()
		switch action {
		case wire.TradeMarket:
			vol := result.Volume
			if result.RawVolume == 0 {
				vol = volume
			}
			prc := result.Price
			if prc == 0 {
				prc = price
			}
			c.posCache[result.Order] = Position{
				Ticket: result.Order, Symbol: symbol, Type: dirType,
				Volume: vol, Price: prc,
			}
		case wire.TradePending:
			c.orderCache[result.Order] = Order{
				Ticket: result.Order, Symbol: symbol, Type: int(opType),
				Volume: volume, Price: price,
			}
		}
		c.stateMu.Unlock()
	}
	return result, nil
}

// ClosePosition closes the position with the given (order) ticket.
// ref may supply the position's fields directly to bypass the
// position-list lookup (see PositionRef). Of opts, only Deviation
// is used.
func (c *Client) ClosePosition(ctx context.Context, ticket int64, ref *PositionRef, opts TradeOptions) (TradeResult, error) {
	return c.closePosition(ctx, ticket, 0, ref, opts)
}

// PartialClose closes volume lots of the position, leaving the rest
// open. Of opts, only Deviation is used.
func (c *Client) PartialClose(ctx context.Context, ticket int64, volume float64, ref *PositionRef, opts TradeOptions) (TradeResult, error) {
	return c.closePosition(ctx, ticket, volume, ref, opts)
}

func (c *Client) closePosition(ctx context.Context, ticket int64, closeVolume float64, ref *PositionRef, opts TradeOptions) (TradeResult, error) {
	pos, err := c.resolvePosition(ctx, ticket, ref)
	if err != nil {
		return TradeResult{}, err
	}
	sym, ok := c.Symbol(pos.Symbol)
	if !ok {
		return TradeResult{}, fmt.Errorf("%w: %s", ErrSymbolNotFound, pos.Symbol)
	}
	volume := pos.Volume
	if closeVolume > 0 {
		volume = closeVolume
	}
	opposite := uint32(wire.TypeSell)
	if pos.Type == TypeSell {
		opposite = wire.TypeBuy
	}
	build := func(tradeAction uint32, price float64) wire.OpParams {
		return wire.OpParams{
			ActionID:       randU32(),
			TradeAction:    tradeAction,
			Symbol:         pos.Symbol,
			Volume:         wire.LotsToVolume(volume),
			Digits:         uint32(sym.Digits),
			TradeType:      opposite,
			TypeFilling:    wire.FillFOK,
			TypeTime:       wire.TimeGTC,
			TypeFlags:      2,
			Price:          price,
			PriceDeviation: opts.deviation(),
			TradePosition:  uint64(ticket),
		}
	}
	priceFor := func() (float64, bool) {
		if q, ok := c.Quote(pos.Symbol); ok {
			if pos.Type == TypeBuy {
				return q.Bid, true
			}
			return q.Ask, true
		}
		// No live quote (symbol not subscribed): fall back to the
		// position's own price, as the reference does.
		return pos.Price, pos.Price != 0
	}
	res, err := c.runMarketTrade(ctx, pos.Symbol, build, priceFor)
	if err == nil && (res.Retcode == RetcodeDone || res.Retcode == RetcodeDonePartial) {
		// Keep the trade-fed cache in step with what just executed:
		// the server list can still report the pre-trade volume, and
		// resolving a follow-up close from it overshoots the
		// remaining volume (live-verified: retcode 10014).
		c.stateMu.Lock()
		if cached, ok := c.posCache[ticket]; ok {
			closed := volume
			if res.RawVolume > 0 {
				closed = res.Volume
			}
			if closeVolume > 0 && closed < cached.Volume {
				cached.Volume -= closed
				c.posCache[ticket] = cached
			} else {
				delete(c.posCache, ticket)
			}
		}
		c.stateMu.Unlock()
	}
	return res, err
}

// dirPrice returns the execution price for a direction: buys pay
// the ask, sells receive the bid.
func dirPrice(q Quote, dirType int) float64 {
	if dirType == TypeBuy {
		return q.Ask
	}
	return q.Bid
}

// ModifyPosition changes a position's stop-loss and/or take-profit. A
// nil sl or tp keeps the position's current value.
func (c *Client) ModifyPosition(ctx context.Context, ticket int64, sl, tp *float64, ref *PositionRef) (TradeResult, error) {
	pos, err := c.resolvePosition(ctx, ticket, ref)
	if err != nil {
		return TradeResult{}, err
	}
	sym, ok := c.Symbol(pos.Symbol)
	if !ok {
		return TradeResult{}, fmt.Errorf("%w: %s", ErrSymbolNotFound, pos.Symbol)
	}
	newSL, newTP := pos.SL, pos.TP
	if sl != nil {
		newSL = *sl
	}
	if tp != nil {
		newTP = *tp
	}
	op := wire.OpParams{
		ActionID:      randU32(),
		TradeAction:   wire.TradeModify,
		Symbol:        pos.Symbol,
		Volume:        wire.LotsToVolume(pos.Volume),
		Digits:        uint32(sym.Digits),
		TradeOrder:    uint64(ticket),
		TradeType:     uint32(pos.Type),
		TypeFilling:   wire.FillReturn,
		TypeTime:      wire.TimeGTC,
		TypeFlags:     2,
		SL:            newSL,
		TP:            newTP,
		TradePosition: uint64(ticket),
	}
	res, err := c.doTrade(ctx, op)
	if err == nil && res.Retcode == RetcodeDone {
		c.stateMu.Lock()
		if cached, ok := c.posCache[ticket]; ok {
			cached.SL, cached.TP = newSL, newTP
			c.posCache[ticket] = cached
		}
		c.stateMu.Unlock()
	}
	return res, err
}

// CancelOrder cancels a pending order. ref may supply the order's
// fields directly to bypass the order-list lookup (see OrderRef).
func (c *Client) CancelOrder(ctx context.Context, ticket int64, ref *OrderRef) (TradeResult, error) {
	order, err := c.resolveOrder(ctx, ticket, ref)
	if err != nil {
		return TradeResult{}, err
	}
	sym, ok := c.Symbol(order.Symbol)
	if !ok {
		return TradeResult{}, fmt.Errorf("%w: %s", ErrSymbolNotFound, order.Symbol)
	}
	op := wire.OpParams{
		ActionID:    randU32(),
		TradeAction: wire.TradeCancel,
		Symbol:      order.Symbol,
		Volume:      wire.LotsToVolume(order.Volume),
		Digits:      uint32(sym.Digits),
		TradeOrder:  uint64(ticket),
		TradeType:   uint32(order.Type),
		TypeFilling: wire.FillFOK,
		TypeTime:    wire.TimeGTC,
		TypeFlags:   2,
		Price:       order.Price,
	}
	res, err := c.doTrade(ctx, op)
	if err == nil && res.Retcode == RetcodeDone {
		c.stateMu.Lock()
		delete(c.orderCache, ticket)
		c.stateMu.Unlock()
	}
	return res, err
}

// ModifyOrder changes a pending order's price, stop-loss and/or
// take-profit. A nil argument keeps the order's current value (note
// the protocol's order record reports only price, so kept SL/TP are
// the zero values the order was parsed with, exactly as in the
// reference).
func (c *Client) ModifyOrder(ctx context.Context, ticket int64, price, sl, tp *float64) (TradeResult, error) {
	orders, err := c.Orders(ctx)
	if err != nil {
		return TradeResult{}, err
	}
	var order *Order
	for i := range orders {
		if orders[i].Ticket == ticket {
			order = &orders[i]
			break
		}
	}
	if order == nil {
		return TradeResult{}, &TradeError{Retcode: 10030, Message: "order not found"}
	}
	sym, ok := c.Symbol(order.Symbol)
	if !ok {
		return TradeResult{}, fmt.Errorf("%w: %s", ErrSymbolNotFound, order.Symbol)
	}
	newPrice, newSL, newTP := order.Price, order.SL, order.TP
	if price != nil {
		newPrice = *price
	}
	if sl != nil {
		newSL = *sl
	}
	if tp != nil {
		newTP = *tp
	}
	op := wire.OpParams{
		ActionID:    randU32(),
		TradeAction: wire.TradeModifyOrder,
		Symbol:      order.Symbol,
		Volume:      wire.LotsToVolume(order.Volume),
		Digits:      uint32(sym.Digits),
		TradeOrder:  uint64(ticket),
		TradeType:   uint32(order.Type),
		TypeFilling: wire.FillReturn,
		TypeTime:    wire.TimeGTC,
		TypeFlags:   2,
		Price:       newPrice,
		SL:          newSL,
		TP:          newTP,
	}
	return c.doTrade(ctx, op)
}

// resolvePosition finds a position by ticket: bypass ref first,
// then the trade-fed cache, then the live position list. The cache
// precedes the list because the list can lag right after this
// client's own trades (it still shows the pre-trade volume), while
// the cache is updated from every trade result; the list remains
// the source for positions this client has not traded.
func (c *Client) resolvePosition(ctx context.Context, ticket int64, ref *PositionRef) (Position, error) {
	if ref != nil && ref.Symbol != "" && ref.Volume > 0 {
		return Position{Ticket: ticket, Symbol: ref.Symbol, Type: ref.Type, Volume: ref.Volume}, nil
	}
	c.stateMu.RLock()
	pos, ok := c.posCache[ticket]
	c.stateMu.RUnlock()
	if ok {
		return pos, nil
	}
	if positions, err := c.Positions(ctx); err == nil {
		for _, p := range positions {
			if p.Ticket == ticket {
				return p, nil
			}
		}
	}
	return Position{}, &TradeError{Retcode: 10030, Message: "position not found"}
}

// resolveOrder finds a pending order by ticket: bypass ref first,
// then the trade-fed cache, then the live order list (see
// resolvePosition for the ordering rationale).
func (c *Client) resolveOrder(ctx context.Context, ticket int64, ref *OrderRef) (Order, error) {
	if ref != nil && ref.Symbol != "" && ref.Volume > 0 {
		return Order{Ticket: ticket, Symbol: ref.Symbol, Type: ref.Type, Volume: ref.Volume, Price: ref.Price}, nil
	}
	c.stateMu.RLock()
	order, ok := c.orderCache[ticket]
	c.stateMu.RUnlock()
	if ok {
		return order, nil
	}
	if orders, err := c.Orders(ctx); err == nil {
		for _, o := range orders {
			if o.Ticket == ticket {
				return o, nil
			}
		}
	}
	return Order{}, &TradeError{Retcode: 10030, Message: "order not found"}
}

// doTrade serializes and sends one Op and waits for its result.
func (c *Client) doTrade(ctx context.Context, op wire.OpParams) (TradeResult, error) {
	return c.sendTrade(ctx, wire.PackOp(op))
}

// marketAction resolves the trade action for a market operation on
// symbol: the configured mode, or in ExecutionAuto the learned
// action (MARKET until an INSTANT requirement is learned).
func (c *Client) marketAction(symbol string) (action uint32, mayFallback bool) {
	switch c.cfg.Execution {
	case ExecutionMarket:
		return wire.TradeMarket, false
	case ExecutionInstant:
		return wire.TradeInstant, false
	}
	c.stateMu.RLock()
	learned, ok := c.execLearned[symbol]
	c.stateMu.RUnlock()
	if ok {
		return learned, false
	}
	return wire.TradeMarket, true
}

func (c *Client) learnExecAction(symbol string, action uint32) {
	c.stateMu.Lock()
	c.execLearned[symbol] = action
	c.stateMu.Unlock()
}

// runMarketTrade executes one market-family operation (open, close,
// partial close), handling the two behaviors live servers exhibit
// that the reference — validated only on a market-execution
// account — does not: instant-execution symbols reject the MARKET
// action with 10013 (in ExecutionAuto the INSTANT action is tried
// and learned), and instant execution requotes when the price moved
// (the operation is resubmitted with a fresh quote). build
// constructs the Op for a given trade action and price; priceFor
// reports the current execution price and is re-read before every
// attempt.
func (c *Client) runMarketTrade(ctx context.Context, symbol string, build func(action uint32, price float64) wire.OpParams, priceFor func() (float64, bool)) (TradeResult, error) {
	action, mayFallback := c.marketAction(symbol)
	res, err := c.tradeWithRequoteRetry(ctx, symbol, action, build, priceFor)
	if err != nil || !mayFallback || res.Retcode != RetcodeInvalid {
		return res, err
	}
	res, err = c.tradeWithRequoteRetry(ctx, symbol, wire.TradeInstant, build, priceFor)
	if err == nil && res.Retcode != RetcodeInvalid {
		c.learnExecAction(symbol, wire.TradeInstant)
	}
	return res, err
}

// tradeWithRequoteRetry submits one market operation with a fixed
// trade action, resubmitting with a refreshed quote after requotes
// (retcode 10004: nothing executed, the price simply moved).
func (c *Client) tradeWithRequoteRetry(ctx context.Context, symbol string, action uint32, build func(action uint32, price float64) wire.OpParams, priceFor func() (float64, bool)) (TradeResult, error) {
	var usedQuoteTime time.Time
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			// Wait briefly for a quote newer than the one the
			// failed attempt used, so the retry does not resend
			// the same stale price.
			deadline := time.Now().Add(800 * time.Millisecond)
			for time.Now().Before(deadline) {
				if q, ok := c.Quote(symbol); ok && q.Time.After(usedQuoteTime) {
					break
				}
				select {
				case <-time.After(50 * time.Millisecond):
				case <-ctx.Done():
					return TradeResult{}, ctx.Err()
				case <-c.done:
					return TradeResult{}, ErrClosed
				}
			}
		}
		if q, ok := c.Quote(symbol); ok {
			usedQuoteTime = q.Time
		}
		price, ok := priceFor()
		if !ok {
			return TradeResult{}, fmt.Errorf("%w: %s", ErrNoQuote, symbol)
		}
		res, err := c.doTrade(ctx, build(action, price))
		if err != nil || res.Retcode != RetcodeRequote || attempt >= maxRequoteRetries {
			return res, err
		}
	}
}

// sendTrade waits on both result paths, mirroring the reference: the
// detailed trade event (command 19) is authoritative; the bare
// command-12 response (retcode only) is used as a fallback once
// tradeFallbackDelay has passed without an event. Intermediate
// events with retcode 10002 are not final results and are skipped.
func (c *Client) sendTrade(ctx context.Context, op []byte) (TradeResult, error) {
	c.tradeMu.Lock()
	defer c.tradeMu.Unlock()

	release, err := c.acquireSlot(ctx, wire.CmdTrade)
	if err != nil {
		return TradeResult{}, err
	}
	defer release()

	ch := make(chan *wire.Response, 1)
	c.pendingMu.Lock()
	c.pending[wire.CmdTrade] = ch
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, wire.CmdTrade)
		c.pendingMu.Unlock()
	}()

	// Drop events left over from earlier operations, as the
	// reference resets its event queue before each trade.
drain:
	for {
		select {
		case <-c.tradeEvents:
		default:
			break drain
		}
	}

	// Bind the operation to one session. Waiting for a session
	// (during a reconnect) before sending is fine; once sent, the
	// operation is never resubmitted — if the session drops
	// mid-flight the outcome is unknown and reported as such.
	sess, err := c.waitSession(ctx)
	if err != nil {
		return TradeResult{}, err
	}
	if err := c.sendEncryptedOn(sess, wire.CmdTrade, op); err != nil {
		return TradeResult{}, err
	}

	start := time.Now()
	deadline := time.NewTimer(tradeTimeout)
	defer deadline.Stop()
	fallback := time.NewTimer(tradeFallbackDelay)
	defer fallback.Stop()
	var cmd12Resp *wire.Response

	for {
		select {
		case ev := <-c.tradeEvents:
			if ev.Retcode != 10002 {
				return TradeResult{
					Retcode: ev.Retcode, Deal: ev.Deal, Order: ev.Order,
					Position: ev.Position, Volume: ev.Volume,
					RawVolume: ev.RawVolume, Price: ev.Price,
					Comment: ev.Comment,
				}, nil
			}
		case resp := <-ch:
			cmd12Resp = resp
			if time.Since(start) > tradeFallbackDelay {
				return tradeResultFromCmd12(resp), nil
			}
		case <-fallback.C:
			if cmd12Resp != nil {
				return tradeResultFromCmd12(cmd12Resp), nil
			}
		case <-deadline.C:
			return TradeResult{}, fmt.Errorf("%w: no trade response", ErrTimeout)
		case <-sess.dead:
			return TradeResult{}, connError(errors.New("connection dropped during trade; outcome unknown, reconcile via Positions/Deals"))
		case <-c.done:
			return TradeResult{}, ErrClosed
		case <-ctx.Done():
			return TradeResult{}, ctx.Err()
		}
	}
}

// tradeResultFromCmd12 builds a result from the bare command-12
// response, whose body starts with the retcode.
func tradeResultFromCmd12(resp *wire.Response) TradeResult {
	r := TradeResult{}
	if len(resp.Body) >= 4 {
		r.Retcode = int(binary.LittleEndian.Uint32(resp.Body[0:4]))
		r.Comment = fmt.Sprintf("retcode=%d", r.Retcode)
	}
	return r
}

// handleTradeEvent processes a command-19 push: a 380-byte record of
// [4-byte header][Op: 248][result block], with the result fields at
// fixed offsets in the trailing block.
func (c *Client) handleTradeEvent(body []byte) {
	apOff := 4 + wire.OpSize
	if len(body) < apOff+36 {
		return
	}
	rawVolume := int64(binary.LittleEndian.Uint64(body[apOff+20 : apOff+28]))
	ev := TradeEvent{
		Retcode:   int(binary.LittleEndian.Uint32(body[apOff : apOff+4])),
		Deal:      int64(binary.LittleEndian.Uint64(body[apOff+4 : apOff+12])),
		Order:     int64(binary.LittleEndian.Uint64(body[apOff+12 : apOff+20])),
		RawVolume: rawVolume,
		Volume:    wire.VolumeToLots(uint64(rawVolume)),
		Price:     math.Float64frombits(binary.LittleEndian.Uint64(body[apOff+28 : apOff+36])),
	}
	ev.Position = ev.Order
	if len(body) > apOff+64 {
		end := apOff + 128
		if end > len(body) {
			end = len(body)
		}
		ev.Comment = wire.DecodeUTF16LE(body[apOff+64 : end])
	}

	select {
	case c.tradeEvents <- ev:
	default:
		// Buffer full: the event is still delivered to the callback;
		// a waiting sendTrade will fall back to the command-12 path.
	}

	c.stateMu.RLock()
	cb := c.onTrade
	c.stateMu.RUnlock()
	if cb != nil {
		callSafely(func() { cb(ev) })
	}
}

func randU32() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b[:])
}
