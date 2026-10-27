package mtapi

import (
	"context"
	"crypto/rand"
	"encoding/binary"
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
	if price == 0 {
		if dirType == TypeBuy {
			price = q.Ask
		} else {
			price = q.Bid
		}
	}
	filling := uint32(wire.FillFOK)
	if action == wire.TradePending {
		filling = uint32(wire.FillReturn)
	}
	op := wire.OpParams{
		ActionID:       randU32(),
		TradeAction:    action,
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
	result, err := c.doTrade(ctx, op)
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
// position-list lookup (see PositionRef).
func (c *Client) ClosePosition(ctx context.Context, ticket int64, ref *PositionRef) (TradeResult, error) {
	return c.closePosition(ctx, ticket, 0, ref)
}

// PartialClose closes volume lots of the position, leaving the rest
// open.
func (c *Client) PartialClose(ctx context.Context, ticket int64, volume float64, ref *PositionRef) (TradeResult, error) {
	return c.closePosition(ctx, ticket, volume, ref)
}

func (c *Client) closePosition(ctx context.Context, ticket int64, closeVolume float64, ref *PositionRef) (TradeResult, error) {
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
	price := pos.Price
	if q, ok := c.Quote(pos.Symbol); ok {
		if pos.Type == TypeBuy {
			price = q.Bid
		} else {
			price = q.Ask
		}
	}
	opposite := uint32(wire.TypeSell)
	if pos.Type == TypeSell {
		opposite = wire.TypeBuy
	}
	op := wire.OpParams{
		ActionID:      randU32(),
		TradeAction:   wire.TradeMarket,
		Symbol:        pos.Symbol,
		Volume:        wire.LotsToVolume(volume),
		Digits:        uint32(sym.Digits),
		TradeType:     opposite,
		TypeFilling:   wire.FillFOK,
		TypeTime:      wire.TimeGTC,
		TypeFlags:     2,
		Price:         price,
		TradePosition: uint64(ticket),
	}
	return c.doTrade(ctx, op)
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
	return c.doTrade(ctx, op)
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
	return c.doTrade(ctx, op)
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

// resolvePosition finds a position by ticket: bypass ref first, then
// the live position list, then the trade-fed cache.
func (c *Client) resolvePosition(ctx context.Context, ticket int64, ref *PositionRef) (Position, error) {
	if ref != nil && ref.Symbol != "" && ref.Volume > 0 {
		return Position{Ticket: ticket, Symbol: ref.Symbol, Type: ref.Type, Volume: ref.Volume}, nil
	}
	if positions, err := c.Positions(ctx); err == nil {
		for _, p := range positions {
			if p.Ticket == ticket {
				return p, nil
			}
		}
	}
	c.stateMu.RLock()
	pos, ok := c.posCache[ticket]
	c.stateMu.RUnlock()
	if ok {
		return pos, nil
	}
	return Position{}, &TradeError{Retcode: 10030, Message: "position not found"}
}

// resolveOrder finds a pending order by ticket: bypass ref first,
// then the live order list, then the trade-fed cache.
func (c *Client) resolveOrder(ctx context.Context, ticket int64, ref *OrderRef) (Order, error) {
	if ref != nil && ref.Symbol != "" && ref.Volume > 0 {
		return Order{Ticket: ticket, Symbol: ref.Symbol, Type: ref.Type, Volume: ref.Volume, Price: ref.Price}, nil
	}
	if orders, err := c.Orders(ctx); err == nil {
		for _, o := range orders {
			if o.Ticket == ticket {
				return o, nil
			}
		}
	}
	c.stateMu.RLock()
	order, ok := c.orderCache[ticket]
	c.stateMu.RUnlock()
	if ok {
		return order, nil
	}
	return Order{}, &TradeError{Retcode: 10030, Message: "order not found"}
}

// doTrade serializes and sends one Op and waits for its result.
func (c *Client) doTrade(ctx context.Context, op wire.OpParams) (TradeResult, error) {
	return c.sendTrade(ctx, wire.PackOp(op))
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

	if err := c.sendEncrypted(wire.CmdTrade, op); err != nil {
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
