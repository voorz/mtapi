package mtapi_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/voorz/mtapi"
)

func dialFake(t *testing.T) (*mtapi.Client, context.Context) {
	t.Helper()
	client, _, ctx := dialFakeConfigured(t, nil, mtapi.ExecutionAuto)
	return client, ctx
}

func dialFakeConfigured(t *testing.T, configure func(*fakeMT5), exec mtapi.ExecutionMode) (*mtapi.Client, *fakeMT5, context.Context) {
	t.Helper()
	fake, addr := startFakeMT5(t)
	if configure != nil {
		configure(fake)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	client, err := mtapi.Dial(ctx, mtapi.Config{
		Login: 12345, Password: "pw123", Server: "Fake-MT5", Host: addr,
		Execution: exec,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client, fake, ctx
}

func TestTradeRoundTrip(t *testing.T) {
	client, ctx := dialFake(t)

	if _, err := client.SubscribeQuote(ctx, "EURUSD"); err != nil {
		t.Fatalf("SubscribeQuote: %v", err)
	}

	// Market buy: result comes from the trade event.
	res, err := client.Buy(ctx, "EURUSD", 0.01, mtapi.TradeOptions{Comment: "test"})
	if err != nil {
		t.Fatalf("Buy: %v", err)
	}
	if res.Retcode != mtapi.RetcodeDone || res.Order != 900001 || res.Deal != 800001 || res.Price != 1.0853 {
		t.Fatalf("buy result = %+v", res)
	}

	positions, err := client.Positions(ctx)
	if err != nil || len(positions) != 1 {
		t.Fatalf("positions = %+v, %v", positions, err)
	}
	pos := positions[0]
	if pos.Ticket != 900001 || pos.Symbol != "EURUSD" || pos.Type != mtapi.TypeBuy || pos.Volume != 0.01 || pos.Price != 1.0853 {
		t.Fatalf("position = %+v", pos)
	}

	// Modify: the fake answers with the bare command-12 response
	// only, so this exercises the fallback path (~3s).
	sl := 1.0800
	res, err = client.ModifyPosition(ctx, 900001, &sl, nil, nil)
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("modify = %+v, %v", res, err)
	}

	// Close via the live position list.
	res, err = client.ClosePosition(ctx, 900001, nil, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("close = %+v, %v", res, err)
	}
	positions, err = client.Positions(ctx)
	if err != nil || len(positions) != 0 {
		t.Fatalf("positions after close = %+v, %v", positions, err)
	}

	// Pending order lifecycle.
	res, err = client.BuyLimit(ctx, "EURUSD", 0.01, 1.0800, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone || res.Order != 900002 {
		t.Fatalf("buy limit = %+v, %v", res, err)
	}
	orders, err := client.Orders(ctx)
	if err != nil || len(orders) != 1 {
		t.Fatalf("orders = %+v, %v", orders, err)
	}
	ord := orders[0]
	if ord.Ticket != 900002 || ord.Type != mtapi.TypeBuyLimit || ord.Volume != 0.01 || ord.Price != 1.08 {
		t.Fatalf("order = %+v", ord)
	}
	res, err = client.CancelOrder(ctx, 900002, nil)
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("cancel = %+v, %v", res, err)
	}
	orders, err = client.Orders(ctx)
	if err != nil || len(orders) != 0 {
		t.Fatalf("orders after cancel = %+v, %v", orders, err)
	}
}

// INSTANT is the ExecutionAuto default: on an instant-execution
// server the first attempt already succeeds, and the close reuses
// the learned mode. Wire values: INSTANT=2, MARKET=3.
func TestInstantIsDefault(t *testing.T) {
	client, fake, ctx := dialFakeConfigured(t, func(f *fakeMT5) { f.instantOnly = true }, mtapi.ExecutionAuto)

	if _, err := client.SubscribeQuote(ctx, "EURUSD"); err != nil {
		t.Fatalf("SubscribeQuote: %v", err)
	}
	res, err := client.Buy(ctx, "EURUSD", 0.01, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone || res.Order != 900001 {
		t.Fatalf("buy on instant server = %+v, %v", res, err)
	}
	res, err = client.ClosePosition(ctx, 900001, nil, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("close on instant server = %+v, %v", res, err)
	}

	fake.mu.Lock()
	actions := append([]uint32(nil), fake.tradeActions...)
	fake.mu.Unlock()
	want := []uint32{2, 2}
	if len(actions) != len(want) {
		t.Fatalf("trade actions = %v, want %v", actions, want)
	}
	for i := range want {
		if actions[i] != want[i] {
			t.Fatalf("trade actions = %v, want %v", actions, want)
		}
	}
}

// On a market-execution server the default INSTANT probe is
// rejected with 10013 (nothing executes); ExecutionAuto falls
// back to MARKET, learns the symbol's mode, and uses it directly
// from then on — for closes too.
func TestMarketFallback(t *testing.T) {
	client, fake, ctx := dialFakeConfigured(t, func(f *fakeMT5) { f.marketOnly = true }, mtapi.ExecutionAuto)

	if _, err := client.SubscribeQuote(ctx, "EURUSD"); err != nil {
		t.Fatalf("SubscribeQuote: %v", err)
	}
	res, err := client.Buy(ctx, "EURUSD", 0.01, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone || res.Order != 900001 {
		t.Fatalf("buy on market server = %+v, %v", res, err)
	}
	res, err = client.ClosePosition(ctx, 900001, nil, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("close on market server = %+v, %v", res, err)
	}

	fake.mu.Lock()
	actions := append([]uint32(nil), fake.tradeActions...)
	fake.mu.Unlock()
	// Expect: INSTANT (rejected) -> MARKET (buy) -> MARKET (close,
	// learned).
	want := []uint32{2, 3, 3}
	if len(actions) != len(want) {
		t.Fatalf("trade actions = %v, want %v", actions, want)
	}
	for i := range want {
		if actions[i] != want[i] {
			t.Fatalf("trade actions = %v, want %v", actions, want)
		}
	}
}

// With an explicit ExecutionMarket there is no fallback: the 10013
// is reported to the caller.
func TestExplicitMarketNoFallback(t *testing.T) {
	client, _, ctx := dialFakeConfigured(t, func(f *fakeMT5) { f.instantOnly = true }, mtapi.ExecutionMarket)

	if _, err := client.SubscribeQuote(ctx, "EURUSD"); err != nil {
		t.Fatalf("SubscribeQuote: %v", err)
	}
	res, err := client.Buy(ctx, "EURUSD", 0.01, mtapi.TradeOptions{})
	if err != nil {
		t.Fatalf("Buy: %v", err)
	}
	if res.Retcode != mtapi.RetcodeInvalid {
		t.Fatalf("buy retcode = %d, want %d", res.Retcode, mtapi.RetcodeInvalid)
	}
}

// A requote (10004) executes nothing; the close must be resubmitted
// with a fresh quote until it fills.
func TestCloseRequoteRetry(t *testing.T) {
	client, _, ctx := dialFakeConfigured(t, func(f *fakeMT5) { f.requoteCloses = 2 }, mtapi.ExecutionAuto)

	if _, err := client.SubscribeQuote(ctx, "EURUSD"); err != nil {
		t.Fatalf("SubscribeQuote: %v", err)
	}
	res, err := client.Buy(ctx, "EURUSD", 0.01, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("buy = %+v, %v", res, err)
	}
	res, err = client.ClosePosition(ctx, 900001, nil, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("close after requotes = %+v, %v", res, err)
	}
	positions, err := client.Positions(ctx)
	if err != nil || len(positions) != 0 {
		t.Fatalf("positions after close = %+v, %v", positions, err)
	}
}

// The position list lags a partial close (it still reports the
// pre-trade volume). A following full close must resolve the
// remaining volume from the trade-fed cache rather than the stale
// list — the real server rejects the overshoot with 10014
// (live-verified).
func TestPartialThenCloseWithLaggingList(t *testing.T) {
	client, _, ctx := dialFakeConfigured(t, nil, mtapi.ExecutionAuto)

	if _, err := client.SubscribeQuote(ctx, "EURUSD"); err != nil {
		t.Fatalf("SubscribeQuote: %v", err)
	}
	res, err := client.Buy(ctx, "EURUSD", 0.02, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("buy = %+v, %v", res, err)
	}
	res, err = client.PartialClose(ctx, 900001, 0.01, nil, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("partial = %+v, %v", res, err)
	}
	res, err = client.ClosePosition(ctx, 900001, nil, mtapi.TradeOptions{})
	if err != nil || res.Retcode != mtapi.RetcodeDone {
		t.Fatalf("close after partial = %+v, %v", res, err)
	}
	positions, err := client.Positions(ctx)
	if err != nil || len(positions) != 0 {
		t.Fatalf("positions after close = %+v, %v", positions, err)
	}
}

func TestTradeErrors(t *testing.T) {
	client, ctx := dialFake(t)

	// No quote yet: market order must fail with ErrNoQuote.
	if _, err := client.Buy(ctx, "EURUSD", 0.01, mtapi.TradeOptions{}); !errors.Is(err, mtapi.ErrNoQuote) {
		t.Fatalf("buy without quote err = %v", err)
	}
	// Unknown symbol.
	if _, err := client.Buy(ctx, "XXXXXX", 0.01, mtapi.TradeOptions{}); !errors.Is(err, mtapi.ErrSymbolNotFound) {
		t.Fatalf("buy unknown symbol err = %v", err)
	}
	// Unknown ticket: TradeError with retcode 10030.
	_, err := client.ClosePosition(ctx, 424242, nil, mtapi.TradeOptions{})
	var te *mtapi.TradeError
	if !errors.As(err, &te) || te.Retcode != 10030 {
		t.Fatalf("close unknown ticket err = %v", err)
	}
}
