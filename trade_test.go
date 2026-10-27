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
	_, addr := startFakeMT5(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	client, err := mtapi.Dial(ctx, mtapi.Config{
		Login: 12345, Password: "pw123", Server: "Fake-MT5", Host: addr,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client, ctx
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
	res, err = client.ClosePosition(ctx, 900001, nil)
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
	_, err := client.ClosePosition(ctx, 424242, nil)
	var te *mtapi.TradeError
	if !errors.As(err, &te) || te.Retcode != 10030 {
		t.Fatalf("close unknown ticket err = %v", err)
	}
}
