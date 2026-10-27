// Command trade runs a full demo trade round trip: market buy,
// modify SL/TP, partial close, full close — printing every result.
//
// DEMO ACCOUNTS ONLY. The program refuses to run unless the
// MT5_DEMO_CONFIRM environment variable is set to "I understand",
// and it should only ever be pointed at a demo server.
//
// Configuration (environment): MT5_LOGIN, MT5_PASSWORD, MT5_SERVER,
// optional MT5_HOST — see examples/stats. The symbol is a
// command-line argument (default: EURUSD).
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/voorz/mtapi"
)

func env(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "missing required environment variable %s\n", key)
		os.Exit(2)
	}
	return v
}

func step(name string, res mtapi.TradeResult, err error) mtapi.TradeResult {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
	fmt.Printf("%s: retcode=%d order=%d deal=%d price=%v volume=%v\n",
		name, res.Retcode, res.Order, res.Deal, res.Price, res.Volume)
	if res.Retcode != mtapi.RetcodeDone {
		fmt.Fprintf(os.Stderr, "%s did not complete (retcode %d)\n", name, res.Retcode)
		os.Exit(1)
	}
	return res
}

func main() {
	if os.Getenv("MT5_DEMO_CONFIRM") != "I understand" {
		fmt.Fprintln(os.Stderr, "refusing to trade: set MT5_DEMO_CONFIRM='I understand' and use a DEMO account")
		os.Exit(2)
	}
	login, err := strconv.ParseUint(env("MT5_LOGIN"), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "MT5_LOGIN: %v\n", err)
		os.Exit(2)
	}
	symbol := "EURUSD"
	if len(os.Args) > 1 {
		symbol = os.Args[1]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client, err := mtapi.Dial(ctx, mtapi.Config{
		Login:    login,
		Password: env("MT5_PASSWORD"),
		Server:   env("MT5_SERVER"),
		Host:     os.Getenv("MT5_HOST"),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	quote, err := client.SubscribeQuote(ctx, symbol)
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%s bid=%.5f ask=%.5f\n", symbol, quote.Bid, quote.Ask)

	buyRes, err := client.Buy(ctx, symbol, 0.02, mtapi.TradeOptions{Comment: "mtapi demo"})
	opened := step("buy 0.02", buyRes, err)
	ticket := opened.Order

	sl := quote.Bid - 0.0050
	tp := quote.Bid + 0.0050
	modRes, err := client.ModifyPosition(ctx, ticket, &sl, &tp, nil)
	step("modify sl/tp", modRes, err)
	partRes, err := client.PartialClose(ctx, ticket, 0.01, nil, mtapi.TradeOptions{})
	step("partial close 0.01", partRes, err)
	closeRes, err := client.ClosePosition(ctx, ticket, nil, mtapi.TradeOptions{})
	step("close", closeRes, err)
	fmt.Println("round trip complete")
}
