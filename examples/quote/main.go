// Command quote subscribes to symbols and prints quotes as they
// stream in, until interrupted.
//
// Configuration (environment): MT5_LOGIN, MT5_PASSWORD, MT5_SERVER,
// optional MT5_HOST — see examples/stats. Symbols are command-line
// arguments (default: EURUSD).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

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

func main() {
	login, err := strconv.ParseUint(env("MT5_LOGIN"), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "MT5_LOGIN: %v\n", err)
		os.Exit(2)
	}
	symbols := os.Args[1:]
	if len(symbols) == 0 {
		symbols = []string{"EURUSD"}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	client.OnQuote(func(q mtapi.Quote) {
		fmt.Printf("%s bid=%.5f ask=%.5f\n", q.Symbol, q.Bid, q.Ask)
	})
	if err := client.Subscribe(ctx, symbols...); err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("subscribed to %v — Ctrl+C to stop\n", symbols)
	<-ctx.Done()
}
