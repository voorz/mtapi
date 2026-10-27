// Command stats logs in to one MT5 account and prints a JSON snapshot
// of the account, its open positions, pending orders and the last 30
// days of deals — the shape a trading-competition statistics job
// needs. It only reads, so an investor (read-only) password is
// enough.
//
// Configuration (environment):
//
//	MT5_LOGIN     account number (required)
//	MT5_PASSWORD  account or investor password (required)
//	MT5_SERVER    server name, e.g. Exness-MT5Trial17 (required)
//	MT5_HOST      optional access point "ip:port", skips resolution
package main

import (
	"context"
	"encoding/json"
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

func main() {
	login, err := strconv.ParseUint(env("MT5_LOGIN"), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "MT5_LOGIN: %v\n", err)
		os.Exit(2)
	}
	cfg := mtapi.Config{
		// A statistics collector runs for hours: recover dropped
		// transports in place instead of dying with them.
		AutoReconnect: true,
		Login:         login,
		Password:      env("MT5_PASSWORD"),
		Server:        env("MT5_SERVER"),
		Host:          os.Getenv("MT5_HOST"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, err := mtapi.Dial(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()
	client.OnReconnect(func() { fmt.Println("(reconnected; session restored)") })

	account, err := client.Account(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "account: %v\n", err)
		os.Exit(1)
	}
	positions, err := client.Positions(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "positions: %v\n", err)
		os.Exit(1)
	}
	orders, err := client.Orders(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "orders: %v\n", err)
		os.Exit(1)
	}
	deals, err := client.Deals(ctx, time.Now().AddDate(0, 0, -30), time.Time{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "deals: %v\n", err)
		os.Exit(1)
	}

	snapshot := map[string]any{
		"account":   account,
		"positions": positions,
		"orders":    orders,
		"deals":     deals,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(snapshot); err != nil {
		fmt.Fprintf(os.Stderr, "encode: %v\n", err)
		os.Exit(1)
	}
}
