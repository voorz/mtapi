// Command multi polls several MT5 accounts in parallel — one Client
// per account — and prints balance/equity snapshots. This is the
// shape a competition leaderboard collector takes.
//
// Configuration (environment):
//
//	MT5_ACCOUNTS  semicolon-separated "login:password:server" entries
//	              (passwords must not contain ':' or ';')
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/voorz/mtapi"
)

func parseAccounts() []mtapi.Config {
	raw := os.Getenv("MT5_ACCOUNTS")
	if raw == "" {
		fmt.Fprintln(os.Stderr, "missing required environment variable MT5_ACCOUNTS")
		os.Exit(2)
	}
	var out []mtapi.Config
	for _, entry := range strings.Split(raw, ";") {
		parts := strings.SplitN(strings.TrimSpace(entry), ":", 3)
		if len(parts) != 3 {
			fmt.Fprintf(os.Stderr, "bad account entry %q (want login:password:server)\n", entry)
			os.Exit(2)
		}
		login, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "login in %q: %v\n", entry, err)
			os.Exit(2)
		}
		out = append(out, mtapi.Config{Login: login, Password: parts[1], Server: parts[2]})
	}
	return out
}

func poll(ctx context.Context, cfg mtapi.Config, wg *sync.WaitGroup) {
	defer wg.Done()
	client, err := mtapi.Dial(ctx, cfg)
	if err != nil {
		fmt.Printf("login %d (%s): dial: %v\n", cfg.Login, cfg.Server, err)
		return
	}
	defer client.Close()
	acct, err := client.Account(ctx)
	if err != nil {
		fmt.Printf("login %d (%s): account: %v\n", cfg.Login, cfg.Server, err)
		return
	}
	positions, err := client.Positions(ctx)
	if err != nil {
		fmt.Printf("login %d (%s): positions: %v\n", cfg.Login, cfg.Server, err)
		return
	}
	fmt.Printf("login %d (%s): balance=%.2f equity=%.2f open positions=%d\n",
		acct.Login, acct.Server, acct.Balance, acct.Equity, len(positions))
}

func main() {
	configs := parseAccounts()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, cfg := range configs {
		wg.Add(1)
		go poll(ctx, cfg, &wg)
	}
	wg.Wait()
}
