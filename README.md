# mtapi

A pure Go client library for the MetaTrader 5 WebSocket (web
terminal) protocol. It connects directly to a broker's MT5 access
servers with an account login and password — no MetaTrader terminal,
no DLL, no Windows — and exposes the full surface of a normal MT5
client:

- account state and the full symbol list;
- live quote subscriptions (streaming, with callbacks);
- open positions, pending orders, deal history, OHLC candles;
- trading: market and pending orders (limit, stop, stop-limit),
  close / partial close, modify, cancel;
- broker server resolution (SearchMQ directory, mirror fallback,
  and offline `servers.dat` parsing).

**Zero external dependencies. Pure Go — builds with `CGO_ENABLED=0`
as a fully static binary on every platform.**

## Install

```sh
go get github.com/voorz/mtapi
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/voorz/mtapi"
)

func main() {
	ctx := context.Background()

	client, err := mtapi.Dial(ctx, mtapi.Config{
		Login:    12345678,
		Password: "secret",
		Server:   "Exness-MT5Trial17", // resolved automatically
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	acct, _ := client.Account(ctx)
	fmt.Printf("balance %.2f %s\n", acct.Balance, acct.Currency)

	quote, _ := client.SubscribeQuote(ctx, "EURUSD")
	fmt.Printf("EURUSD bid %.5f ask %.5f\n", quote.Bid, quote.Ask)

	res, err := client.Buy(ctx, "EURUSD", 0.01, mtapi.TradeOptions{})
	fmt.Printf("buy: retcode=%d order=%d err=%v\n", res.Retcode, res.Order, err)
}
```

An **investor (read-only) password** works for everything except
trading, which makes the library a good fit for trading-competition
statistics and account monitoring — see
[examples/stats](examples/stats).

## Concepts

- **One `Client` is one account session.** For several accounts, dial
  several clients (see [examples/multi](examples/multi)). All methods
  are safe for concurrent use.
- **Positions are identified by their order ticket**, not the deal
  ticket. `TradeResult.Order` of an opening trade is the ticket that
  `ClosePosition`, `PartialClose` and `ModifyPosition` take.
- **Subscribe before trading.** Market orders are priced from the
  live quote, so a symbol must be subscribed (and its first quote
  received) first — `SubscribeQuote` does both.
- **The position list can lag right after a trade.** `ClosePosition`
  and friends accept an optional `*PositionRef` / `*OrderRef` that
  supplies the position's fields directly and skips the list lookup,
  and the client additionally caches positions/orders from trade
  results.
- **Errors** are classified with `errors.Is` against the package's
  sentinels (`ErrAuth`, `ErrTimeout`, `ErrServerNotFound`,
  `ErrSymbolNotFound`, `ErrNoQuote`, ...) and trade failures come back
  as `*TradeError` carrying the MT5 retcode (`errors.As`).

## Configuration

| Field        | Meaning                                                                 |
| ------------ | ----------------------------------------------------------------------- |
| `Login`      | MT5 account number (required)                                           |
| `Password`   | account or investor password (required)                                 |
| `Server`     | server name, resolved via broker search                                 |
| `Host`       | optional access point `ip:port`; skips resolution when set              |
| `Build`      | terminal protocol build, default 5830                                   |
| `Timeout`    | per-request timeout, default 10s                                        |
| `TLSConfig`  | TLS override (default does not verify access-server certificates — they are reached by IP and present non-matching certs, as the reference client handles them) |
| `HTTPClient` | HTTP client for server resolution                                       |

## Examples

| Example                              | Shows                                                        |
| ------------------------------------ | ------------------------------------------------------------ |
| [examples/stats](examples/stats)     | login → account/positions/orders/deals as one JSON snapshot  |
| [examples/quote](examples/quote)     | streaming quotes via the `OnQuote` callback                  |
| [examples/trade](examples/trade)     | demo round trip: buy → modify → partial close → close        |
| [examples/multi](examples/multi)     | polling many accounts in parallel                            |

## Design and protocol notes

[DESIGN.md](DESIGN.md) documents the architecture, the staged scope
(what was ported from the reverse-engineered reference, and what is
planned next — including the native TCP transport), and the protocol
summary: wire framing, AES-256-CBC session encryption, the schema
serialization system, and the trade request layout.

## License

MIT — see [LICENSE](LICENSE).
