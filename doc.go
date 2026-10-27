// Package mtapi is a pure Go client library for the MetaTrader 5
// WebSocket (web terminal) protocol. It connects directly to a
// broker's MT5 access servers with an account login and password —
// no MetaTrader terminal, no DLL, no Windows — and exposes the full
// surface of a normal MT5 client: account state, the symbol list,
// live quotes, positions, pending orders, deal history, candles, and
// trading.
//
// Usage:
//
//	client, err := mtapi.Dial(ctx, mtapi.Config{
//		Login:    12345678,
//		Password: "secret",
//		Server:   "Exness-MT5Trial17",
//	})
//	if err != nil { ... }
//	defer client.Close()
//
//	acct, err := client.Account(ctx)
//	q, err := client.SubscribeQuote(ctx, "EURUSD")
//	res, err := client.Buy(ctx, "EURUSD", 0.01, mtapi.TradeOptions{})
//
// One Client is one account session; use several clients for several
// accounts. An investor (read-only) password supports everything
// except trading, which makes the library suitable for competition
// statistics and monitoring. Positions are identified by their order
// ticket, not the deal ticket — see Position.
//
// The implementation is a clean-room Go port of the
// reverse-engineered protocol documented in DESIGN.md: zero external
// dependencies, and it builds with CGO_ENABLED=0.
package mtapi
