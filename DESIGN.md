# mtapi — Design

A pure Go client library for the MetaTrader 5 WebSocket (web terminal)
protocol. It connects directly to a broker's MT5 access servers with an
account login and password — no MetaTrader terminal, no DLL, no Windows.

Module: `github.com/voorz/mtapi`

## 1. Goal and approach

Port the existing, working reverse-engineered implementation to Go as a
clean, importable library:

- **Reference implementation**: `MT5API/mt5client` (pure Python, async)
  from the MT5-Client-Reverse-Engineer project, plus its protocol
  documentation (`WEBSOCKET_PROTOCOL.md`, `FUNCTION_COVERAGE.md`,
  `PLAN.md`, `BUILD.md`, `ADDITIONAL_FEATURES.md`).
- **Rule (owner decision, 2026-10-27)**: Stage 1 ports what the reference
  already has. Anything the reference lacks or leaves thin is Stage 2 —
  we complete it later, deliberately, not by scope-creeping Stage 1.
- The reference repository carries no license. This project is an
  original Go implementation written from the documented protocol
  behaviour (wire formats, command ids, schemas, cryptographic steps);
  it does not copy the reference's code text.

## 2. Protocol summary (from the reference documentation)

- **Server resolution**: a server name (e.g. `Exness-MT5Trial17`) is
  resolved to access-point IPs via MetaQuotes' SearchMQ endpoint
  (`https://updates.metaquotes.net/public/mt5/network`, MD5-signed
  requests), with `search.mtapi.io` as fallback. A configured host
  bypasses resolution entirely.
- **Transport**: TLS WebSocket to `wss://<access-ip>:443/terminal`.
- **Crypto**: AES-256-CBC (zero IV). A static key protects the auth
  handshake (command 0), which exchanges a per-session key; the login
  (command 28) and all later payloads are encrypted with the session
  key. An XOR chain cipher is part of the wire encoding.
- **Wire format**: commands and responses are binary payloads with
  fixed schemas (the `Ac.parse` serialization system): account (`fl`,
  cmd 3), positions/orders (`uu`, cmd 4), deals (cmd 5), quotes (`Uh`,
  cmd 8), candles (cmd 11, 48-byte records), trade requests (cmd 12,
  a 248-byte `Op` struct inside a 380-byte envelope), trade events
  (cmd 19), symbols (`Mh`, cmd 34).
- **Defaults**: protocol build 5830; volume is encoded as
  lots × 100,000,000; a successful trade returns retcode 10009.
- **Semantics that must survive the port**:
  - A position is identified by its **order ticket**, not the deal
    ticket.
  - The cmd-4 position/order list can be **stale** right after a
    trade. The reference works around this with bypass parameters and
    a position cache fed by trade results; the Go client keeps the
    same strategy.
  - Market orders require a live quote: subscribe first.

## 3. Stage 1 scope — port what exists

### 3.1 Client surface (Python → Go)

| Reference (Python, async)            | Go (package `mtapi`)                                   |
| ------------------------------------ | ------------------------------------------------------ |
| `MT5Client(login, password, server, build=5830)` | `mtapi.Dial(ctx, mtapi.Config{...}) (*Client, error)` |
| `get_account()`                      | `Client.Account(ctx)`                                  |
| `get_symbols()`                      | `Client.Symbols(ctx)`                                  |
| `subscribe(symbol)` / `subscribe_batch(symbols)` / `send_subscribe(symbols)` / `get_quote(symbol)` | `Client.Subscribe(ctx, symbols...)`, `Client.Quote(symbol)` (cached) |
| `buy` / `sell`                       | `Client.Buy(ctx, symbol, volume, opts...)` / `Sell`    |
| `buy_limit` / `sell_limit` / `buy_stop` / `sell_stop` (+ stop-limit) | `Client.BuyLimit` / `SellLimit` / `BuyStop` / `SellStop` / `BuyStopLimit` / `SellStopLimit` |
| `close_position` / `partial_close`   | `Client.ClosePosition` / `PartialClose`                |
| `modify_position`                    | `Client.ModifyPosition`                                |
| `cancel_order` / `modify_order`      | `Client.CancelOrder` / `ModifyOrder`                   |
| `get_positions()` / `get_orders()`   | `Client.Positions(ctx)` / `Client.Orders(ctx)`         |
| `get_deals(from_time, to_time)`      | `Client.Deals(ctx, from, to time.Time)`                |
| `get_candles(symbol, timeframe, count)` | `Client.Candles(ctx, symbol, timeframe, count)`     |
| `on_quote` / `on_trade` / `on_spec` callbacks | `Client.OnQuote(func(Quote))`, `OnTrade(func(TradeEvent))`, `OnSpec(...)` |
| `search` / `search_mq` / `find_server` / `find_server_ips` | `mtapi.FindServer(ctx, name)`, `mtapi.FindServerIPs(ctx, name)` |

All blocking methods take a `context.Context`. One `Client` is one
account session; it is safe for concurrent use (a send lock plus a
pending-request table, as in the reference). Multi-account use is
multiple clients, as in the reference's `asyncio.gather` example.

### 3.2 Data models

`Account`, `Symbol`, `Quote`, `Position`, `Order`, `Deal`, `Candle`,
`TradeResult`, `ServerInfo` — same fields as the reference dataclasses,
with Go types (`time.Time` in UTC, `int64` tickets, `float64` prices
and volumes in lots at the API surface; raw wire units stay internal).

### 3.3 Errors

Reference hierarchy, Go-flavoured: a base `*mtapi.Error` carrying the
MT5 retcode where applicable, with sentinel/typed variants for
`AuthError`, `TradeError`, `ServerNotFoundError`, `ConnectionError`,
`TimeoutError` (checked with `errors.Is` / `errors.As`).

### 3.4 Server resolution

SearchMQ client (signature scheme as documented), `mt5.mtapi.io`
and `search.mtapi.io` fallbacks, `Config.Host` direct override, and a
`servers.dat` parser (the reference ships one as a standalone script)
for offline resolution.

## 3.5 Live verification notes (Stage 1, 2026-10-27)

Verified end to end against a live Exness demo account
(Exness-MT5Trial5, Pro group) from a test host — the read path
(account, symbols, quotes, candles, positions, orders, deals), a
full market trade round trip (buy → modify SL/TP → partial close →
close), and the pending-order lifecycle (buy limit → modify →
cancel). Every trade method the library exposes has now been
executed against the live server.
Findings that changed the code, all invisible from the reference
alone:

- **The account record does not carry the login number** (it appears
  nowhere in the login or account responses; the record's first
  field is an internal id — the reference surfaces it as the login).
  `Account.Login` reports the authenticated login instead. On this
  server the record's equity/margin fields are also zero; the
  balance is exact. Callers needing equity can compute
  balance + Σ(position profit + swap) from the position list.
- **Market orders are execution-mode sensitive.** The reference
  hardcodes trade action MARKET, which instant-execution symbols
  reject with retcode 10013 (nothing executes; the command-12 body
  is 0 and the rejection arrives in the trade event). INSTANT is
  required instead — for closes as well as opens. Implemented as
  `Config.Execution` (Auto learns per symbol) — see trade.go.
- **Instant closes requote.** With a zero price deviation a close
  is answered 10004 as soon as the price ticks; closes therefore
  accept a deviation like opens do, and market operations retry
  requotes with a fresh quote (bounded).
- **The position list lags partial closes.** Right after a partial
  close the list still reports the pre-trade volume, and a full
  close resolved from it is rejected 10014 (invalid volume). The
  trade-fed cache is therefore updated from every trade result and
  takes precedence over the list when resolving a ticket.

## 4. Stage 2 backlog — what the reference does not have

Deliberately out of Stage 1; each item gets verified against the
protocol docs before it is scheduled:

- **Native TCP transport** (the `mt5api.dll` binary protocol).
  The reference documents the login handshake in depth
  (`CONNECTION_PROTOCOL.md`) but has no complete post-login command
  implementation. Spike (2026-10-27): the documented handshake was
  implemented from the document and **verified live** against a
  real Exness access point — plain TCP, TAG 0 login packet, 32-byte
  challenge (Msg=0), streaming-MD5 challenge solution, TAG 1
  response with Msg=0 (DONE) and a ~3 KB account payload. The
  handshake is therefore a solved problem. Everything after login
  (account/symbol/trade commands on this transport) is undocumented:
  the decompiled DLL sources the document cites were never
  committed to the reference repository, so the post-login command
  layer must be reverse-engineered from live traffic — an
  open-ended research item, not a port. The Stage 1 transport is
  an interface so this can be added without API changes.
- **Function-coverage categories that exist only as scripts or doc
  claims**, not in the packaged client: order book / depth of market,
  times & sales, tick history download, profit/margin calculation
  helpers, password change, internal mail, equity history, demo
  account creation.
- ~~**Robustness beyond the reference**: automatic reconnect with
  session re-establishment and subscription restore~~ — **done in
  Stage 2 (2026-10-27)**. Config.AutoReconnect: a supervisor re-dials
  (fresh address resolution per attempt), re-runs the auth/login
  handshake, reloads account and symbols, drops stale quotes and
  trade-fed caches, and restores subscriptions by name, with
  backoff from 1s to a 30s cap. Idempotent reads retry
  transparently on the new session; a trade bound to a dead session
  fails with ErrConnection and is never resubmitted. A
  handshake/login failure during reconnect is permanent and
  surfaces as the client's fatal error. Client.OnReconnect fires
  after each recovery. Verified against the fake server (drop and
  recover; second login; subscription restore) and live through a
  killable TCP relay in front of a real access point: an in-flight
  Positions call blocked through a ~19s outage and completed on
  the new session.
- MT4 support of any kind (the reference is MT5-only).

## 5. Package layout

```
github.com/voorz/mtapi
  client.go      Client, Config, Dial, Close
  account.go     Account
  market.go      Symbols, Subscribe, Quote, Candles
  trade.go       Buy/Sell, pending orders, Close/Modify/Cancel
  history.go     Positions, Orders, Deals
  types.go       public data models, Timeframe
  errors.go      Error type and variants
  events.go      OnQuote / OnTrade / OnSpec
  search.go      FindServer / FindServerIPs (public wrappers)
  internal/wire/     frame codec, command builders, schema parsers
  internal/cryptox/  AES-256-CBC, XOR chain, key handling
  internal/searchmq/ SearchMQ protocol, fallback source, servers.dat
  internal/ws/       minimal WebSocket client (see §6)
  examples/stats/    login → account/positions/deals → JSON snapshot
  examples/quote/    subscribe and print quotes
  docs/              protocol notes distilled from the reference docs
```

## 6. Build rules

- **Pure Go, `CGO_ENABLED=0`** — the deliverable must build to a
  statically linked binary/library on any platform.
- **Zero external dependencies.** AES-CBC, TLS and MD5 are standard
  library. The one missing piece, a WebSocket client, is implemented
  in `internal/ws` (handshake, frame codec, masking, ping/pong,
  fragmentation — the client subset of RFC 6455) rather than imported.
- All documentation and commit messages in English; Conventional
  Commits, one logical change per commit.
- CI runs offline checks only (`go build`, `go vet`, `go test`).
  Live verification never runs in CI.

## 7. Verification plan

- **Unit tests** with golden vectors taken from the reference's
  documented behaviour (frame bytes, schema layouts, crypto steps).
- **Live verification** against an Exness demo account — the same
  environment the reference was validated on — run from the B1 test
  machine, never from CI and never against a live-money account
  during Stage 1.
- Milestone demos double as acceptance checks (§8).

## 8. Stage 1 milestones

| #  | Content                                                        | Acceptance                                  |
| -- | -------------------------------------------------------------- | ------------------------------------------- |
| M1 | ws transport, cryptox, SearchMQ, auth + login, Account/Symbols | Demo login prints account and symbol count  |
| M2 | Subscribe/quotes, Positions, Orders, Deals, Candles            | `examples/stats` emits a full JSON snapshot |
| M3 | Trading: market, pending, close/partial, modify, cancel; trade events; position cache + bypass semantics | Demo round-trip: open → modify → partial close → close, events observed |
| M4 | Heartbeat, servers.dat offline resolution, multi-account example, docs | Two demo accounts polled in parallel from one program |
