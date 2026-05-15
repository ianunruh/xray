# Market Maker Implementation Plan

## Context

The xray trading system has an event-sourced orderbook, portfolio management, and saga orchestration, but no automated trading strategies. We're adding a market maker that runs as a separate binary, connects as an external gRPC client, and quotes spreads around an external reference price from Polygon (Massive). It's designed with a strategy interface so other strategies can be added later.

## Architecture

Separate binary `cmd/xray-mm/` connecting to the xray server via Connect RPC. One goroutine per symbol, each running an independent engine. Portfolio-aware (orders go through the saga flow for real cash/share tracking).

**Pre-requisite: CreditShares RPC.** The portfolio service needs a new `CreditShares` RPC so the MM can bootstrap with shares for the sell side. Without this, the MM can only place bids on an empty market (sell-side saga would fail on HoldShares). This is the share equivalent of Deposit — adds shares to a portfolio without a trade.

```
# Server changes (pre-requisite)
proto/portfolio/v1/service.proto   # Add CreditShares RPC + messages
proto/portfolio/v1/events.proto    # Add SharesCredited event
internal/portfolio/aggregate.go    # Apply SharesCredited
internal/portfolio/commands.go     # CreditShares command + execute
internal/portfolio/events.go       # Event constant
internal/portfolio/server.go       # CreditShares handler

# Market maker (new)
cmd/xray-mm/main.go               # Entry point, config, signal handling, wiring
internal/mm/
  config.go                        # YAML config struct + loader + validation
  pricesource.go                   # PriceSource interface + PriceSnapshot type
  polygon.go                       # Polygon REST adapter (poll prev close every 30s)
  static.go                        # Static price source for testing
  strategy.go                      # Strategy interface + SpreadStrategy
  engine.go                        # Per-symbol engine: main loop, order tracking, requoting
```

## CreditShares RPC (server pre-requisite)

New proto messages in `portfolio/v1/service.proto`:
```protobuf
rpc CreditShares(CreditSharesRequest) returns (CreditSharesResponse);

message CreditSharesRequest {
  string account_id = 1;
  string symbol = 2;
  int64 quantity = 3;
  int64 cost_per_share = 4;
}
message CreditSharesResponse {}
```

New event in `portfolio/v1/events.proto`:
```protobuf
message SharesCredited {
  string account_id = 1;
  string symbol = 2;
  int64 quantity = 3;
  int64 cost_per_share = 4;
  google.protobuf.Timestamp credited_at = 5;
}
```

Portfolio aggregate `Apply(SharesCredited)`: increment `Holdings[symbol].Quantity` by quantity, add `cost_per_share * quantity` to `Holdings[symbol].TotalCost`.

Server handler: validate quantity > 0, execute command, return empty response.

Run `buf generate` after proto changes.

## Config (`internal/mm/config.go`)

YAML config with per-symbol settings. The `polygon_api_key` can also be set via the `POLYGON_API_KEY` environment variable (env var is used as fallback when not set in config).

```yaml
server_url: "http://localhost:8080"
polygon_api_key: "key"             # or set POLYGON_API_KEY env var
log_level: "info"
price_source: "polygon"            # or "static"

symbols:
  - symbol: AAPL
    account_id: mm-AAPL
    initial_deposit: 10000000000   # $1M — deposited on first startup
    initial_shares: 1000           # credited on first startup for sell-side quotes
    spread: 10000                  # $1.00 total spread
    quantity: 10                   # shares per level
    levels: 3
    level_spacing: 10000           # $1.00 between levels (defaults to spread)
    max_position: 100
    requote_interval: 30s
    price_move_threshold: 5000     # $0.50

polygon:
  base_url: "https://api.polygon.io"
  poll_interval: 30s

static_prices:
  AAPL: 1500000
```

Go structs: `Config`, `SymbolConfig`, `PolygonConfig`. Defaults applied for optional fields. Validation checks required fields, consistent price_source/api_key, etc.

## Price Source (`internal/mm/pricesource.go`, `polygon.go`, `static.go`)

```go
type PriceSnapshot struct {
    Price     int64
    FetchedAt time.Time
}

type PriceSource interface {
    GetPrice(symbol string) (PriceSnapshot, bool)
    Start(ctx context.Context) error
}
```

**Polygon adapter**: Polls `GET /v2/aggs/ticker/{ticker}/prev?apiKey=...` on a timer. Converts float64 close price to int64 with 4 decimals. Thread-safe via `sync.RWMutex`. Logs warnings on errors/empty results but keeps running.

**Static adapter**: Returns fixed prices from config. For testing.

## Strategy (`internal/mm/strategy.go`)

```go
type QuoteLevel struct {
    Side     orderbookv1.Side
    Price    int64
    Quantity int64
}

type InventoryState struct {
    Position    int64
    MaxPosition int64
}

type Strategy interface {
    ComputeQuotes(refPrice int64, inventory InventoryState) []QuoteLevel
}
```

**SpreadStrategy**: Places N levels on each side. Level i bid = `refPrice - halfSpread - i*levelSpacing`, ask = `refPrice + halfSpread + i*levelSpacing`. Uniform size per level.

Hard inventory cutoff: position >= maxPosition stops buy-side quotes, position <= -maxPosition stops sell-side quotes.

## Engine (`internal/mm/engine.go`)

One Engine per symbol. All mutable state accessed from a single goroutine (channel-based communication from background goroutines).

### State tracking

```go
type trackedOrder struct {
    sagaID  string
    orderID string  // empty until saga resolves
    side    orderbookv1.Side
    price   int64
    qty     int64
}
```

Maps: `sagaID -> trackedOrder` and `orderID -> sagaID` (for fill matching).

### Main loop

```
Run(ctx):
  1. Bootstrap: GetPortfolio, deposit cash if cash_balance == 0, credit shares if holding == 0
  2. Cleanup orphan orders from previous run (GetPortfolio pending_orders -> cancel via orderbook)
  3. Start StreamTrades in background goroutine -> sends fills to fillCh
  4. Initial requote
  5. Select loop:
     - ctx.Done -> cancelAllOrders(fresh context), return
     - requoteTicker (30s) -> requote
     - priceCheckTicker (1s) -> check price move threshold -> requote if exceeded
     - fillCh (trade from stream) -> match order IDs, requote on fill
     - resolveCh (saga resolved to order_id) -> update tracking maps
```

### Requote flow

1. Cancel all tracked orders via `OrderBookService.CancelOrder` (CodeNotFound = already gone, ignore)
2. Clear tracking maps
3. `GetPortfolio` to get current position (holdings quantity for symbol)
4. `strategy.ComputeQuotes(refPrice, inventory)` to get desired levels
5. For each level: `PortfolioService.PlaceOrder` -> get saga_id -> track it -> spawn resolve goroutine
6. Resolve goroutine polls `GetOrderStatus` with exponential backoff (100ms..1s, max 20 attempts) until order_id available, sends result on `resolveCh`

### Fill detection

StreamTrades for the symbol. Match `trade.BuyOrderId` / `trade.SellOrderId` against tracked order IDs. On match, trigger immediate requote (refreshes position and quotes).

Stream auto-reconnects on disconnect with 1s backoff.

### Staleness guard

Skip requoting if price source has no data or data is >5min old. Log warning.

### Shutdown

On SIGTERM/SIGINT: context cancelled -> each engine cancels all active orders with a background context -> engines return -> main waits on WaitGroup -> clean exit.

### Orphan cleanup

On startup, `GetPortfolio` returns `pending_orders`. For each with `ORDER_PLACED` status, look up its `order_id` via `GetOrderStatus` and cancel it on the orderbook.

## Entry Point (`cmd/xray-mm/main.go`)

1. Parse `-config` flag (default `mm.yaml`)
2. Load + validate config
3. Create slog JSON logger
4. `signal.NotifyContext` for SIGINT/SIGTERM
5. Create shared `http.Client`, Connect RPC clients (orderbook + portfolio)
6. Create price source (polygon or static based on config)
7. Start price source in background goroutine
8. For each symbol config: create SpreadStrategy, create Engine, start in goroutine
9. `sync.WaitGroup` wait for all engines
10. Log shutdown complete

## Dependencies

- `gopkg.in/yaml.v3` — promote from indirect to direct (already in go.sum via testify)
- All other deps (connectrpc, generated clients) already direct

## Testing

**Unit tests (no server):**
- `strategy_test.go` — level computation, inventory cutoffs, edge cases (zero price, max levels)
- `config_test.go` — YAML parsing, defaults, validation errors
- `polygon_test.go` — mock HTTP server, response parsing, error handling, float-to-int64 conversion

**Integration tests (deferred):**
- Engine lifecycle against memstore-backed server (complex setup, defer to later)

## Verification

1. `go build ./...` compiles cleanly
2. `go test ./...` passes (strategy + config + polygon unit tests)
3. Manual: start xray server, start xray-mm with static prices, observe orders on book via `GetMarketDepth`
4. Manual: place a crossing order from another client, observe MM requote

## Implementation order

**Phase 1: CreditShares RPC (server change)**
1. Add proto messages + event to `portfolio/v1/service.proto` and `portfolio/v1/events.proto`
2. `buf generate`
3. Add event constant, command, aggregate Apply, server handler to `internal/portfolio/`
4. Add test for CreditShares command

**Phase 2: Market maker foundation**
5. `internal/mm/config.go` + `internal/mm/config_test.go`
6. `internal/mm/pricesource.go` + `internal/mm/static.go`
7. `internal/mm/polygon.go` + `internal/mm/polygon_test.go`
8. `internal/mm/strategy.go` + `internal/mm/strategy_test.go`

**Phase 3: Market maker engine + binary**
9. `internal/mm/engine.go`
10. `cmd/xray-mm/main.go`
11. Promote yaml.v3 in go.mod
