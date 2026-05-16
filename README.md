# xray

Event-sourced order book — a learning project implementing a simple but realistic stock exchange order book with buy/sell limit orders, partial and complete fills. Uses event sourcing with protobuf serialization.

![Web UI](docs/webui.png)

## Architecture

xray is an event-sourced CQRS system. Aggregates own state and emit events; events fan out via NATS to reactors (which issue follow-up commands), projections (which build read models), and brokers (which push live updates to subscribers).

```mermaid
graph TB
    Client((Client / Web UI / Strategies))

    subgraph services["RPC services"]
        direction TB
        OBSvc[OrderBookService]
        PortSvc[PortfolioService]
        SagaSvc[SagaService]
    end

    subgraph aggregates["Aggregates (write-side, event-sourced)"]
        direction TB
        OrderBook[OrderBook]
        Portfolio[Portfolio]
        OrderSaga[OrderSaga]
        BracketSaga[BracketSaga]
        OCOSaga[OCOSaga]
    end

    EventStore[("Postgres event log")]
    NATS{{"NATS JetStream<br/>per-consumer durable cursors"}}

    subgraph reactors["Reactors (drive sagas)"]
        direction TB
        OSR[OrderSaga Reactor]
        BR[Bracket Reactor]
        OCR[OCOSaga Reactor]
    end

    subgraph projections["Projections (read-side)"]
        direction TB
        TradeP[trades]
        OrderP[orders]
        PortP[portfolio]
        PnLP[pnl]
        SagaP[sagas]
        DepthP[depth*]
        CandleP[candles*]
    end

    subgraph brokers["Brokers (live push)"]
        direction TB
        OBB[OrderBook Broker]
        PB[Portfolio Broker]
    end

    Reconciler[Periodic Reconciler]

    Client -->|RPC| OBSvc
    Client -->|RPC| PortSvc
    Client -->|RPC| SagaSvc

    OBSvc ==>|commands| OrderBook
    PortSvc ==>|commands| Portfolio
    SagaSvc ==>|StartOrderSaga| OrderSaga
    SagaSvc ==>|StartSaga| BracketSaga
    SagaSvc ==>|StartOCOSaga| OCOSaga

    OrderBook -.->|events| EventStore
    Portfolio -.->|events| EventStore
    OrderSaga -.->|events| EventStore
    BracketSaga -.->|events| EventStore
    OCOSaga -.->|events| EventStore
    EventStore -.-> NATS

    NATS -.-> OSR
    NATS -.-> BR
    NATS -.-> OCR
    NATS -.-> projections
    NATS -.-> OBB
    NATS -.-> PB

    OSR ==>|RecordCashHeld<br/>RecordOrderPlaced<br/>RecordFill| OrderSaga
    OSR ==>|HoldCash / SettleTrade<br/>HoldShares / SettleSale| Portfolio
    OSR ==>|PlaceOrder<br/>CancelOrder| OrderBook

    BR ==>|RecordEntryFilled<br/>RecordExitFilled| BracketSaga
    BR ==>|StartOrderSaga<br/>entry leg| OrderSaga
    BR ==>|StartOCOSaga<br/>exit leg| OCOSaga

    OCR ==>|RecordSharesHeld<br/>RecordExitPlaced<br/>RecordFill| OCOSaga
    OCR ==>|HoldShares / SettleSale<br/>ReleaseShares| Portfolio
    OCR ==>|PlaceOrder<br/>TP + SL with OCO group| OrderBook

    OBSvc -.->|reads| TradeP
    OBSvc -.->|reads| OrderP
    OBSvc -.->|reads| DepthP
    OBSvc -.->|reads| CandleP
    PortSvc -.->|reads| PortP
    PortSvc -.->|reads| PnLP
    SagaSvc -.->|reads| SagaP

    OBB -.->|stream| Client
    PB -.->|stream| Client

    Reconciler -.->|active sagas| SagaP
    Reconciler ==>|Reconcile / ReplayTrade| OSR
    Reconciler ==>|Reconcile| BR
    Reconciler ==>|Reconcile / ReplayTrade| OCR

    classDef agg fill:#e8f4ff,stroke:#0066cc,color:#000
    classDef svc fill:#e8ffe8,stroke:#009900,color:#000
    classDef rct fill:#fff4e8,stroke:#cc6600,color:#000
    classDef prj fill:#f4e8ff,stroke:#6600cc,color:#000
    classDef brk fill:#ffe8f4,stroke:#cc0066,color:#000
    classDef infra fill:#f0f0f0,stroke:#333,color:#000

    class OrderBook,Portfolio,OrderSaga,BracketSaga,OCOSaga agg
    class OBSvc,PortSvc,SagaSvc svc
    class OSR,BR,OCR rct
    class TradeP,OrderP,PortP,PnLP,SagaP,DepthP,CandleP prj
    class OBB,PB brk
    class EventStore,NATS,Reconciler infra
```

**Legend:**
- Thick arrows (`==>`) = synchronous commands; thin solid (`-->`) = RPC requests; dashed (`-.->`) = async event flow or read queries.
- Projections marked `*` are in-memory (rebuilt on every boot); the others are PG-backed with durable cursors.
- Each persistent consumer has its own NATS cursor, so projections can advance independently and the saga reactors don't replay from event 0 on every restart.

### Bracket order lifecycle

Brackets compose: the bracket aggregate is a thin orchestrator that spawns an entry `OrderSaga`, observes its completion, then spawns an exit `OCOSaga`. The bracket reactor never touches the portfolio or the orderbook directly — all of that lives in the two child sagas.

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant Svc as SagaService
    participant B as BracketSaga
    participant BR as Bracket Reactor
    participant O as OrderSaga (entry)
    participant OSR as OrderSaga Reactor
    participant C as OCOSaga (exit)
    participant OCR as OCOSaga Reactor
    participant Pf as Portfolio
    participant OB as OrderBook

    User->>Svc: Place(BracketPlan)
    Svc->>B: StartSaga
    B-->>BR: SagaStarted

    BR->>O: StartOrderSaga
    O-->>OSR: OrderSagaStarted
    OSR->>Pf: HoldCash
    OSR->>OB: PlaceOrder (entry)
    OB-->>OSR: TradeExecuted
    OSR->>Pf: SettleTrade (per-trade idempotent)
    OSR->>O: RecordFill / RecordCompleted

    O-->>BR: OrderSagaCompleted
    BR->>C: StartOCOSaga
    BR->>B: RecordEntryFilled

    C-->>OCR: OCOSagaStarted
    OCR->>Pf: HoldShares
    OCR->>OB: PlaceOrder TP + SL (shared OCO group)
    Note over OB: TP fills → orderbook<br/>atomically cancels SL
    OB-->>OCR: TradeExecuted (TP)
    OB-->>OCR: OrderCancelled (SL, reason=oco_triggered)
    OCR->>Pf: SettleSale
    OCR->>C: RecordFill / RecordCompleted

    C-->>BR: OCOSagaCompleted
    BR->>B: RecordExitFilled / SagaCompleted
```

If the user cancels mid-flight, the bracket reactor fails the affected child saga (the entry ordersaga during `PendingEntry`, the exit OCOSaga during `PendingExit`); each child cleans up its own portfolio holds, and the bracket observes the resulting failure event to mark itself `Failed`.

### Package layout

- `proto/` — protobuf definitions (source of truth for events and services)
- `gen/` — generated Go code from protobuf (do not edit)
- `pkg/es/` — reusable event sourcing framework (aggregates, registry, store interface, command handler, snapshots, projection consumer)
- `pkg/es/memstore/` — in-memory EventStore for tests
- `pkg/es/pgstore/` — PostgreSQL EventStore + migrations
- `pkg/es/natsstore/` — NATS publisher + projection consumer with per-name durable cursors
- `internal/orderbook/` — OrderBook aggregate, matching engine, order/trade/depth/candle projections, broker, RPC server
- `internal/portfolio/` — Portfolio aggregate (cash + share holds with per-saga and per-trade idempotency), projections, broker, RPC server
- `internal/ordersaga/` — OrderSaga aggregate + stateless reactor (one order, full portfolio coordination)
- `internal/bracket/` — BracketSaga aggregate + reactor (entry ordersaga + exit OCOSaga orchestration)
- `internal/ocosaga/` — OCOSaga aggregate + reactor (OCO exit: hold shares, TP+SL, settle whichever wins)
- `internal/sagasvc/` — Unified `saga.v1.SagaService` (Place/Get/Cancel/List) + cross-kind projection
- `internal/reconciler/` — Periodic reconciler for stuck sagas and lost trade settlements
- `internal/diagnostics/` — Diagnostics RPCs (event log inspection)
- `internal/pricesource/` — Price source interface + implementations (Polygon, static)
- `internal/trader/` — Shared strategy utilities (bootstrap, order tracking, trade streaming)
- `internal/mm/`, `internal/noise/`, `internal/trend/` — Strategy engines
- `cmd/xray/` — HTTP/gRPC server entry point (registers all services, projections, reactors, reconciler)
- `cmd/xray-mm/`, `cmd/xray-noise/`, `cmd/xray-trend/` — Strategy client binaries

## Key design decisions

- **Prices**: `int64` with 4 implied decimal places (e.g., `$150.50` = `1505000`)
- **Quantities**: `int64` whole units
- **Aggregate IDs**: Prefixed, e.g., `orderbook:AAPL`
- **Event serialization**: Protobuf (`proto.Marshal` / `proto.Unmarshal`), stored as `BYTEA` in Postgres
- **Matching**: Inline during `PlaceOrder` — produces `OrderPlaced` + `TradeExecuted` events atomically
- **API**: Connect (connectrpc.com/connect) — single server speaks gRPC, gRPC-Web, and Connect (JSON-over-HTTP)

## Running the server

```sh
docker compose up -d      # start Postgres
go run ./cmd/xray         # starts HTTP/gRPC server on :8080
```

Environment variables:

- `DATABASE_URL` — Postgres connection string (default: `postgres://xray:xray@localhost:5432/xray?sslmode=disable`)
- `LISTEN_ADDR` — Listen address (default: `:8080`)

## API

The server exposes RPCs via Connect (gRPC, gRPC-Web, and JSON-over-HTTP on the same port):

**`saga.v1.SagaService`** — unified entry point for all order types:

- `Place(PlaceSagaRequest{account_id, plan})` — `plan` is a oneof: `single_order`, `bracket`, or `oco`
- `Get(saga_id)` — returns status, kind, and kind-specific details
- `Cancel(saga_id)` — fails the saga; releases any holds; cancels resting orderbook orders
- `List({account_id, symbol, kind, status})` — all filters optional; hides child sagas spawned by brackets

**`portfolio.v1.PortfolioService`** — account state:

- `Deposit` / `Withdraw` / `CreditShares` — manipulate cash and shares directly (bootstrap and admin)
- `GetPortfolio` / `StreamPortfolio` — cash balance, holdings, pending single-order sagas (point-in-time and server-streamed)
- `GetPnL` — per-position and total realized P&L
- `ListPortfolios` — enumerate all known accounts

**`orderbook.v1.OrderBookService`** — direct orderbook access (mostly useful for tests, market-making, and admin):

- `PlaceOrder` / `CancelOrder` / `ReplaceOrder` — direct order placement, bypasses portfolio
- `GetOrderBook` / `GetMarketDepth` / `StreamMarketDepth` — book state (full and aggregated)
- `GetOrder` / `ListOrders` — single-order and per-symbol order queries
- `ListTrades` / `StreamTrades` — trade history and live stream
- `GetCandles` / `StreamCandles` — OHLC bars
- `ListSymbols` / `CloseMarket` — symbol catalog and end-of-day mass-cancel

### Example usage with buf curl

All examples assume the server is running at `localhost:8080` and use the `--schema proto` flag so `buf curl` can reflect on the local proto files. Run from the repo root.

```sh
# --- Setup: open an account and fund it -----------------------------------

# Deposit $50,000 cash (price units: $1.00 = 10000)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/Deposit \
  -d '{"account_id":"acct-1","amount":"500000000"}'

# Credit 1000 shares of AAPL at $150 cost basis (for testing sells without buying first)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/CreditShares \
  -d '{"account_id":"acct-1","symbol":"AAPL","quantity":"1000","cost_per_share":"1500000"}'

# Read it back
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/GetPortfolio \
  -d '{"account_id":"acct-1"}'

# --- Single orders via SagaService ----------------------------------------

# Limit BUY 100 @ $150 (GTC)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "single_order":{
      "symbol":"AAPL",
      "side":"SIDE_BUY",
      "price":"1500000",
      "quantity":"100",
      "order_type":"ORDER_TYPE_LIMIT",
      "time_in_force":"TIME_IN_FORCE_GTC"
    }
  }'

# Market BUY 50 (IOC) — needs ask-side liquidity; the hold uses a walked-book estimate + slippage buffer
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "single_order":{
      "symbol":"AAPL",
      "side":"SIDE_BUY",
      "quantity":"50",
      "order_type":"ORDER_TYPE_MARKET",
      "time_in_force":"TIME_IN_FORCE_IOC"
    }
  }'

# --- Bracket: BUY 100 @ $150, TP $155, SL $145 ----------------------------
# Spawns an entry ordersaga and an exit OCO saga; both are hidden from List
# but visible via Get if you have their derived saga IDs.
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "bracket":{
      "symbol":"AAPL",
      "entry_side":"SIDE_BUY",
      "entry_price":"1500000",
      "entry_quantity":"100",
      "take_profit_price":"1550000",
      "stop_loss_price":"1450000"
    }
  }'

# --- OCO: exit existing 100 AAPL at $155 TP or $145 SL --------------------
# Standalone OCO — no entry order; assumes you already own the shares.
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "oco":{
      "symbol":"AAPL",
      "exit_side":"SIDE_SELL",
      "quantity":"100",
      "take_profit_price":"1550000",
      "stop_loss_price":"1450000"
    }
  }'

# --- Inspect ---------------------------------------------------------------

# Get a saga's current state + details (works for any kind)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Get \
  -d '{"saga_id":"<saga_id from Place>"}'

# List active sagas for an account (filters all optional)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/List \
  -d '{"account_id":"acct-1","status":"SAGA_STATUS_ACTIVE"}'

# Just the brackets
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/List \
  -d '{"account_id":"acct-1","kind":"SAGA_KIND_BRACKET","status":"SAGA_STATUS_ACTIVE"}'

# --- Cancel ----------------------------------------------------------------

# Cancel a saga (kind-aware — handles single, bracket, OCO appropriately)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Cancel \
  -d '{"saga_id":"<saga_id>"}'

# --- Direct orderbook (no portfolio coordination) -------------------------
# Useful for seeding liquidity in tests or filling someone else's bracket.

# Resting sell at $150 so a BUY bracket has something to match
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/PlaceOrder \
  -d '{"symbol":"AAPL","side":"SIDE_SELL","price":"1500000","quantity":"100"}'

# Inspect the book
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/GetMarketDepth \
  -d '{"symbol":"AAPL"}'

# Recent trades
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/ListTrades \
  -d '{"symbol":"AAPL"}'
```

## Market Maker

The `xray-mm` binary is a spread-based market maker that connects to the xray server as an external client. It quotes bids and asks around a reference price from an external source (Polygon/Massive), with one account per symbol.

### Running

```sh
go run ./cmd/xray-mm -config mm.yaml
```

Or set the Polygon API key via environment variable:

```sh
POLYGON_API_KEY=your-key go run ./cmd/xray-mm -config mm.yaml
```

### Configuration

```yaml
server_url: "http://localhost:8080"
polygon_api_key: "key"             # or set POLYGON_API_KEY env var
log_level: "info"
price_source: "polygon"            # "polygon" or "static"

symbols:
  - symbol: AAPL
    account_id: mm-AAPL
    initial_deposit: 10000000000   # $1M — deposited on first startup
    initial_shares: 1000           # credited on first startup for sell-side quotes
    spread: 10000                  # $1.00 total spread
    quantity: 10                   # shares per level
    levels: 3                      # number of bid/ask levels
    level_spacing: 10000           # $1.00 between levels (defaults to spread)
    max_position: 100              # hard inventory limit per side
    requote_interval: 30s          # timer-based requote backstop
    price_move_threshold: 5000     # $0.50 — requote on ref price move

polygon:
  base_url: "https://api.polygon.io"
  poll_interval: 30s
```

### How it works

- **Hybrid requoting**: Timer backstop (configurable interval), immediate requote on fills, requote when reference price moves beyond threshold
- **Portfolio-aware**: Orders go through the saga flow (hold cash/shares, place order, settle fills) for real P&L tracking
- **Inventory limits**: Hard stop — stops quoting buy side at max long position, sell side at max short
- **Bootstrapping**: On first startup, deposits initial cash and credits initial shares so both sides of the book can be quoted
- **Graceful shutdown**: Cancels all outstanding orders on SIGTERM/SIGINT
- **Orphan cleanup**: On startup, cancels any leftover orders from a previous run
- **Strategy interface**: `SpreadStrategy` is the first implementation; designed for adding other strategies (e.g., inventory-aware Avellaneda-Stoikov)

See [docs/plans/market-maker.md](docs/plans/market-maker.md) for the full design document.

## Noise Trader

The `xray-noise` binary generates random order flow to simulate retail/noise trading activity. It places a mix of market and limit orders at random intervals, creating volume and book depth alongside the market maker.

### Running

```sh
go run ./cmd/xray-noise -config noise.yaml
```

### Configuration

```yaml
server_url: "http://localhost:8080"
price_source: "static"                # "polygon" or "static"
static_prices:
  AAPL: 1505000                       # $150.50

symbols:
  - symbol: AAPL
    account_id: noise-AAPL
    initial_deposit: 5000000000       # $500K
    initial_shares: 500               # credited on first startup for sell-side orders
    order_interval: 3s                # place an order every 3 seconds
    min_quantity: 1                   # random quantity range
    max_quantity: 10
    price_jitter: 50000               # $5.00 — limit orders land in [ref-$5, ref+$5]
    market_order_pct: 0.3             # 30% market orders, 70% limit
    max_position: 200                 # hard inventory limit per side
    buy_bias: 0.5                     # 0.0 = always sell, 1.0 = always buy, 0.5 = neutral
```

### How it works

- **Fire-and-forget**: Places one random order per tick, no cancel/requote cycle
- **Mixed order types**: Configurable fraction of market orders (IOC) vs limit orders (GTC)
- **Symmetric jitter**: Limit order prices are `refPrice + uniform(-jitter, +jitter)`, so roughly half cross the spread (aggressive) and half rest on the book (passive)
- **Position limits**: Flips to the opposite side when at max position, skips if both sides are limited
- **Directional bias**: `buy_bias` controls the probability of buying vs selling (0.5 = neutral)
- **Bootstrapping**: Same as the market maker — deposits initial cash and credits shares on first startup

## Trend Follower

The `xray-trend` binary is an EMA crossover trend follower that buys when the fast EMA crosses above the slow EMA and sells when it crosses below. It streams live trades from the order book to update its EMAs in real-time.

### Running

```sh
go run ./cmd/xray-trend -config trend.yaml
```

### Configuration

```yaml
server_url: "http://localhost:8080"
polygon_api_key: "key"             # or set POLYGON_API_KEY env var
log_level: "info"
price_source: "polygon"            # "polygon" or "static"

symbols:
  - symbol: AAPL
    account_id: trend-AAPL
    initial_deposit: 5000000000    # $500K
    initial_shares: 500            # credited on first startup
    fast_period: 10                # fast EMA period (in trades)
    slow_period: 30                # slow EMA period (in trades)
    quantity: 50                   # shares per order
    max_position: 200              # target long position on buy signal
    order_timeout: 30s             # cancel unfilled orders after this duration
    price_offset: 5000             # $0.50 — offset from last trade price for limit orders

polygon:
  base_url: "https://api.polygon.io"
  poll_interval: 30s
```

### How it works

- **EMA crossover**: Computes fast and slow exponential moving averages from the live trade stream. Generates a buy signal when fast crosses above slow, sell signal when it crosses below.
- **Warm-up period**: Waits for `slow_period` trades before generating any signals to let EMAs converge.
- **Position targeting**: On buy signal, targets `max_position` shares long. On sell signal, targets flat (0 shares). Places orders incrementally up to `quantity` per signal.
- **Limit orders with offset**: Places limit orders at `last_trade_price +/- price_offset` rather than market orders, avoiding adverse fills in thin books.
- **Order expiry**: Cancels unfilled orders after `order_timeout` to avoid stale resting orders.
- **Cancel-on-signal**: Cancels all outstanding orders before acting on a new signal to avoid conflicting positions.
- **Bootstrapping**: Same as other traders — deposits initial cash and credits shares on first startup.
- **Graceful shutdown**: Cancels all outstanding orders on SIGTERM/SIGINT.

## Development

```sh
buf generate              # regenerate protobuf Go code
go build ./...            # compile everything
go test ./...             # run all tests (memstore-backed, no Postgres required)
```
