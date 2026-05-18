# xray

Event-sourced order book — a learning project implementing a simple but realistic
stock exchange with limit/market/stop/trailing-stop/auction orders, iceberg
display sizing, brackets, OCO and TWAP execution sagas, short selling on margin
with auto-liquidation, periodic interest and borrow-fee accrual, opening/closing
call auctions with crossing prints, plus a live web UI with replay/scrub,
diagnostics, and a causation-chain viewer. Uses event sourcing with protobuf
serialization throughout.

![Web UI](docs/webui.png)

## Architecture

xray is an event-sourced CQRS system. Aggregates own state and emit events;
events fan out via NATS to reactors (which issue follow-up commands),
projections (which build read models), and brokers (which push live updates to
subscribers).

```mermaid
flowchart TB
    Client(["Client / Web UI / Strategies"])

    subgraph svc["RPC services"]
        OBSvc["OrderBookService"]
        PortSvc["PortfolioService"]
        SagaSvc["SagaService"]
        DiagSvc["DiagnosticsService"]
    end

    subgraph agg["Aggregates · write-side"]
        OrderBook["OrderBook"]
        Portfolio["Portfolio"]
        OrderSaga["OrderSaga"]
        BracketSaga["BracketSaga"]
        OCOSaga["OCOSaga"]
        TWAPSaga["TWAPSaga"]
    end

    EventStore[("Postgres event log")]
    NATS{{"NATS JetStream"}}

    subgraph rct["Reactors · drive sagas / liquidations / fees"]
        OSR["OrderSaga Reactor"]
        BR["Bracket Reactor"]
        OCR["OCOSaga Reactor"]
        TWR["TWAPSaga Reactor"]
        MCR["MarginCall Reactor"]
        FA["Fees Accruer"]
    end

    subgraph prj["Projections · read-side"]
        PgProj["PG: trades, orders, portfolio,<br/>pnl, sagas, shorts, longs,<br/>margin calls, daily close"]
        MemProj["In-memory: depth, candles,<br/>marks, active calls"]
    end

    subgraph brk["Brokers · live push"]
        OBB["OrderBook Broker"]
        PB["Portfolio Broker"]
    end

    Reconciler["Periodic Reconciler"]

    Client --> svc
    svc ==>|commands| agg
    agg -.->|events| EventStore
    EventStore -.-> NATS
    NATS -.-> rct
    NATS -.-> prj
    NATS -.-> brk
    rct ==>|commands| agg
    prj -.->|reads| svc
    brk -.->|streams| Client
    Reconciler -.->|active sagas /<br/>open margin calls| prj
    Reconciler ==>|Reconcile / ReplayTrade /<br/>tick TWAP / grace expiry| rct

    classDef agg fill:#e8f4ff,stroke:#0066cc,color:#000
    classDef svc fill:#e8ffe8,stroke:#009900,color:#000
    classDef rct fill:#fff4e8,stroke:#cc6600,color:#000
    classDef prj fill:#f4e8ff,stroke:#6600cc,color:#000
    classDef brk fill:#ffe8f4,stroke:#cc0066,color:#000
    classDef infra fill:#f0f0f0,stroke:#333,color:#000

    class OrderBook,Portfolio,OrderSaga,BracketSaga,OCOSaga,TWAPSaga agg
    class OBSvc,PortSvc,SagaSvc,DiagSvc svc
    class OSR,BR,OCR,TWR,MCR,FA rct
    class PgProj,MemProj prj
    class OBB,PB brk
    class EventStore,NATS,Reconciler infra
```

**Legend:** thick arrows (`==>`) = synchronous commands; thin solid (`-->`) = RPC requests; dashed (`-.->`) = async events or read queries.

**Where the reactor command loops close:**

| Reactor | Owns aggregate | Commands portfolio | Commands orderbook | Spawns child saga |
|---|---|---|---|---|
| OrderSaga | OrderSaga (RecordCashHeld, RecordSharesHeld, RecordCollateralHeld, RecordOrderPlaced, RecordFill, RecordCompleted) | HoldCash / SettleTrade (buys); HoldShares / SettleSale (sells); HoldCollateral / OpenShort (sell-to-open); HoldShortCover / CoverShort (buy-to-cover); Release on cancel | PlaceOrder / CancelOrder | — |
| Bracket  | BracketSaga (RecordEntryFilled, RecordExitFilled, RecordSagaFailed) | — | — | OrderSaga (entry); OCOSaga (exit) |
| OCOSaga  | OCOSaga (RecordSharesHeld, RecordExitPlaced, RecordFill, RecordCompleted) | HoldShares / SettleSale (long exits); HoldShortCover / CoverShort (short covers); ReleaseShares on cancel | PlaceOrder (TP + SL with shared OCO group); CancelOrder | — |
| TWAPSaga | TWAPSaga (RecordSliceLaunched, RecordSliceCompleted, RecordSagaCompleted) | — | — | OrderSaga (per slice) |
| MarginCall | (no aggregate; pure orchestrator) | IssueMarginCall / CoverMarginCall | — | OrderSaga (forced liquidation; `initiator=MARGIN_CALL`) |

In-memory projections (depth, candles, marks, active margin calls) rebuild on
every boot. PG-backed projections and saga reactors each have their own NATS
durable cursor, so they advance independently and don't replay from event 0
across restarts.

### Saga compositions

**Bracket** — the bracket aggregate is a thin orchestrator that spawns an entry
`OrderSaga`, observes its completion, then spawns an exit `OCOSaga`. The bracket
reactor never touches the portfolio or the orderbook directly — all of that
lives in the two child sagas.

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

If the user cancels mid-flight, the bracket reactor fails the affected child
saga (the entry ordersaga during `PendingEntry`, the exit OCOSaga during
`PendingExit`); each child cleans up its own portfolio holds, and the bracket
observes the resulting failure event to mark itself `Failed`.

**TWAP** — slices one parent order into N child `OrderSaga`s spaced over a time
window. Each slice is a marketable-limit IOC at the user-supplied `limit_price`;
underfilled qty rolls forward into the next slice so the saga stays on-pace.
The TWAP reactor advances on child completion events; the periodic reconciler
is the backstop that launches slices whose `dueAt` passed during a wakeup gap
(e.g., process restart).

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant Svc as SagaService
    participant T as TWAPSaga
    participant TR as TWAP Reactor
    participant Rec as Reconciler
    participant O as OrderSaga (slice N)
    participant OSR as OrderSaga Reactor

    User->>Svc: Place(TWAPPlan)
    Svc->>T: StartTWAPSaga
    T-->>TR: TWAPSagaStarted
    TR->>O: StartOrderSaga (slice 0)

    Note over O,OSR: child runs full ordersaga cycle:<br/>HoldCash → PlaceOrder (IOC) →<br/>TradeExecuted → SettleTrade → Completed

    O-->>TR: OrderSagaCompleted
    TR->>T: RecordSliceCompleted (slice 0, filled_qty, cash_settled)

    Note over Rec: tick — slice 1 not yet due, no-op
    Rec->>TR: Reconcile(twap_id)

    Note over Rec: tick — slice 1 dueAt has passed
    Rec->>TR: Reconcile(twap_id)
    TR->>O: StartOrderSaga (slice 1, qty = planned + rollover)
    O-->>TR: OrderSagaCompleted
    TR->>T: RecordSliceCompleted (slice 1)

    Note over TR,T: ...repeat through slice N-1...
    TR->>T: RecordSagaCompleted
```

Cancellation sets the parent to `Failed` first (suppressing future slices) then
best-effort cancels the in-flight child's orderbook order. Already-completed
slices stay settled — that's the point of incremental execution.

**Margin-call liquidation** — when the portfolio's equity drops below the
maintenance requirement (mark moves against open shorts or longs), the
margin-call reactor emits `MarginCallIssued`, waits out a grace period, then
spawns one or more forced-liquidation `OrderSaga`s with `initiator=MARGIN_CALL`.
Those liquidation sagas have `status=LIQUIDATED` in the projection (distinct
from `FAILED`) so the UI and audit log can attribute them correctly.

### Package layout

- `proto/` — protobuf definitions (source of truth for events and services)
- `gen/` — generated Go code from protobuf (do not edit)
- `pkg/es/` — reusable event sourcing framework: aggregates, registry, store
  interface, command handler, snapshots, projection consumer, correlation/causation
  tracking, time-travel helpers (`LoadRange`, `VersionAtTimestamp`)
- `pkg/es/memstore/` — in-memory EventStore for tests
- `pkg/es/pgstore/` — PostgreSQL EventStore + migrations
- `pkg/es/natsstore/` — NATS publisher + projection consumer with per-name durable cursors
- `internal/orderbook/` — OrderBook aggregate, matching engine, opening/closing
  auction uncross, order/trade/depth/candle/mark/daily-close projections, broker, RPC server
- `internal/portfolio/` — Portfolio aggregate (cash, longs, shorts, margin loans,
  collateral, fees) with per-saga and per-trade idempotency; PnL/shorts/longs/margin-call
  projections; broker; RPC server
- `internal/ordersaga/` — OrderSaga aggregate + reactor (one order, full portfolio
  coordination across all four side/position combinations)
- `internal/bracket/` — BracketSaga aggregate + reactor (entry ordersaga + exit OCOSaga orchestration)
- `internal/ocosaga/` — OCOSaga aggregate + reactor (shared share/cover hold; TP+SL; settle whichever wins)
- `internal/twapsaga/` — TWAPSaga aggregate + reactor (N child OrderSagas over a time window, with rollover)
- `internal/margincall/` — Reactor that watches marks and emits liquidation sagas when equity breaches maintenance
- `internal/margin/` — Policy constants (Reg-T initial/maintenance margins, financing rates) and shared helpers
- `internal/feesaccruer/` — Periodic ticker that charges margin interest and short borrow fees idempotently
- `internal/reconciler/` — Periodic reconciler: stuck sagas (L1), unsettled trades (L2),
  TWAP slice scheduling, margin-call grace expiry
- `internal/sagasvc/` — Unified `saga.v1.SagaService` (Place/Get/Cancel/List) + cross-kind projection
- `internal/diagnostics/` — Diagnostics RPCs (event log inspection, causation chain traversal)
- `internal/pricesource/` — Price source interface + implementations (Polygon HTTP, static)
- `internal/trader/` — Shared strategy utilities (bootstrap, order tracking, trade streaming)
- `internal/mm/`, `internal/noise/`, `internal/trend/` — Strategy engines
- `cmd/xray/` — HTTP/gRPC server entry point (registers all services, projections, reactors, reconciler, fees accruer)
- `cmd/xray-mm/`, `cmd/xray-noise/`, `cmd/xray-trend/` — Strategy client binaries
- `cmd/loadtest/` — Synthetic-traffic generator for orderbook benchmarking
- `web/` — React + Mantine front-end (Vite); embedded into the Go binary at build time

## Key design decisions

- **Prices**: `int64` with 4 implied decimal places (e.g., `$150.50` = `1505000`)
- **Quantities**: `int64` whole units
- **Aggregate IDs**: Prefixed, e.g., `orderbook:AAPL`, `order-saga:<uuid>`, `bracket-saga:<uuid>`, `oco-saga:<uuid>`, `twap-saga:<uuid>`. Child sagas use further-namespaced IDs (`bracket-entry:`, `bracket-oco:`, `twap-slice:<twap_id>:<idx>`) so a single prefix filter hides them from user-facing `List` responses.
- **Event serialization**: Protobuf (`proto.Marshal` / `proto.Unmarshal`), stored as `BYTEA` in Postgres
- **Matching**: Inline during `PlaceOrder` — produces `OrderPlaced` + `TradeExecuted` events atomically. Auctions accumulate orders without crossing; `Uncross` runs an equilibrium-price batch match.
- **Idempotency**: Per-saga (holds keyed by `order_saga_id`) and per-trade (settlements keyed by `(saga_id, trade_id)`), so replays from any cursor are safe.
- **Correlation / causation**: Every command attaches a causation chain; the diagnostics service can walk it to render the full event graph for any correlation ID.
- **API**: Connect (connectrpc.com/connect) — single server speaks gRPC, gRPC-Web, and Connect (JSON-over-HTTP)

## Running the server

```sh
docker compose up -d      # start Postgres + NATS JetStream
go run ./cmd/xray         # starts HTTP/gRPC server on :8080, serves web UI at /
```

Open <http://localhost:8080> in a browser for the web UI.

Environment variables:

- `DATABASE_URL` — Postgres connection string (default: `postgres://xray:xray@localhost:5432/xray?sslmode=disable`)
- `NATS_URL` — NATS connection string (default: `nats://127.0.0.1:4222`)
- `LISTEN_ADDR` — Listen address (default: `:8080`)
- `LOG_LEVEL` — `debug` / `info` / `warn` / `error` (default: `info`)

## API surface

The server exposes RPCs via Connect (gRPC, gRPC-Web, and JSON-over-HTTP on the same port):

### `saga.v1.SagaService` — unified entry point for all order types

- `Place(PlaceSagaRequest{account_id, plan})` — `plan` is a oneof: `single_order`, `bracket`, `oco`, or `twap`
- `Get(saga_id)` — returns status, kind, and kind-specific details
- `Cancel(saga_id)` — kind-aware; releases any holds, cancels resting orderbook orders, stops future TWAP slices
- `List({account_id, symbol, kind, status})` — all filters optional; hides child sagas spawned by brackets and TWAPs

### `portfolio.v1.PortfolioService` — account state, P&L, margin

- `Deposit` / `Withdraw` / `CreditShares` — manipulate cash and shares directly (bootstrap and admin)
- `GetPortfolio` / `StreamPortfolio` — cash balance, holdings, shorts, pending sagas (point-in-time and server-streamed)
- `GetPnL` — per-position realized + unrealized P&L
- `GetMarginSnapshot` — equity, maintenance requirement, buying power, margin loan, short-borrow exposure
- `PreviewOrderImpact` — what an order would do to buying power / margin / position before placing it (powers the OrderForm impact panel)
- `ListMarginCalls` — historical and active margin calls with snapshot data
- `ListPortfolios` — enumerate all known accounts

### `orderbook.v1.OrderBookService` — direct orderbook access, auctions, replay

- `PlaceOrder` / `CancelOrder` / `ReplaceOrder` — direct order placement, bypasses portfolio
- `CloseMarket` — end-of-day mass-cancel for a symbol
- `OpenAuction` / `BeginClosingAuction` / `Uncross` — drive a symbol through the auction lifecycle
- `GetOfficialClose` / `ListOfficialCloses` — canonical session-end prices
- `GetOrderBook` / `GetMarketDepth` / `StreamMarketDepth` — book state (full and aggregated)
- `GetOrder` / `ListOrders` — single-order and per-symbol order queries
- `ListTrades` / `StreamTrades` — trade history and live stream
- `GetCandles` / `StreamCandles` — OHLC bars
- `ListSymbols` — symbol catalog
- `GetReplayBounds` / `ReplayOrderBook` — time-travel: query first/last version+timestamp of an aggregate, then load its state at any past version (powers the UI scrubber)

### `diagnostics.v1.DiagnosticsService` — event-log inspection

- `ListAggregates` — paginated catalog of every aggregate in the event store (type, event count, timestamp bounds)
- `GetAggregateEvents` — full event history for one aggregate, with each event's data JSON-decoded
- `GetEventChain` — every event sharing a `correlation_id`, ordered by causation, for tracing how one user action fanned out across the system

## Order types and time-in-force

Order types: `LIMIT`, `MARKET`, `STOP_MARKET` (triggers as market when `trigger_price` is touched), `STOP_LIMIT` (triggers as limit), `TRAILING_STOP_MARKET` / `TRAILING_STOP_LIMIT` (stop price ratchets with the mark — see below). Stop triggers emit a `StopTriggered` event before matching.

Time-in-force: `GTC`, `IOC`, `FOK`, `DAY`, plus `AT_OPEN` / `AT_CLOSE` which bind an order to a specific auction cross. `AT_OPEN` + `MARKET` = MOO, `AT_OPEN` + `LIMIT` = LOO, etc. Unfilled at uncross → cancelled with reason `missed_auction`.

OCO group: any `OrderPlaced` may carry an `oco_group_id`. The first order in the group to trade *any* qty atomically cancels every other order in the group before further matching or stop-trigger passes.

### Iceberg orders

Any `LIMIT` + `GTC`/`DAY` order may carry a `display_quantity > 0`. Only that slice is exposed to the matching engine (and to depth) at a time; the rest is hidden reserve. When the displayed slice fully fills, the engine emits `IcebergSliceReplenished` and re-inserts the order at the back of its price level — the new slice loses time priority to anyone already resting at that level (standard NASDAQ behavior). The portfolio hold still covers the full quantity. Final slice shrinks to whatever's left in reserve.

### Trailing stops

`TRAILING_STOP_MARKET` / `TRAILING_STOP_LIMIT` take an initial `stop_price` plus exactly one of `trail_amount` (absolute, price units) or `trail_offset_bps` (basis points of mark). After every continuous trade the engine ratchets each trailing stop's trigger *tighter* on favorable mark moves — SELL stops rise as the mark rises, BUY stops fall as the mark falls — emitting `TrailingStopAdjusted` so replay reproduces the ratchet path exactly. The stop never widens on adverse moves. `TRAILING_STOP_LIMIT` additionally carries a `limit_offset`: when the stop fires, the activated limit price is `stop ± limit_offset` (below stop for sells, above for buys).

## Market phases and auctions

Symbols default to `CONTINUOUS` (standard price-time priority matching). Calling `OpenAuction(symbol)` or `BeginClosingAuction(symbol)` transitions to an `AUCTION` / `CLOSING_AUCTION` phase, where incoming orders accumulate without crossing. `Uncross(symbol)` runs an equilibrium-price algorithm:

1. Emits `AuctionUncrossed` (header event) with the clearing price, matched qty, and remaining imbalance side+qty
2. Emits a batch of `TradeExecuted` events at the clearing price, each tagged with `cross_type=OPENING` or `CLOSING`
3. For closing auctions, emits `OfficialCloseSet` (the canonical end-of-day mark for the session)
4. Flips the symbol back to `CONTINUOUS` (or to `CLOSED`, after a closing auction)

`CloseMarket(symbol)` mass-cancels everything resting on the book — useful for end-of-day cleanup or `DAY` order expiry.

## Margin, shorts, and auto-liquidation

The portfolio supports four hold types: cash (for `BUY+LONG`), shares (for `SELL+LONG`), collateral (for `SELL+SHORT`, 50% additional margin), and short-cover capacity (for `BUY+SHORT`). Settlement events route proceeds and pay costs through dedicated short-position pools (`ProceedsPool` and `CollateralPool`), drawing on each pool in turn before falling back to cash.

The fees accruer ticks hourly (configurable) and emits one event per account per cycle:

- `MarginInterestAccrued` — charges interest on outstanding margin loan when `CashBalance < 0`. Rate: 8% APR by default (`MarginLoanRateBps`).
- `ShortBorrowFeeAccrued` — charges per-symbol borrow on every open short, priced at mark × qty. Rate: 3% APR by default (`ShortBorrowRateBps`).

Both events advance `Portfolio.LastAccruedAt`, so duplicate ticks at the same wall-clock are no-ops, missed windows are caught up in one tick, and replay is safe.

The margin-call reactor watches `TradeExecuted` (mark updates), `OfficialCloseSet` (session-end mark), and `ShortOpened` / `ShortCovered`. When portfolio equity falls below the maintenance requirement, it emits `MarginCallIssued` with a frozen snapshot (equity, requirement, trigger symbol/mark, grace expiry). After the grace period (default 30s), the reconciler invokes `EvaluateGraceExpiry` and the reactor spawns a forced-liquidation `OrderSaga` for the largest open position. On each subsequent tick while still breached it spawns another liquidation until equity is back above the requirement, at which point it emits `MarginCallCovered`. Liquidation saga IDs are deterministic (`liquidation:{account_id}:{trigger_id}`) so replays are idempotent.

## Replay / time-travel

The orderbook supports point-in-time queries via two RPCs:

- `GetReplayBounds(aggregate_id)` — returns the first/last version+timestamp of any aggregate, plus its current market phase
- `ReplayOrderBook(aggregate_id, version|timestamp)` — rebuilds the orderbook's state at a given past version

The web UI's `ReplayControls` component uses these to drive a time scrubber: pick any past instant and the market depth, candle chart, and trade table all re-render against the historical state.

## Diagnostics and chain viewer

The web UI ships two diagnostic views:

- **Diagnostics** — browse every aggregate in the event store, drill into one to see its full event history with JSON-decoded payloads
- **Chain** — paste any `correlation_id` (or click ⇢ from a saga row) to see the full fan-out of events that descended from a single user action, ordered by causation. Useful for "why did this liquidation fire?" sleuthing.

Both are powered by `DiagnosticsService`.

## Example usage with buf curl

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

# Margin snapshot — equity, maintenance requirement, buying power
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/GetMarginSnapshot \
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
      "time_in_force":"TIME_IN_FORCE_GTC",
      "position_side":"POSITION_SIDE_LONG"
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
      "time_in_force":"TIME_IN_FORCE_IOC",
      "position_side":"POSITION_SIDE_LONG"
    }
  }'

# Sell-to-open (open a short) — 50% collateral held in addition to the
# proceeds the sale will produce. Server validates margin headroom.
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "single_order":{
      "symbol":"AAPL",
      "side":"SIDE_SELL",
      "price":"1500000",
      "quantity":"100",
      "order_type":"ORDER_TYPE_LIMIT",
      "time_in_force":"TIME_IN_FORCE_GTC",
      "position_side":"POSITION_SIDE_SHORT"
    }
  }'

# Iceberg SELL 1000 @ $150, only 100 shown at a time. Each slice that
# fills replenishes from the hidden reserve at the back of the queue.
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "single_order":{
      "symbol":"AAPL",
      "side":"SIDE_SELL",
      "price":"1500000",
      "quantity":"1000",
      "display_quantity":"100",
      "order_type":"ORDER_TYPE_LIMIT",
      "time_in_force":"TIME_IN_FORCE_GTC",
      "position_side":"POSITION_SIDE_LONG"
    }
  }'

# Trailing-stop SELL on a long position: initial stop $149, trails the
# mark by $1. As price rises the stop ratchets up; it never widens.
# Fires as a market IOC when the mark touches the ratcheted stop.
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "single_order":{
      "symbol":"AAPL",
      "side":"SIDE_SELL",
      "quantity":"100",
      "stop_price":"1490000",
      "trail_amount":"10000",
      "order_type":"ORDER_TYPE_TRAILING_STOP_MARKET",
      "time_in_force":"TIME_IN_FORCE_GTC",
      "position_side":"POSITION_SIDE_LONG"
    }
  }'

# Trailing-stop-limit SELL using a 50bps trail and a $0.50 limit offset.
# When triggered, places a limit at (stop - $0.50).
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "single_order":{
      "symbol":"AAPL",
      "side":"SIDE_SELL",
      "quantity":"100",
      "stop_price":"1490000",
      "trail_offset_bps":50,
      "limit_offset":"5000",
      "order_type":"ORDER_TYPE_TRAILING_STOP_LIMIT",
      "time_in_force":"TIME_IN_FORCE_GTC",
      "position_side":"POSITION_SIDE_LONG"
    }
  }'

# --- Bracket: BUY 100 @ $150, TP $155, SL $145 ----------------------------

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
      "stop_loss_price":"1450000",
      "position_side":"POSITION_SIDE_LONG"
    }
  }'

# --- OCO: exit existing 100 AAPL at $155 TP or $145 SL --------------------

buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "oco":{
      "symbol":"AAPL",
      "exit_side":"SIDE_SELL",
      "quantity":"100",
      "take_profit_price":"1550000",
      "stop_loss_price":"1450000",
      "position_side":"POSITION_SIDE_LONG"
    }
  }'

# --- TWAP: buy 1000 shares over 5 slices, 30s apart, capped at $150.50 ----
# Each slice is a marketable-limit IOC at limit_price; underfilled qty
# rolls into the next slice. Total ~2.5 min window.
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Place \
  -d '{
    "account_id":"acct-1",
    "twap":{
      "symbol":"AAPL",
      "side":"SIDE_BUY",
      "position_side":"POSITION_SIDE_LONG",
      "total_quantity":"1000",
      "slice_count":5,
      "slice_interval_ms":"30000",
      "limit_price":"1505000"
    }
  }'

# --- Inspect ---------------------------------------------------------------

# Get a saga's current state + details (works for any kind, including TWAP)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Get \
  -d '{"saga_id":"<saga_id from Place>"}'

# List active sagas for an account (filters all optional)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/List \
  -d '{"account_id":"acct-1","status":"SAGA_STATUS_ACTIVE"}'

# Just the TWAPs
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/List \
  -d '{"account_id":"acct-1","kind":"SAGA_KIND_TWAP","status":"SAGA_STATUS_ACTIVE"}'

# Active margin calls
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/ListMarginCalls \
  -d '{"account_id":"acct-1","only_active":true}'

# --- Cancel ---------------------------------------------------------------

# Cancel a saga (kind-aware — handles single, bracket, OCO, TWAP appropriately)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/saga.v1.SagaService/Cancel \
  -d '{"saga_id":"<saga_id>"}'

# --- Auction lifecycle ---------------------------------------------------

# Open the AAPL auction (accumulates orders, no crossing)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/OpenAuction \
  -d '{"symbol":"AAPL","reason":"session_open"}'

# Drop a few MOO/LOO orders with TIME_IN_FORCE_AT_OPEN...

# Run the cross — emits AuctionUncrossed + batch of TradeExecuted with cross_type=OPENING
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/Uncross \
  -d '{"symbol":"AAPL"}'

# --- Replay / time-travel ------------------------------------------------

# Bounds for an aggregate (first/last version + timestamp)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/GetReplayBounds \
  -d '{"aggregate_id":"orderbook:AAPL"}'

# Rebuild orderbook state at a past version
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/ReplayOrderBook \
  -d '{"aggregate_id":"orderbook:AAPL","version":"12345"}'

# --- Diagnostics --------------------------------------------------------

# Browse every aggregate in the event store
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/diagnostics.v1.DiagnosticsService/ListAggregates \
  -d '{"limit":"50"}'

# Full event history for one aggregate
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/diagnostics.v1.DiagnosticsService/GetAggregateEvents \
  -d '{"aggregate_id":"order-saga:<uuid>"}'

# Trace a user action's fan-out via correlation_id (copy from any saga's event log)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/diagnostics.v1.DiagnosticsService/GetEventChain \
  -d '{"correlation_id":"<uuid>"}'
```

## Market Maker

The `xray-mm` binary is a spread-based market maker that quotes bids and asks
around a reference price from an external source (Polygon-compatible), with one
account per symbol. Inventory-aware quoting skews the mid price based on the
current position so it self-balances.

```sh
go run ./cmd/xray-mm -config mm.yaml
POLYGON_API_KEY=your-key go run ./cmd/xray-mm -config mm.yaml
```

```yaml
server_url: "http://localhost:8080"
polygon_api_key: "key"              # or set POLYGON_API_KEY env var
log_level: "info"
price_source: "polygon"             # "polygon" or "static"

symbols:
  - symbol: AAPL
    account_id: mm-AAPL
    initial_deposit: 100000000000   # $10M — deposited on first startup
    initial_shares: 10000           # credited on first startup for sell-side quotes
    spread: 30000                   # $3.00 total spread
    quantity: 20                    # shares per level
    levels: 3                       # number of bid/ask levels
    level_spacing: 10000            # $1.00 between levels (defaults to spread)
    max_position: 30000             # hard inventory limit per side
    requote_interval: 30s           # timer-based requote backstop
    price_move_threshold: 20000     # $2.00 — requote on ref price move
    max_skew: 10000                 # $1.00 mid shift at |position| == max_position

polygon:
  base_url: "https://api.massive.com"
  poll_interval: 60s
```

**How it works:** hybrid requoting (timer + on-fill + on-ref-price-move),
portfolio-aware orders through the saga flow for real P&L, inventory skew that
biases the mid price linearly with position, hard inventory limits, orphan
order cleanup on startup, graceful cancel-all on SIGTERM/SIGINT.

See [docs/plans/market-maker.md](docs/plans/market-maker.md) for the full design.

## Noise Trader

The `xray-noise` binary generates random retail-like order flow to create
volume and book depth alongside the market maker. It spawns N parallel
instances per symbol; each places a mix of market and limit orders at random
intervals.

```sh
go run ./cmd/xray-noise -config noise.yaml
```

```yaml
server_url: "http://localhost:8080"
price_source: "polygon"

symbols:
  - symbol: AAPL
    account_id: noise-AAPL
    instances: 20                   # parallel noise traders for this symbol
    initial_deposit: 5000000000     # $500K per instance
    initial_shares: 500             # per instance
    random_initial_shares: true     # credit uniform random in [0, initial_shares]
    order_interval: 3s
    min_quantity: 1
    max_quantity: 10
    price_jitter: 200000            # $20.00 — limit orders in [ref-$20, ref+$20]
    market_order_pct: 0.5           # 50% market orders, 50% limit
    max_position: 1000              # hard inventory limit per side
    buy_bias: 0.5                   # 0.0=always sell, 1.0=always buy, 0.5=neutral
```

## Trend Follower

The `xray-trend` binary is an EMA crossover trend follower. It streams live
trades from the order book, maintains fast/slow EMAs, and crosses the spread
slightly to get filled when a crossover fires.

```sh
go run ./cmd/xray-trend -config trend.yaml
```

```yaml
server_url: "http://localhost:8080"
price_source: "polygon"

symbols:
  - symbol: AAPL
    account_id: trend-AAPL
    initial_deposit: 5000000000     # $500K
    initial_shares: 500
    random_initial_shares: true
    fast_period: 10                 # trades for fast EMA
    slow_period: 30                 # trades for slow EMA
    quantity: 50                    # shares per order
    max_position: 500
    order_timeout: 30s              # cancel unfilled orders after this
    price_offset: 5000              # $0.50 — cross the spread slightly
```

## Loadtest

`cmd/loadtest` is a synthetic-traffic generator for orderbook benchmarking. It
runs configurable pools of place / cancel / depth-query / order-status workers
in parallel for a fixed duration and reports per-operation p50/p95/p99 latency
and throughput.

```sh
go run ./cmd/loadtest \
  -addr http://localhost:8080 \
  -duration 30s \
  -symbols AAPL,GOOG,MSFT \
  -place-workers 4 \
  -cancel-workers 2 \
  -depth-workers 2
```

## Web UI

Vite + React + Mantine, embedded into the Go binary at build time. Three top-level views:

- **Trading** — market depth (DOM), candle chart, order form with live impact
  preview, portfolio panel (cash, holdings, shorts, margin metrics), tabs for
  Orders (resting orders, active brackets, OCOs, TWAPs with slice progress
  bars) and Positions (with close-position prefills)
- **Diagnostics** — paginated aggregate browser; per-aggregate event log with
  decoded JSON; ⇢ jump-to-chain shortcuts
- **Chain** — full causation chain for any correlation_id, ordered by event time

Across all views: live order-status notifications, audible chime on fills,
replay scrubber for time-travel through orderbook history, and a margin-call
banner when an account is in breach.

Build the front-end and run the dev server:

```sh
cd web && npm install
cd web && npm run dev    # Vite dev server, proxies API to :8080
cd web && npm run build  # production build (output in web/dist/, embedded by Go)
```

## Development

```sh
buf generate              # regenerate protobuf Go code
cd web && buf generate    # regenerate protobuf TypeScript code
go build ./...            # compile everything
go test ./...             # run all tests (memstore-backed, no Postgres required)
cd web && npm run build   # type-check and bundle the web UI
```
