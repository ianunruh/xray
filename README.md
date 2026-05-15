# xray

Event-sourced order book — a learning project implementing a simple but realistic stock exchange order book with buy/sell limit orders, partial and complete fills. Uses event sourcing with protobuf serialization.

## Architecture

- `proto/` — Protobuf definitions (source of truth for event and service schemas)
- `gen/` — Generated Go code from protobuf (do not edit)
- `gen/orderbook/v1/orderbookv1connect/` — Generated Connect service handler and client
- `pkg/es/` — Reusable event sourcing framework (registry, aggregates, store interface, command handler)
- `pkg/es/memstore/` — In-memory EventStore for tests
- `pkg/es/pgstore/` — PostgreSQL EventStore (pgxpool)
- `internal/orderbook/` — Order book domain (aggregate, commands, matching engine, gRPC server)
- `internal/portfolio/` — Portfolio domain (cash/share tracking, order sagas)
- `internal/ordersaga/` — Order saga reactor (portfolio-aware order orchestration)
- `internal/bracket/` — Bracket order saga (entry + take-profit/stop-loss exits)
- `internal/mm/` — Market maker (strategy, engine, price sources)
- `cmd/xray/` — HTTP/gRPC server entry point
- `cmd/xray-mm/` — Market maker binary

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

**OrderBookService:**

- `PlaceOrder` — Place a limit order, returns order ID and any immediate trades
- `CancelOrder` — Cancel an existing order by ID
- `GetOrderBook` — Get full book state (bids and asks) for a symbol
- `GetOrder` — Look up a single order by symbol and order ID

**PortfolioService:**

- `Deposit` — Deposit cash into a portfolio account
- `Withdraw` — Withdraw cash from a portfolio account
- `CreditShares` — Credit shares to a portfolio account (for bootstrapping, e.g., market maker inventory)
- `GetPortfolio` — Get portfolio state (cash balance, holdings, pending orders)
- `PlaceOrder` — Place an order through the portfolio (holds cash, places order, settles trades)
- `GetOrderStatus` — Check the status of a portfolio order saga

**SagaService:**

- `PlaceBracketOrder` — Place an entry order with automatic take-profit and stop-loss exits
- `GetSaga` — Look up a saga by ID (status, order IDs, prices)

### Example usage with buf curl

```sh
# Place a sell order
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/PlaceOrder \
  -d '{"symbol":"AAPL","side":"SIDE_SELL","price":"1505000","quantity":"100"}'

# Place a buy order
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/PlaceOrder \
  -d '{"symbol":"AAPL","side":"SIDE_BUY","price":"1490000","quantity":"50"}'

# Get the order book
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/GetOrderBook \
  -d '{"symbol":"AAPL"}'

# Get the market depth
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.OrderBookService/GetMarketDepth \
  -d '{"symbol":"AAPL"}'

# Place a bracket order (entry buy at $150, take-profit at $155, stop-loss at $145)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.SagaService/PlaceBracketOrder \
  -d '{"symbol":"AAPL","side":"SIDE_BUY","price":"1500000","quantity":"100","take_profit_price":"1550000","stop_loss_price":"1450000"}'

# Check saga status
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/orderbook.v1.SagaService/GetSaga \
  -d '{"saga_id":"<saga_id from above>"}'

# Deposit cash into a portfolio
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/Deposit \
  -d '{"account_id":"acct-1","amount":"500000000"}'

# Place an order through the portfolio (holds cash, places in order book, settles fills)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/PlaceOrder \
  -d '{"account_id":"acct-1","symbol":"AAPL","side":"SIDE_BUY","price":"1500000","quantity":"100","order_type":"ORDER_TYPE_LIMIT","time_in_force":"TIME_IN_FORCE_GTC"}'

# Check order status (use saga_id from PlaceOrder response)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/GetOrderStatus \
  -d '{"saga_id":"<saga_id from above>"}'

# Get portfolio (balance, holdings, pending orders)
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/GetPortfolio \
  -d '{"account_id":"acct-1"}'

# Withdraw cash
buf curl --protocol grpc --http2-prior-knowledge \
  --schema proto http://localhost:8080/portfolio.v1.PortfolioService/Withdraw \
  -d '{"account_id":"acct-1","amount":"1000000"}'
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

## Development

```sh
buf generate              # regenerate protobuf Go code
go build ./...            # compile everything
go test ./...             # run all tests (memstore-backed, no Postgres required)
```
