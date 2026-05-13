# xray

Event-sourced order book — a learning project implementing a simple but realistic stock exchange order book with buy/sell limit orders, partial and complete fills. Uses event sourcing with protobuf serialization.

## Architecture

- `proto/` — Protobuf definitions (source of truth for event schemas)
- `gen/` — Generated Go code from protobuf (do not edit)
- `pkg/es/` — Reusable event sourcing framework (registry, aggregates, store interface, command handler)
- `pkg/es/memstore/` — In-memory EventStore for tests
- `pkg/es/pgstore/` — PostgreSQL EventStore (pgxpool)
- `internal/orderbook/` — Order book domain (aggregate, commands, matching engine)
- `cmd/xray/` — Entry point

## Code generation

Protobuf code generation uses `buf`. Run from the repo root:

```sh
buf generate
```

This reads `buf.yaml` and `buf.gen.yaml`, compiles `proto/orderbook/v1/events.proto`, and outputs generated Go code to `gen/orderbook/v1/`.

## Key design decisions

- **Prices**: `int64` with 4 implied decimal places (e.g., `$150.50` = `1505000`)
- **Quantities**: `int64` whole units
- **Aggregate IDs**: Prefixed, e.g., `orderbook:AAPL`
- **Event serialization**: Protobuf (`proto.Marshal` / `proto.Unmarshal`), stored as `BYTEA` in Postgres
- **Matching**: Inline during `PlaceOrder` — produces `OrderPlaced` + `TradeExecuted` events atomically
- **Testing**: Use `testify` for assertions

## Commands

```sh
buf generate              # regenerate protobuf Go code
go build ./...            # compile everything
go test ./...             # run all tests (memstore-backed, no Postgres required)
```
