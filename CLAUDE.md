# xray

## Commands

```sh
buf generate              # regenerate protobuf Go code
go build ./...            # compile everything
go test ./...             # run all tests (memstore-backed, no Postgres required)

cd web && npm install     # install web UI dependencies
cd web && buf generate    # regenerate protobuf TypeScript code
cd web && npm run dev     # start Vite dev server (proxies API to :8080)
cd web && npm run build   # production build (output in web/dist/, embedded by Go)
```

## Conventions

- **Prices**: `int64` with 4 implied decimal places (e.g., `$150.50` = `1505000`)
- **Quantities**: `int64` whole units
- **Aggregate IDs**: Prefixed, e.g., `orderbook:AAPL`
- **Event serialization**: Protobuf (`proto.Marshal` / `proto.Unmarshal`), stored as `BYTEA` in Postgres
- **Code generation**: `buf generate` reads `buf.yaml` and `buf.gen.yaml`, outputs to `gen/`. `cd web && buf generate` outputs TypeScript to `web/src/gen/`. Do not edit generated files.
- **Testing**: Use `testify` for assertions
