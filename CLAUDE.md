# xray

## Commands

```sh
buf generate              # regenerate protobuf Go code
go build ./...            # compile everything
go test ./...             # run all tests (memstore-backed, no Postgres required)

cd webapp && npm install  # install web UI dependencies
cd webapp && buf generate # regenerate protobuf TypeScript code
cd webapp && npm run dev  # start RR dev server on :5174 (proxies API to :8080)
cd webapp && npm run build # production build
```

The webapp (React Router v7 framework mode) runs as a separate process
from the Go server; it is not embedded into the Go binary. Run both for
the full UI experience.

## Conventions

- **Prices**: `int64` with 4 implied decimal places (e.g., `$150.50` = `1505000`)
- **Quantities**: `int64` whole units
- **Aggregate IDs**: Prefixed, e.g., `orderbook:AAPL`
- **Event serialization**: Protobuf (`proto.Marshal` / `proto.Unmarshal`), stored as `BYTEA` in Postgres
- **Code generation**: `buf generate` reads `buf.yaml` and `buf.gen.yaml`, outputs to `gen/`. `cd webapp && buf generate` outputs TypeScript to `webapp/src/gen/`. Do not edit generated files.
- **Testing**: Use `testify` for assertions
