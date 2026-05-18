# Per-Account Fee & Interest History

## Context

Three event types charge cash to a portfolio:

- `TransactionFeeCharged` — emitted alongside every trade settlement,
  one per (saga, trade, account). Cash debited at a fixed bps of
  notional.
- `MarginInterestAccrued` — emitted by the fees accruer per cycle on
  every account with an outstanding margin loan. Amount may be zero
  (the event still fires to advance the accrual clock).
- `ShortBorrowFeeAccrued` — emitted by the fees accruer per cycle, one
  per (account, symbol), only when amount > 0.

The per-order `fees_paid` column in the orders table shows the
transaction-fee total per order, but there is no per-account history
view. The events are queryable through `/events` filtered to
`portfolio:<account>` — that's a debug tool, not an account-holder
view. Margin interest and short borrow fees have no UI surface at all
beyond their effect on `CashBalance`.

This plan adds a PG-backed projection over the three event types, a
`ListFeeHistory` RPC, and a new `PortfolioFees` UI tab. The events are
already on the wire; nothing in the engine, sagas, or accruer changes.

Non-goals for v1: filters by symbol or kind (clientside-filterable
once the rows are in hand); fee-rate configuration UI (rates are
policy constants in `internal/margin/`); CSV export.

## Architecture

```
event flow                          new projection
────────────                        ─────────────────────────
TransactionFeeCharged    ─┐
MarginInterestAccrued    ─┼──► fees-history consumer ──► PG: projection_fees
ShortBorrowFeeAccrued    ─┘                              (one row per event)

read path
─────────
PortfolioService.ListFeeHistory
  ↳ PgFeesProjection.ListFeeHistory(account_id, limit)
  ↳ SELECT … WHERE account_id = $1 ORDER BY charged_at DESC LIMIT $2

webapp
──────
trading.tsx loader → portfolioClient.listFeeHistory({account, limit})
  ↳ Fees tab → PortfolioFees panel → Table
```

The projection is dedicated (its own `ProjectionConsumer` named
`fees-history`) rather than tacked onto the existing
`portfolio-projection` or `pnl-projection` consumers. Reasons: it
needs all three event types and none of the others touch
`MarginInterestAccrued` / `ShortBorrowFeeAccrued`, and isolating it
means a fees-projection error or rebuild doesn't block the other
read paths.

```
# Proto
proto/portfolio/v1/service.proto    # FeeKind enum, FeeRecord, ListFeeHistory rpc

# Storage
pkg/es/pgstore/migrations/000027.sql  # NEW projection_fees table

# Server
internal/portfolio/projection_fees.go      # NEW projection + reader
internal/portfolio/projection_fees_test.go # NEW unit tests against memstore
internal/portfolio/server.go               # ListFeeHistory handler
cmd/xray/main.go                           # wire projection + consumer + reader

# Webapp
webapp/app/routes/trading.tsx       # loader fetches fee history
webapp/app/components/PortfolioFees.tsx  # NEW table component
```

## PG schema

```sql
-- migrations/000027.sql
CREATE TABLE IF NOT EXISTS projection_fees (
    id           BIGSERIAL PRIMARY KEY,
    account_id   TEXT NOT NULL,
    kind         INT  NOT NULL,           -- 1=TXN, 2=MARGIN_INT, 3=SHORT_BORROW
    amount       BIGINT NOT NULL,
    symbol       TEXT,                    -- nullable (margin interest has no symbol)
    charged_at   TIMESTAMPTZ NOT NULL,
    related_id   TEXT,                    -- trade_id for TXN; null for accruals
    rate_bps     BIGINT,                  -- only for accruals
    notional     BIGINT,                  -- only for TXN
    period_start TIMESTAMPTZ              -- only for accruals
);

CREATE INDEX IF NOT EXISTS idx_fees_account_charged
    ON projection_fees(account_id, charged_at DESC);
```

No `ON CONFLICT` handling on insert — `MarginInterestAccrued`'s
zero-amount events DO get rows (so the UI can show "rate ticked, no
debit"); each event maps to exactly one row. Replay on rebuild
truncates the table first.

`related_id` is left text for forward-compat (could carry a saga ID,
trade ID, or symbol for short borrow); for now: trade_id for TXN,
null for accruals.

## Proto

```protobuf
enum FeeKind {
  FEE_KIND_UNSPECIFIED      = 0;
  FEE_KIND_TRANSACTION      = 1;
  FEE_KIND_MARGIN_INTEREST  = 2;
  FEE_KIND_SHORT_BORROW     = 3;
}

message FeeRecord {
  string                       account_id   = 1;
  FeeKind                      kind         = 2;
  int64                        amount       = 3;
  string                       symbol       = 4;   // empty for margin interest
  google.protobuf.Timestamp    charged_at   = 5;
  string                       related_id   = 6;   // trade_id for TXN
  int64                        rate_bps     = 7;   // accruals only
  int64                        notional     = 8;   // TXN only
  google.protobuf.Timestamp    period_start = 9;   // accruals only
}

message ListFeeHistoryRequest {
  string account_id = 1;
  // 0 = no cap; UI passes a reasonable default like 200.
  int32  limit      = 2;
}

message ListFeeHistoryResponse {
  repeated FeeRecord records = 1;
}

// Added to PortfolioService:
rpc ListFeeHistory(ListFeeHistoryRequest) returns (ListFeeHistoryResponse);
```

## Projection (`projection_fees.go`)

Implements `es.Projection` + `es.Resettable` so projection rebuilds
work via the existing diagnostics flow.

```go
type PgFeesProjection struct {
    pool *pgxpool.Pool
}

func NewPgFeesProjection(pool *pgxpool.Pool) *PgFeesProjection

func (p *PgFeesProjection) Reset(ctx context.Context) error {
    _, err := p.pool.Exec(ctx, `TRUNCATE projection_fees`)
    return err
}

func (p *PgFeesProjection) HandleEvents(ctx context.Context, events []es.Event) error {
    batch := &pgx.Batch{}
    for _, evt := range events {
        switch data := evt.Data.(type) {
        case *portfoliov1.TransactionFeeCharged:
            batch.Queue(
                `INSERT INTO projection_fees
                 (account_id, kind, amount, symbol, charged_at, related_id, rate_bps, notional)
                 VALUES ($1, 1, $2, $3, $4, $5, $6, $7)`,
                data.AccountId, data.Amount, data.Symbol,
                data.ChargedAt.AsTime(), data.TradeId, data.RateBps, data.Notional,
            )
        case *portfoliov1.MarginInterestAccrued:
            batch.Queue(
                `INSERT INTO projection_fees
                 (account_id, kind, amount, charged_at, rate_bps, period_start)
                 VALUES ($1, 2, $2, $3, $4, $5)`,
                data.AccountId, data.Amount, data.PeriodEnd.AsTime(),
                data.RateBps, data.PeriodStart.AsTime(),
            )
        case *portfoliov1.ShortBorrowFeeAccrued:
            batch.Queue(
                `INSERT INTO projection_fees
                 (account_id, kind, amount, symbol, charged_at, rate_bps, period_start)
                 VALUES ($1, 3, $2, $3, $4, $5, $6)`,
                data.AccountId, data.Amount, data.Symbol, data.PeriodEnd.AsTime(),
                data.RateBps, data.PeriodStart.AsTime(),
            )
        }
    }
    // Send batch, return on first error.
}

func (p *PgFeesProjection) ListFeeHistory(ctx context.Context, accountID string, limit int32) ([]*portfoliov1.FeeRecord, error)
```

`charged_at` uses `PeriodEnd` for accruals (the end of the cycle is
when the cash moved) and `ChargedAt` for transaction fees. The UI
shows it as a single sortable column.

## Server handler

```go
type FeesReader interface {
    ListFeeHistory(ctx context.Context, accountID string, limit int32) ([]*portfoliov1.FeeRecord, error)
}

// Server gains feesReader field, wired through NewServer.

func (s *Server) ListFeeHistory(ctx, req) (*ListFeeHistoryResponse, error) {
    if s.feesReader == nil {
        return nil, connect.NewError(connect.CodeUnimplemented, ...)
    }
    records, err := s.feesReader.ListFeeHistory(ctx, req.Msg.AccountId, req.Msg.Limit)
    if err != nil { return nil, connect.NewError(connect.CodeInternal, err) }
    return connect.NewResponse(&portfoliov1.ListFeeHistoryResponse{Records: records}), nil
}
```

## Wiring in `cmd/xray`

```go
feesProjection := portfolio.NewPgFeesProjection(pool)
// ...
natsstore.NewProjectionConsumer(js, registry, log, "fees-history").
    WithPersistent(store, feesProjection),
```

Inject into `portfolio.NewServer` alongside `marginCallsReader`.

## Webapp

### Loader
Add to the parallel `Promise.all` in `trading.tsx`:

```ts
portfolioClient.listFeeHistory({ accountId: account, limit: 200 }),
```

Push the result onto `loaderData` as `feeHistory: FeeRecord[]`.

### Component (`PortfolioFees.tsx`)

A table component that takes `feeHistory: FeeRecord[]` (or reads from
context like the margin-call panel does). Columns:

| Time | Kind | Symbol | Amount | Detail |
|---|---|---|---|---|
| 16:00:00 | Transaction | AAPL | $0.15 | trade-abc12 · 100 sh @ $150 |
| 16:00:00 | Short Borrow | TSLA | $1.20 | 50 sh @ 200 bps |
| 15:00:00 | Margin Interest | — | $2.50 | $50,000 @ 600 bps |

- "Kind" with colored badge (TXN=blue, INTEREST=orange, BORROW=red).
- Symbol "—" for margin interest.
- Amount as formatted money.
- Detail column varies by kind:
  - TXN: `trade-…` (truncated) · notional formatted
  - MARGIN_INTEREST: principal × rate
  - SHORT_BORROW: qty @ rate
- Top row: small summary chips — total transaction fees, total
  interest, total borrow — over the visible window.
- Empty state: "No fees charged yet."

### Tab placement

Add a fourth tab `Fees` alongside `Trade | Orders | Positions`. The
URL `tab` param parser in `trading.tsx:75-79` gains a fee case.

## Phased rollout

Each step shippable; ordering matches dependency.

1. **Proto + generate.** Add the enum, message, and rpc to
   `service.proto`. Run `buf generate` and `cd webapp && buf generate`.
   The generated stubs are needed by both server and webapp before
   anything else compiles.

2. **Migration + projection + tests.** New migration file 27. New
   `projection_fees.go` (projection + reader). Unit tests against
   `memstore` covering all three event types, the rebuild path
   (Reset → re-apply → idempotent), and the listing query.

3. **Server handler + cmd/xray wiring.** Add `feesReader` to
   `Server`, the `ListFeeHistory` handler, and the consumer
   registration. Build smoke: `go build ./... && go test ./...`.

4. **Webapp loader + component + tab.** Pull rows in the loader, add
   the `Fees` tab and `PortfolioFees` component. Verify in browser
   with a session that has trades + an open short to generate borrow
   fees.

## Edge cases — explicit tests

| Case | Expected |
|---|---|
| Account with no fees | `ListFeeHistory` returns `[]`; UI shows "No fees charged yet." |
| MarginInterestAccrued with amount=0 | Still inserts a row (so the UI can show "rate ticked, no debit") |
| TransactionFeeCharged with zero rate | Cannot occur — accruer skips zero-amount events for this type by construction; if it did, the projection still inserts |
| ShortBorrowFeeAccrued for multiple symbols in one batch | One row per (account, symbol) per cycle |
| Same event redelivered (consumer crash) | Durable cursor prevents this in normal flow; projection rebuild truncates first |
| Account ID with no events | Empty result, no error |
| `limit=0` | No cap (all matching rows) |
| Mixed event types in one batch | Each row inserted with the correct kind tag |

## Tradeoffs and notes

- **One row per event.** No aggregation in the projection — the UI can
  group/summarize on read. Cheap, easy to verify against the event
  log, and lets future filters (per-symbol, per-day) work without a
  schema change.
- **`related_id` as `TEXT` instead of typed columns per kind.** A row
  per kind would be more "normalized" but the projection becomes one
  table per kind with three nearly-identical schemas. The single-table
  approach mirrors how `projection_pnl` handles realized fills.
- **No NATS event filter on the consumer.** The fees-history
  consumer's `FilterSubject = "events.>"` (the default in
  `ProjectionConsumer`), so it sees every event and only acts on the
  three it cares about. A narrower filter would cut wakeups but
  complicates the projection registry; not worth the optimization at
  xray's scale.
- **Rate display.** Stored in bps for fidelity; the UI converts to
  percent on render so users see "6.00%" not "600 bps".

## Follow-ups (not in v1)

- **Date range filter on the RPC.** `from` / `to` timestamp inputs
  with a corresponding `WHERE charged_at BETWEEN $2 AND $3` clause.
- **Kind / symbol filters.** Easy server-side once the volume warrants
  it; UI-side filtering is enough for the typical row count.
- **Daily roll-ups.** A separate projection that summarizes per-day
  totals — useful for monthly statements.
- **CSV export.** A small `?format=csv` switch on the RPC handler, or
  client-side from the loaded rows.
