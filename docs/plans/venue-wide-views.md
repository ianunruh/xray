# Venue-Wide Views (Lesser UI Gaps)

## Context

`docs/plans/ui-coverage-gaps.md` flagged two lesser items beyond the
five main verified gaps (all of which have shipped). Both are
venue-wide read views over data that the server already computes but
that no UI surface reaches:

1. **Per-symbol short-interest aggregate.** `projection_shorts.go`
   already maintains `(symbol, account_id, quantity)` triples. The
   `MarketsPanel` lists symbols with their phase and official close,
   but no column shows aggregate short interest. Surfacing it costs a
   new aggregation query and a couple of columns.

2. **Background-process introspection.** Three loops drive the system
   in the background — the fees accruer, the periodic reconciler, and
   the margin-call reactor. None of them expose what they're doing in
   the UI beyond their `/projections` entry. The accruer's "last
   accrued at" per account, the reconciler's last-tick counts, and
   the margin reactor's grace window are all observable server-side
   but invisible from outside.

This plan covers both. They're independent — implement in either
order — but small enough to ship together.

## 1. Per-symbol short-interest panel

### Backend

`PgShortsBySymbolProjection` (`internal/portfolio/projection_shorts.go`)
already stores `projection_shorts_by_symbol(symbol, account_id, quantity)`.
Add a second read method:

```go
// SymbolShortInterest is the venue-wide aggregate for one symbol.
type SymbolShortInterest struct {
    Symbol       string
    TotalQty     int64
    AccountCount int32
}

func (p *PgShortsBySymbolProjection) ListShortInterest(ctx context.Context) ([]*SymbolShortInterest, error) {
    rows, err := p.pool.Query(ctx,
        `SELECT symbol, SUM(quantity), COUNT(*)
        FROM projection_shorts_by_symbol
        WHERE quantity > 0
        GROUP BY symbol
        ORDER BY symbol`,
    )
    // ... scan + return
}
```

### Proto

```protobuf
message ListShortInterestRequest {}

message ListShortInterestResponse {
  repeated SymbolShortInterest entries = 1;
}

message SymbolShortInterest {
  string symbol         = 1;
  int64  total_quantity = 2;
  int32  account_count  = 3;
}

// Added to PortfolioService:
rpc ListShortInterest(ListShortInterestRequest) returns (ListShortInterestResponse);
```

`ListShortInterest` doesn't take an account parameter — this is a
venue-wide view, deliberately. Per-account shorts are already on the
positions panel.

### Wiring

- New reader interface on `Server` (`shortInterestReader`); the
  existing `shortsProjection` satisfies it once the new method lands.
- Add the RPC handler.
- Webapp: extend `markets.tsx` loader to call
  `portfolioClient.listShortInterest({})` and join the result onto
  `rows`. Two new columns: "Short Interest" (qty) and "# Accounts".
  Symbols with zero short interest show "—".

No new projection, no migration, no consumer wiring.

## 2. Background-process introspection

### Stats surface

Each of the three components records its last-tick stats in memory,
behind a small mutex. The shape varies by component, so they expose
distinct `Status()` methods rather than sharing a common type:

**Accruer (`internal/feesaccruer/accruer.go`):**

```go
type AccruerStatus struct {
    Interval         time.Duration
    MinElapsed       time.Duration
    LastTickAt       time.Time
    LastTickDuration time.Duration
    LastTickAccounts int   // total processed last tick
    LastTickFailed   int   // subset that errored
}

func (a *Accruer) Status() AccruerStatus
```

Update inside `Tick` under a new `mu sync.Mutex` field; readers take
the lock briefly to copy out.

**Reconciler (`internal/reconciler/reconciler.go`):**

```go
type ReconcilerStatus struct {
    Interval                time.Duration
    LastTickAt              time.Time
    LastTickDuration        time.Duration
    LastTickSagasReconciled int
    LastTickMarginCallsEvaluated int
    LastTickFailedSagas     int
}

func (r *Reconciler) Status() ReconcilerStatus
```

Update inside `ReconcileOnce` similarly.

**Margin reactor (`internal/margincall/reactor.go`):**

```go
type ReactorStatus struct {
    Grace             time.Duration
    ActiveCallCount   int  // from activeCalls.AllOpen()
}

func (r *Reactor) Status(ctx context.Context) (ReactorStatus, error)
```

The reactor itself is event-driven (no tick loop), so it doesn't have
"last tick" semantics. Its status is its config + a live count via the
existing `activeCalls` tracker.

### Proto + RPC

Goes on DiagnosticsService — these are operational diagnostics, not
domain reads. One RPC returning all three:

```protobuf
message GetOperationsStatusRequest {}

message GetOperationsStatusResponse {
  AccruerStatus   accruer   = 1;
  ReconcilerStatus reconciler = 2;
  MarginReactorStatus margin_reactor = 3;
}

message AccruerStatus {
  int64                       interval_ms        = 1;
  int64                       min_elapsed_ms     = 2;
  google.protobuf.Timestamp   last_tick_at       = 3;
  int64                       last_tick_ms       = 4;
  int32                       last_tick_accounts = 5;
  int32                       last_tick_failed   = 6;
}

message ReconcilerStatus {
  int64                       interval_ms                       = 1;
  google.protobuf.Timestamp   last_tick_at                      = 2;
  int64                       last_tick_ms                      = 3;
  int32                       last_tick_sagas_reconciled        = 4;
  int32                       last_tick_margin_calls_evaluated  = 5;
  int32                       last_tick_failed_sagas            = 6;
}

message MarginReactorStatus {
  int64 grace_ms          = 1;
  int32 active_call_count = 2;
}

rpc GetOperationsStatus(GetOperationsStatusRequest) returns (GetOperationsStatusResponse);
```

### Wiring

- `diagnostics.Server` gains constructor params for the three
  components (or for `Status` providers — a tiny interface keeps the
  package decoupled).
- New handler builds the response from the three sources.
- `cmd/xray/main.go` passes the components into `NewServer`.

### Webapp

Add an "Operations" card to the existing `/projections` page since
that's where all in-flight diagnostics already live. Render the three
status blocks side-by-side. Light revalidation interval (every 5s) so
the user sees the loops actually ticking.

## Phased rollout

Short-interest first (one slice, zero risk), then ops-status (touches
three packages, one new RPC). Each phase is shippable.

1. **Short interest read method.** Add `ListShortInterest` to
   `PgShortsBySymbolProjection`. No proto / wiring yet.
2. **Short interest proto + RPC + UI.** `ListShortInterest` proto +
   handler + markets-loader join + two new columns.
3. **Accruer status.** `Status()` method + mu-tracked last-tick
   state. No proto yet.
4. **Reconciler status.** Same shape.
5. **Margin reactor status.** Status method using `activeCalls`.
6. **Diagnostics RPC.** Proto + handler + wiring.
7. **Webapp Operations panel.** Loader fetch + card on `/projections`.

## Edge cases — explicit tests

| Case | Expected |
|---|---|
| No shorts in the system | `ListShortInterest` returns `[]`; UI shows "—" for the columns |
| Short opened then fully covered | Aggregate qty drops to 0, row disappears (the projection already deletes when qty <= 0) |
| Accruer hasn't ticked yet (post-boot) | `LastTickAt` is zero; UI shows "—" instead of an epoch date |
| Reconciler tick in progress when Status is read | Reader gets the last completed tick's stats (mutex serializes) |
| No active margin calls | `ActiveCallCount = 0`; UI shows "0 open" |

## Tradeoffs and notes

- **No persisted history of ticks.** The status is "last completed";
  history would need a new projection. Not v1 — point-in-time
  observability is enough to verify the loops are running.
- **Status methods, not metrics.** A real production deployment
  would scrape Prometheus instead; for xray's "look at it in a
  browser" use case, an RPC is enough and keeps the surface
  homogeneous with the rest of the diagnostics view.

## Follow-ups (not in v1)

- **Per-symbol short interest history** — a separate projection
  bucketing daily/hourly so the UI can chart it.
- **Tick-history time series for accruer / reconciler** — same shape,
  rolling N entries kept in memory.
- **Wire to `/projections` page header** as a status bar instead of a
  card, once the Operations card is doing real work.
