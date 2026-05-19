# T+1 Settlement

## Context

Today every trade settles instantly. The moment `TradeExecuted` fires,
the order saga reactor issues `SettleTrade` / `SettleSale` /
`OpenShort` / `CoverShort` against the portfolio, and the resulting
`CashSettled` / `SharesSettled` / `ShortOpened` / `ShortCovered` events
immediately move cash and shares into `CashBalance` and `Holdings`.
`CashBalance` is therefore "everything you've ever transacted netted
together" — there is no distinction between cash you can withdraw and
cash that hasn't cleared yet.

Real markets (post-May 2024 in the US) settle T+1: the trade happens
on day T, but the actual cash/share movements land on day T+1. Brokers
typically permit you to *trade against* unsettled funds (margin
accounts) or with restrictions (cash-account free-riding rules), but
withdrawals are gated on settled cash.

This plan adds a settlement cycle as a first-class event flow:

- Settlements that fire on trade date post to *pending* buckets on the
  portfolio aggregate rather than directly to `CashBalance` /
  `Holdings`.
- A new `SettlementReactor` ticks periodically (analogous to
  `feesaccruer`) and emits `SettlementCleared` events for every
  pending leg whose `settles_at` has passed, moving the leg from
  pending to settled.
- A new `settled_cash` field on `GetPortfolioResponse` and the margin
  snapshot exposes the distinction; withdrawals are gated against
  it.
- A `SETTLEMENT_ENABLED` env var (default `true`) toggles the cycle
  off — when off, settlements remain instant, matching today's
  behavior.

The toggle is **per-event** (settlement decisions are encoded on the
event at emit time, not at replay time) so that replay is deterministic
regardless of the runtime config when replay happens.

Non-goals for v1: configurable per-asset-class settlement windows
(everything is T+1 in modern US equities, so a single constant is
fine); cash-account free-riding enforcement (we treat all accounts as
margin); fail-to-deliver / buy-in flows (sketched as a follow-up); a
"settlement calendar" with market holidays (we use a flat 24-hour
offset, with the duration configurable).

## Architecture

```
trade-date (today's flow, modified)             settlement-date (new)
─────────────────────────────────              ────────────────────────
TradeExecuted                                  SettlementReactor.Tick
  ↓                                              ↓
OrderSagaReactor.onTrade                       enumerate accounts w/ pending legs
  ↓                                              ↓
ExecuteSettleTrade / ExecuteSettleSale /       for each leg with settles_at <= now:
  ExecuteOpenShort / ExecuteCoverShort           emit SettlementCleared
  ↓                                                ↓
CashSettled { ..., settles_at: T+1 }           portfolio.Apply moves
SharesSettled { ..., settles_at: T+1 }           pending → balance
ShortOpened { ..., settles_at: T+1 }
ShortCovered { ..., settles_at: T+1 }
  ↓
portfolio.Apply: if settles_at is in the
  future, post to pending bucket;
  otherwise post to balance (today's path)
```

```
# Server
internal/portfolio/aggregate.go     # PendingLegs slice; new apply paths; SettlementCleared applier
internal/portfolio/commands.go      # SettleTrade/SettleSale/OpenShort/CoverShort stamp settles_at; new ClearSettlement
internal/portfolio/events.go        # new event constants
internal/portfolio/server.go        # GetPortfolio + margin snapshot expose settled_cash, pending bucket; Withdraw gated
internal/settlement/                # NEW reactor package (mirrors feesaccruer layout)
  reactor.go                          # Run / Tick / Status
  reactor_test.go
  pending.go                          # PendingSettlementsTracker interface (which accounts have due legs)
internal/portfolio/projection_pending.go    # NEW projection: accounts with pending legs + their next due time
cmd/xray/main.go                    # wire reactor + projection + env toggle

# Storage
pkg/es/pgstore/migrations/000028.sql  # NEW projection_pending_settlements table

# Proto
proto/portfolio/v1/events.proto     # settles_at field on existing settlement events; SettlementCleared
proto/portfolio/v1/service.proto    # settled_cash, pending_cash_credits, pending_cash_debits

# Webapp
webapp/app/components/PortfolioPanel.tsx    # settled vs. unsettled split
webapp/app/components/PortfolioFees.tsx     # (existing) — no change
webapp/app/routes/projections.tsx           # surface SettlementReactor status alongside accruer
```

## Aggregate changes

Add a single pending-legs slice and one new event type. Today's four
settlement events grow a `settles_at` timestamp and apply differently
based on whether it's in the past.

```go
type PendingLeg struct {
    TradeID     string
    OrderSagaID string
    Kind        PendingLegKind  // CASH_CREDIT, CASH_DEBIT, SHARE_DEBIT, SHARE_CREDIT,
                                // SHORT_OPEN, SHORT_COVER
    Symbol      string
    Quantity    int64           // for share legs / position math
    CashAmount  int64           // signed; + = credit, - = debit
    CostPerShare int64          // for CASH_DEBIT cost-basis
    Proceeds    int64           // for SHARE_DEBIT
    SettlesAt   time.Time
    EmittedAt   time.Time
}

type Portfolio struct {
    // existing fields...

    // PendingLegs are settlement legs awaiting settlement-date
    // clearing. One leg per (saga, trade, leg-kind). Keyed by
    // (TradeID, Kind) so dedup is cheap and clearing is O(1) on lookup.
    PendingLegs map[string]*PendingLeg

    // SettledCash is the portion of CashBalance that has cleared
    // settlement. CashBalance now represents settled + pending net.
    // Invariant: SettledCash + sum(PendingLegs.CashAmount) == CashBalance.
    SettledCash int64
}
```

**Why a flat slice instead of one bucket per kind:** each leg needs to
be addressable by `(TradeID, Kind)` for idempotent clearing, and the
reactor wants to enumerate due legs cheaply across kinds. A map is
both — and the cardinality is bounded by "trades within the last
settlement window," which is small.

**Why `CashBalance` keeps the gross meaning:** existing code (buying
power, margin maintenance, the order-impact preview, all PnL
projections, all client UIs) reads `CashBalance` and treats it as the
trading-relevant cash figure — i.e., what you can deploy in new orders.
That stays true. Only `Withdraw` and the new `SettledCash` view care
about the cleared-vs-pending split. Re-deriving every reader to use a
new "tradeable cash" computation would be a far bigger blast radius
than this plan should justify.

### Modified apply paths

When `settles_at == settled_at` (or zero — legacy events) the existing
behavior runs verbatim. When `settles_at > settled_at`:

- **`CashSettled` (long buy)**: `CashBalance -= amount` and
  `Holdings[symbol] += quantity` happen as today (so margin/buying
  power immediately reflect the trade), but `SettledCash` is NOT
  decremented. Instead a `PendingLeg{Kind:CASH_DEBIT, CashAmount:-amount, …}`
  is added. The hold consumption (`fromHold` / overflow) still runs.
- **`SharesSettled` (long sell)**: shares debit immediately as today;
  `CashBalance += proceeds` happens as today; `SettledCash` is NOT
  incremented. A `PendingLeg{Kind:CASH_CREDIT, CashAmount:+proceeds, …}`
  is added.
- **`ShortOpened`**: proceeds and collateral flow into the pools as
  today (so margin math is correct), and a `PendingLeg{Kind:SHORT_OPEN, …}`
  records that on clear the pool entries are also marked settled (for
  audit; the cash is already locked in the pool).
- **`ShortCovered`**: pool draining and PnL flow as today; a
  `PendingLeg{Kind:SHORT_COVER, …}` records the cash residual that
  hit `CashBalance` so it can be moved into `SettledCash` on clear.

### New event: `SettlementCleared`

```go
case *portfoliov1.SettlementCleared:
    leg, ok := p.PendingLegs[legKey(data.TradeId, data.Kind)]
    if !ok { return nil }  // already cleared; idempotent
    p.SettledCash += leg.CashAmount
    delete(p.PendingLegs, legKey(data.TradeId, data.Kind))
```

That's the whole applier. Everything else (shares, pools, PnL, fees)
already happened on trade date — settlement only moves the
unsettled-cash bookkeeping line.

### New command: `ClearSettlement`

```go
type ClearSettlement struct {
    AccountID string
    TradeID   string
    Kind      portfoliov1.SettlementLegKind
}

func ExecuteClearSettlement(p *Portfolio, cmd ClearSettlement) ([]es.Event, error) {
    leg, ok := p.PendingLegs[legKey(cmd.TradeID, cmd.Kind)]
    if !ok {
        return nil, nil  // idempotent — already cleared or never pending
    }
    if leg.SettlesAt.After(time.Now()) {
        return nil, ErrSettlementNotDue
    }
    now := time.Now()
    return []es.Event{{
        AggregateID: p.AggregateID(),
        Type:        EventSettlementCleared,
        Timestamp:   now,
        Data: &portfoliov1.SettlementCleared{
            AccountId:  cmd.AccountID,
            TradeId:    cmd.TradeID,
            Kind:       cmd.Kind,
            ClearedAt:  timestamppb.New(now),
            CashAmount: leg.CashAmount,
        },
    }}, nil
}
```

## Events (proto)

```protobuf
enum SettlementLegKind {
  SETTLEMENT_LEG_KIND_UNSPECIFIED   = 0;
  SETTLEMENT_LEG_KIND_CASH_CREDIT   = 1;  // long sell proceeds
  SETTLEMENT_LEG_KIND_CASH_DEBIT    = 2;  // long buy cost
  SETTLEMENT_LEG_KIND_SHORT_OPEN    = 3;
  SETTLEMENT_LEG_KIND_SHORT_COVER   = 4;
}

// Added to CashSettled, SharesSettled, ShortOpened, ShortCovered:
google.protobuf.Timestamp settles_at = N;  // zero/equal-to-settled_at = instant

message SettlementCleared {
  string                       account_id  = 1;
  string                       trade_id    = 2;
  string                       order_saga_id = 3;
  SettlementLegKind            kind        = 4;
  int64                        cash_amount = 5;  // signed
  google.protobuf.Timestamp    cleared_at  = 6;
}
```

`settles_at` defaults to `settled_at + SettlementWindow` at emit time
when the toggle is on. When the toggle is off, `settles_at == settled_at`
and the aggregate skips the pending-leg detour entirely. Replay reads
the field straight off the event — runtime config is never consulted.

## Settlement reactor (`internal/settlement/`)

Layout mirrors `feesaccruer`:

```go
type Config struct {
    Interval time.Duration  // how often to scan for due legs (default 1m)
}

type Reactor struct {
    handler  *es.Handler[*portfolio.Portfolio]
    accounts PendingSettlementsTracker
    clock    Clock
    cfg      Config
    log      *slog.Logger

    mu     sync.Mutex
    status Status
}

type Status struct {
    Interval         time.Duration
    LastTickAt       time.Time
    LastTickDuration time.Duration
    LastTickCleared  int
    LastTickFailed   int
}

// PendingSettlementsTracker returns accounts with at least one pending
// leg, ideally narrowed to those whose earliest leg is due by `before`.
type PendingSettlementsTracker interface {
    AccountsWithDueSettlements(ctx context.Context, before time.Time) ([]string, error)
}
```

`Tick` loads each due account's portfolio, walks its `PendingLegs`,
and issues one `ClearSettlement` per leg whose `SettlesAt <= now`.
Errors on a single account are logged and don't abort the tick.

Run loop is identical to `feesaccruer.Run` — a `time.NewTicker(Interval)`
with context cancellation. `Tick(ctx, now)` is exported so tests can
drive single cycles deterministically.

The reactor is registered against the diagnostics service's
`GetOperationsStatus` (the same place `feesaccruer.Status()` and
`reconciler.Status()` already surface) so the `/projections` page in
the UI shows tick cadence, last-cleared count, and any failures.

## Pending-settlements projection

A PG projection over `SettlementCleared` and the four (modified)
settlement events tracks per-account "next due time," so the reactor
doesn't have to load every portfolio every tick.

```sql
-- migrations/000028.sql
CREATE TABLE IF NOT EXISTS projection_pending_settlements (
    account_id  TEXT NOT NULL,
    trade_id    TEXT NOT NULL,
    kind        INT  NOT NULL,           -- SettlementLegKind
    settles_at  TIMESTAMPTZ NOT NULL,
    cash_amount BIGINT NOT NULL,
    PRIMARY KEY (account_id, trade_id, kind)
);
CREATE INDEX IF NOT EXISTS idx_pending_settlements_due
    ON projection_pending_settlements(settles_at)
    WHERE settles_at > '1970-01-01';
```

On `CashSettled` / `SharesSettled` / `ShortOpened` / `ShortCovered`
with a non-trivial `settles_at`: insert a row. On `SettlementCleared`:
delete the row. `AccountsWithDueSettlements(before)` is a
`SELECT DISTINCT account_id ... WHERE settles_at <= $1`. Cheap and
gives the reactor a precise work list.

A separate projection consumer (`pending-settlements`) keeps this from
fighting other projections' cursors during rebuild, same pattern as
`fees-history`.

## Server changes

### `GetPortfolio` / `StreamPortfolio`

Add fields:

```protobuf
message GetPortfolioResponse {
  // existing...
  int64 settled_cash          = 7;
  int64 pending_cash_credits  = 8;  // sum of CashAmount > 0
  int64 pending_cash_debits   = 9;  // sum of CashAmount < 0 (positive value)
}
```

Computed from `Portfolio.SettledCash` and a walk over `PendingLegs`.

### `GetMarginSnapshot`

Add `settled_cash` for parity. Buying power continues to use
`CashBalance` (the gross figure) — the policy is "margin account, can
trade against unsettled" — but the snapshot surfaces the split so the
UI can show it.

### `Withdraw`

Gate on settled cash:

```go
func ExecuteWithdraw(p *Portfolio, cmd Withdraw) ([]es.Event, error) {
    // existing checks...
    if cmd.Amount > p.SettledCash {
        return nil, ErrUnsettledFunds
    }
    // existing emit path...
}
```

`SettledCash` debits on `CashWithdrawn` apply; `SettledCash` credits
on `CashDeposited` apply. (Deposits are always instantly settled.)

## Config / toggle

```go
// In cmd/xray/main.go:
settlementEnabled := envBoolOr("SETTLEMENT_ENABLED", true)
settlementWindow := parseDurationOr("SETTLEMENT_WINDOW", 24*time.Hour)
settlementInterval := parseDurationOr("SETTLEMENT_TICK_INTERVAL", time.Minute)

policy := portfolio.SettlementPolicy{
    Enabled: settlementEnabled,
    Window:  settlementWindow,
}
// passed into portfolio.NewServer and into the four reactor command builders
```

When `Enabled` is false, every `SettleTrade` / `SettleSale` / etc.
stamps `SettlesAt == SettledAt` and the aggregate skips the pending
path. The reactor still starts and ticks (cheaply, finds no work) so
operational paths stay consistent. The `/projections` UI shows it
"running, settlement disabled" so the disabled state is visible.

`SettlementPolicy` is injected into the ordersaga reactor (the only
caller of the four settlement commands today) rather than read from a
package global — keeps tests deterministic and lets a future-day
multi-policy scheme (per-symbol, per-account) slot in.

## Webapp

### `PortfolioPanel.tsx`

Existing "Cash" line splits into:

```
Cash (tradable)   $50,000.00
  Settled            $48,500.00
  Pending credits      +$1,500.00  (2 trades)
  Pending debits         -$0.00
```

Mantine `Tooltip` on each pending line links to the orders that
generated it (just filter saga list by trade id).

### `/projections` page

Add the settlement reactor card alongside the existing fees-accruer
and reconciler cards. Same Status() shape, same renderer.

## Phased rollout

Each step is shippable. The toggle defaulting to *on* in dev is
acceptable because:
- existing data has no `settles_at` ⇒ apply path treats every legacy
  event as instant settlement (the "zero or equal" check),
- new events stamp `settles_at = now + 24h` and start landing in
  pending,
- the reactor clears them on schedule.

So the migration story is "deploy with the toggle on; existing
balances are unaffected; new trades settle on T+1 going forward."

1. **Proto + generate.** Add `settles_at` to the four settlement
   events, add `SettlementLegKind` and `SettlementCleared`, add
   `settled_cash` / `pending_cash_*` fields to `GetPortfolioResponse`
   and `GetMarginSnapshotResponse`. Run `buf generate` (Go + webapp).

2. **Aggregate: pending-leg + SettlementCleared applier.** Add
   `PendingLegs` + `SettledCash` to `Portfolio`. Modify the four
   apply paths to branch on `settles_at`. Add `ExecuteClearSettlement`
   + `SettlementCleared` applier. Unit tests against memstore for:
   each leg kind in both modes, the clear path, replay invariance.

3. **Command stamping + policy plumbing.** Add
   `portfolio.SettlementPolicy` and route it into the four
   `Execute*` commands. Inject `SettlementPolicy` into
   `ordersaga.Reactor`. Existing tests pass with policy
   `{Enabled: false}` (legacy semantics).

4. **Withdraw gating + settled-cash exposure.** Add `ErrUnsettledFunds`
   guard. Wire `SettledCash` and `PendingLegs` totals into
   `GetPortfolio` and `GetMarginSnapshot`. Unit tests for the
   withdraw gate (with/without settled cash, with/without policy
   enabled).

5. **Pending-settlements projection.** New migration, new
   `projection_pending.go`, new tests. Register a `pending-settlements`
   consumer in `main.go`.

6. **Settlement reactor.** New `internal/settlement/` package mirroring
   `feesaccruer`. Implements `Run` / `Tick(ctx, now)` / `Status()`.
   Unit tests with memstore + a manual `PendingSettlementsTracker`.

7. **cmd/xray wiring.** Construct the policy, start the reactor,
   register Status() with diagnostics. Add env vars to README.

8. **Webapp surfaces.** Update `PortfolioPanel.tsx` for the split.
   Add the reactor card to `/projections`. Manual smoke: deposit,
   trade, observe pending, tick forward, observe cleared.

## Edge cases — explicit tests

| Case | Expected |
|---|---|
| Toggle off → event has `settles_at == settled_at` → applier takes instant path | `SettledCash` and `CashBalance` move together, no pending leg |
| Toggle on → trade → reactor tick before `settles_at` | No `SettlementCleared` event; pending leg remains |
| Toggle on → trade → reactor tick after `settles_at` | Single `SettlementCleared`; `SettledCash` advances; row deleted from projection |
| Tick fires twice at same wall-clock | Second `ClearSettlement` is a no-op (PendingLegs lookup misses) |
| Reactor crashes mid-tick (some accounts cleared, some not) | Next tick picks up where it stopped; projection is the durable work list |
| `Withdraw` when `amount <= SettledCash` | Succeeds; `SettledCash` decrements |
| `Withdraw` when `amount > SettledCash` but `amount <= CashBalance` | `ErrUnsettledFunds`; nothing emitted |
| Deposit while pending legs exist | `CashBalance` and `SettledCash` both rise; pending legs unaffected |
| Replay a legacy event with no `settles_at` | Treated as `settles_at == settled_at` (instant path); no migration needed |
| Replay with toggle off but historical events have non-trivial `settles_at` | Pending legs still get created; reactor still clears them on the recorded `settles_at`. Toggle only affects *new* emits |
| Snapshot serialization | `PendingLegs` + `SettledCash` go into the `Portfolio` snapshot proto; `snapshot_test.go` covers round-trip |
| Margin call fires while a pending credit is in flight | Margin math is unchanged (uses `CashBalance`); the call evaluates against gross cash, as today. Document this — the alternative (gating on `SettledCash`) would shrink buying power dramatically, contradicting "margin account, can trade against unsettled" |

## Tradeoffs and notes

- **One pending leg per cash movement, not per share movement.** Shares
  are already in `Holdings` on trade date in the current model and we
  keep that — the model never had a "pending shares" concept and adding
  one would ripple through the matching engine, sell-side hold logic,
  and PnL projections. The realism we add with cash-only T+1 is
  meaningful (settled-vs-unsettled cash, withdraw gating) without
  rewriting the share lifecycle. A future expansion could add
  `PendingShareCredits` / `PendingShareDebits` for completeness, with
  the same per-event toggle pattern.
- **`CashBalance` keeps its current meaning.** This is the load-bearing
  decision. Many downstream readers — buying power, margin
  maintenance, the order-impact preview, the strategy bots — treat
  `CashBalance` as "the trading-relevant cash figure" and we don't
  want to thread a new concept through all of them. `SettledCash` is
  additive: a new field, surfaced where it matters (`Withdraw`, the
  UI cash line, the margin snapshot), and ignored everywhere else.
- **Per-event toggle, not runtime.** `settles_at` is frozen on the
  event so replay is deterministic regardless of runtime config. The
  alternative (consulting `SettlementPolicy` from the applier) breaks
  replay across config changes — turning the toggle off and rebuilding
  would silently rewrite history.
- **Reactor as polling loop, not event-driven.** The reactor wakes on
  a timer and queries the projection for due work, rather than
  reacting to `TradePending` events directly. Reasons: (a) the
  scheduling axis is wall-clock, not event-arrival, so a NATS
  subscriber would have to translate either way; (b) the projection
  doubles as the "what's outstanding right now" answer for diagnostics
  and the UI; (c) consistent with `feesaccruer`, which has the same
  shape for the same reasons.
- **Snapshot bloat.** `PendingLegs` adds entries that linger for one
  settlement window (~24h). At, say, 10k trades/day per account, that's
  10k map entries per snapshot — a few hundred KB. Acceptable; revisit
  if a portfolio's snapshot crosses ~10MB.
- **Two-bucket invariant.** `SettledCash + Σ PendingLegs.CashAmount ==
  CashBalance` is a load-bearing equality. Worth a `Portfolio.Invariant()`
  check function exercised in tests and (optionally) gated behind a
  debug-only `Apply` wrapper.

## Follow-ups (not in v1)

- **Per-share settlement.** Add `PendingShareCredits` /
  `PendingShareDebits` symmetric to cash, with the same toggle.
  Useful for free-riding enforcement and for layering corporate
  actions on top.
- **Fail-to-deliver / buy-in.** A probabilistic "share didn't deliver"
  path on settlement-day: leg fails to clear, reactor emits
  `SettlementFailed`, follow-up `BuyIn` saga forces a market purchase
  to cure. The plumbing is the same as auto-liquidation.
- **Trade-date vs. settlement-date P&L.** Two PnL projections — one
  driven off trade-date events (today's), one off `SettlementCleared`.
  Lets the UI show "P&L on books" vs. "settled P&L."
- **Market-holiday calendar.** Replace the flat 24h offset with an
  exchange calendar so a Friday trade settles Monday, not Saturday.
- **Cash-account mode.** A second `SettlementPolicy` flavor that
  enforces free-riding rules — withdraws *and* buying power gate on
  settled funds.
- **Per-account / per-symbol toggle.** Settlement is global today;
  could become `account.settlement_policy` so a single dev/test
  account can opt out without affecting the rest.
