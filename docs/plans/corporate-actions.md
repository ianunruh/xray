# Corporate Actions

## Context

Corporate actions — splits, dividends, ticker changes — are the
canonical "one declaration fans out across many aggregates" event
flow. A single 2-for-1 split of AAPL has to ripple coherently across
every portfolio holding AAPL, every resting order on the AAPL book,
every in-flight saga targeting AAPL, every PnL row tracking AAPL
cost basis, and every projection that knows the symbol. Get any of
those wrong and the system drifts permanently — replays surface the
error in the causation chain rather than masking it.

This plan covers three actions chosen for breadth and depth:

- **Stock splits** (forward + reverse) — quantity and price both
  rewrite. The "many aggregates change in lockstep" showcase.
- **Cash dividends** — every record-date holder gets a cash credit.
  The "many aggregates each receive a small credit" showcase, with a
  record-date / pay-date scheduling element analogous to T+1
  settlement.
- **Symbol changes (renames)** — historical orderbook keeps its
  events; new orders route to the new symbol. The "rewriting how
  events route, not what they say" showcase.

Each action is operator-declared via a new RPC and applied by a
scheduler reactor (one tick, watches for actions whose effective
date has passed, drives the fan-out commands). Holders' positions
adjust, in-flight sagas adjust, resting orderbook orders cancel
with reason `corporate_action`. PnL projections and mark history
preserve the pre-action prices for historical accuracy; new prices
start fresh post-action.

Non-goals for v1: stock dividends (similar to splits but with a
cash-equivalent value that affects cost basis differently); mergers,
spin-offs, acquisitions (multi-symbol effects, complex ratio math);
rights offerings (creates new tradeable instruments); per-account
tax-lot accounting (we track avg-cost only); automatic adjustment
of stop / trailing-stop trigger prices on resting orders (we cancel
those — see Tradeoffs).

## Architecture

```
declare-time                          apply-time (effective date)
────────────                          ─────────────────────────────────
SagaService.DeclareCorporateAction    CorporateAction reactor (tick)
  ↓                                     ↓ enumerate due actions
CorporateAction aggregate               ↓
  CorporateActionDeclared             For each due action by type:
                                        Splits → walk holders + orderbook + sagas
                                        Dividends → walk record-date holders
                                        Renames → emit SymbolRenamed marker

every affected aggregate emits its own adjustment event:

Portfolio:  HoldingAdjusted{symbol, ratio} | DividendCredited{...}
OrderBook:  OrderCancelled{reason: "corporate_action:split"}
            (post-rename: new symbol = new aggregate)
TWAPSaga:   SagaQuantityAdjusted | SagaPriceAdjusted
BracketSaga, OCOSaga: target-price + qty adjustments
```

```
# Proto
proto/corpaction/v1/                # NEW package
  events.proto                       # CorporateActionDeclared, *Applied, holder-side adjustments
  service.proto                      # DeclareCorporateAction, ListCorporateActions
proto/portfolio/v1/events.proto      # HoldingAdjusted, DividendCredited
proto/orderbook/v1/events.proto      # nothing new — uses existing OrderCancelled w/ reason

# Storage
pkg/es/pgstore/migrations/000031.sql # NEW projection_corporate_actions table

# Server
internal/corpaction/                 # NEW package
  aggregate.go                       # CorporateAction (Declared → Applied)
  commands.go
  events.go
  reactor.go                         # tick-driven applier
  projection_pg.go                   # action ledger for the UI
  reactor_test.go
internal/portfolio/commands.go       # ExecuteAdjustHolding, ExecuteCreditDividend
internal/portfolio/aggregate.go      # applyHoldingAdjusted, applyDividendCredited
internal/portfolio/projection_pg.go  # mirror adjustments into projection_holdings
internal/ordersaga/reactor.go        # watch CorporateActionApplied, cancel/adjust own state
internal/twapsaga/reactor.go         # adjust slice math on splits
internal/bracket/reactor.go          # adjust TP/SL prices on splits
internal/ocosaga/reactor.go          # adjust TP/SL prices on splits
internal/sagasvc/server.go           # DeclareCorporateAction RPC
cmd/xray/main.go                     # wire reactor + projection + diagnostics

# Webapp
webapp/app/routes/corporate-actions.tsx  # NEW: list + declare form
webapp/app/components/PortfolioPanel.tsx # show "split-adjusted" badge on positions
```

## CorporateAction aggregate

Pure state machine. One aggregate per action, keyed by a UUID at
declaration; the operator can declare many over time, including
multiple actions on the same symbol with different effective dates.

```go
type CorporateAction struct {
    es.AggregateBase

    ActionID      string
    Symbol        string
    Type          ActionType    // SPLIT, CASH_DIVIDEND, SYMBOL_CHANGE
    Status        Status        // Declared, Applied, Failed

    // Type-specific payload — exactly one of these is non-zero
    // depending on Type, mirroring the oneof on the proto event.
    SplitNumerator   int32     // split: 2-for-1 = 2/1, 1-for-10 (reverse) = 1/10
    SplitDenominator int32
    DividendPerShare int64     // cash dividend: amount per share, in price units
    NewSymbol        string    // symbol change

    EffectiveDate time.Time    // for splits/renames: the moment of cutover
    RecordDate    time.Time    // for dividends: which holders are entitled
    PayDate       time.Time    // for dividends: when cash actually moves
    DeclaredAt    time.Time
    AppliedAt     time.Time
}
```

Status progression:
- `Declared` → operator submitted; sitting in the queue
- `Applied` → fan-out events emitted, action is in the history
- `Failed` → fan-out hit an unrecoverable error (logged, surfaced
  via diagnostics; operator can investigate or retry)

The aggregate itself stays tiny — it's the *ledger entry* for the
action. All the actual state change happens on the affected
aggregates (portfolios, orders, sagas) via their own events. This
keeps replay precise: the corporate-action aggregate just records
"declared at T1, applied at T2"; the *real* changes live on the
aggregates they touch.

## Events

### Action lifecycle (`proto/corpaction/v1/events.proto`)

```protobuf
enum ActionType {
  ACTION_TYPE_UNSPECIFIED   = 0;
  ACTION_TYPE_SPLIT         = 1;
  ACTION_TYPE_CASH_DIVIDEND = 2;
  ACTION_TYPE_SYMBOL_CHANGE = 3;
}

message CorporateActionDeclared {
  string                    action_id      = 1;
  string                    symbol         = 2;
  ActionType                type           = 3;
  // Split: ratio is numerator/denominator. 2-for-1 = 2/1,
  // 1-for-10 (reverse) = 1/10.
  int32                     split_numerator   = 4;
  int32                     split_denominator = 5;
  // Cash dividend: per-share, in price units (e.g. $0.24 = 2400).
  int64                     dividend_per_share = 6;
  // Symbol change: the post-effective ticker.
  string                    new_symbol     = 7;
  // Splits + renames: instant of cutover.
  google.protobuf.Timestamp effective_date = 8;
  // Dividends: holders at record_date are entitled; cash credits
  // on pay_date.
  google.protobuf.Timestamp record_date    = 9;
  google.protobuf.Timestamp pay_date       = 10;
  google.protobuf.Timestamp declared_at    = 11;
}

message CorporateActionApplied {
  string                    action_id      = 1;
  google.protobuf.Timestamp applied_at     = 2;
  int32                     holders_count  = 3;  // how many portfolios touched
  int32                     orders_count   = 4;  // how many resting orders cancelled
  int32                     sagas_count    = 5;  // how many in-flight sagas adjusted
}

message CorporateActionFailed {
  string                    action_id   = 1;
  string                    reason      = 2;
  google.protobuf.Timestamp failed_at   = 3;
}
```

The applied event carries counts so the diagnostics panel and the
ledger UI can show "this split touched 12 accounts and cancelled 47
orders" without re-deriving from the event log.

### Per-aggregate adjustment events

**`portfolio.v1.HoldingAdjusted`** — emitted per-account when a
split (or reverse split) applies. Idempotent: keyed by
`(account_id, action_id)` via `AppliedActions` set on the aggregate.

```protobuf
message HoldingAdjusted {
  string                    account_id  = 1;
  string                    action_id   = 2;
  string                    symbol      = 3;
  // Split numerator/denominator at the moment of apply. Stored on
  // the event so replays are deterministic across config changes.
  int32                     numerator   = 4;
  int32                     denominator = 5;
  // Quantities before and after. The applier moves Holdings to new_*;
  // total_cost is preserved (cost basis doesn't change in a split),
  // so avg_cost auto-scales by 1/ratio.
  int64                     old_quantity = 6;
  int64                     new_quantity = 7;
  google.protobuf.Timestamp adjusted_at  = 8;
}
```

**`portfolio.v1.DividendCredited`** — one per (account, action). Pays
cash on `pay_date`. Settled instantly into both `CashBalance` and
`SettledCash` (dividends are wired cash, no T+1 deferral).

```protobuf
message DividendCredited {
  string                    account_id     = 1;
  string                    action_id      = 2;
  string                    symbol         = 3;
  int64                     shares_of_record = 4;  // snapshot at record_date
  int64                     per_share      = 5;
  int64                     amount         = 6;  // shares * per_share
  google.protobuf.Timestamp credited_at    = 7;
}
```

**Sagas (TWAP, Bracket, OCO)** — each saga type that has an
in-flight target price emits its own adjustment event, e.g.,
`TWAPSagaAdjusted{ratio_numerator, ratio_denominator, new_limit_price,
new_remaining_quantity}`. The reactor walks the saga projection by
symbol, computes the new values, dispatches an `Adjust*` command
per saga.

**OrderBook** — resting orders for the symbol are cancelled with
reason `corporate_action:split` / `corporate_action:rename`. No new
event type; we reuse the existing `OrderCancelled` with a structured
reason string the UI parses.

### Symbol changes

Renames are mostly a routing concern: pre-rename, the orderbook
aggregate is `orderbook:AAPL`; post-rename, it's `orderbook:AAPC`.
We emit one new event on the *old* aggregate (`SymbolRenamed{
new_symbol}`) so its history terminates cleanly, and from that
moment new orders go to the new aggregate ID. Holdings on each
portfolio get a separate `SymbolMigrated{old, new}` event that
rewrites the key in the `Holdings` map.

```protobuf
message SymbolRenamed {
  string                    old_symbol  = 1;
  string                    new_symbol  = 2;
  string                    action_id   = 3;
  google.protobuf.Timestamp renamed_at  = 4;
}

message SymbolMigrated {
  string                    account_id  = 1;
  string                    action_id   = 2;
  string                    old_symbol  = 3;
  string                    new_symbol  = 4;
  google.protobuf.Timestamp migrated_at = 5;
}
```

Open orders for the old symbol cancel on rename (same as splits) —
re-submission against the new symbol is the user's job. Sagas
mid-flight cancel for the same reason; reactor doesn't try to
re-target across symbols (too easy to get wrong, low value for v1).

## CorporateAction reactor (`internal/corpaction/`)

Mirrors `settlement.Reactor` shape: `Run` / `Tick(ctx, now)` /
`Status()`, registered with diagnostics. Each tick:

1. Query `projection_corporate_actions` for actions in status
   `Declared` whose effective date (splits/renames) or pay date
   (dividends) has passed.
2. For each due action, dispatch the type-specific applier in a
   sub-routine that:
   - Snapshot-loads all affected aggregates (holders projection +
     orders projection + sagas projection)
   - Emits the fan-out events in a deterministic order (holders →
     orders → sagas → action `Applied`)
   - Counts and bundles into a single `CorporateActionApplied`
3. On per-account or per-order error: log and continue. The action
   stays `Declared` and retries next tick. Truly stuck actions
   (poison action — e.g., malformed payload) flip to `Failed` after
   N retries.

Idempotency: every per-aggregate adjustment event carries the
`action_id`, and each touched aggregate keeps an `AppliedActions
map[action_id]struct{}` so re-applying the same action is a no-op.
Cheap; matches how `Portfolio.SettledTrades` dedups settlement.

Holders enumeration uses `projection_holdings`:
```sql
SELECT DISTINCT account_id FROM projection_holdings
 WHERE symbol = $1 AND quantity > 0
```

For cash dividends, the holders snapshot happens **at record date**,
not pay date. A record-date freeze projection
(`projection_dividend_record_holders`) takes a snapshot when the
reactor first observes `now >= record_date` and the snapshot is
what pay-date credits draw from. This handles the realistic case
where someone sells the day after record date — they still get the
dividend.

## Storage

```sql
-- migrations/000031.sql

CREATE TABLE IF NOT EXISTS projection_corporate_actions (
    action_id        TEXT PRIMARY KEY,
    symbol           TEXT NOT NULL,
    type             INT  NOT NULL,
    status           INT  NOT NULL,        -- Declared=1, Applied=2, Failed=3
    split_numerator  INT,
    split_denominator INT,
    dividend_per_share BIGINT,
    new_symbol       TEXT,
    effective_date   TIMESTAMPTZ,
    record_date      TIMESTAMPTZ,
    pay_date         TIMESTAMPTZ,
    declared_at      TIMESTAMPTZ NOT NULL,
    applied_at       TIMESTAMPTZ,
    failed_reason    TEXT,
    holders_count    INT,
    orders_count     INT,
    sagas_count      INT
);

CREATE INDEX IF NOT EXISTS idx_corp_actions_due
    ON projection_corporate_actions(effective_date)
    WHERE status = 1;
CREATE INDEX IF NOT EXISTS idx_corp_actions_div_due
    ON projection_corporate_actions(pay_date)
    WHERE status = 1 AND type = 2;
CREATE INDEX IF NOT EXISTS idx_corp_actions_symbol
    ON projection_corporate_actions(symbol);

-- Dividend record-date holder snapshots — written when the
-- reactor first observes record_date, read at pay_date.
CREATE TABLE IF NOT EXISTS projection_dividend_record_holders (
    action_id    TEXT NOT NULL,
    account_id   TEXT NOT NULL,
    shares       BIGINT NOT NULL,
    snapshotted_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (action_id, account_id)
);
```

## Phased rollout

Each step shippable; ordering matches dependency.

1. **Proto + corpaction aggregate.** New `corpaction.v1` proto
   package. Aggregate + commands + events for declare/apply/fail.
   Generated stubs in Go + webapp. Unit tests for the state machine.

2. **Reactor scaffolding + projection.** Migration 000031, projection
   for the action ledger, reactor with empty appliers, dispatch
   stub. Tests with fake projection.

3. **Splits — portfolio side.** `HoldingAdjusted` event + applier
   on the portfolio aggregate. `AppliedActions` dedup set.
   `ExecuteAdjustHolding` command. PG projection mirrors the qty
   change into `projection_holdings`. Tests cover forward (2-for-1)
   and reverse (1-for-10) ratios, including non-divisible
   quantities (round down — same as a real broker handling
   fractional shares).

4. **Splits — orderbook + sagas.** Reactor fans split out: cancel
   resting orders for the symbol (reuse `ExecuteCancelOrder` with
   reason `corporate_action:split`); walk in-flight TWAP/bracket/OCO
   sagas, dispatch adjustment commands. New `Adjust*` saga commands
   that scale `Quantity` and `LimitPrice` by the ratio. Tests
   exercise mid-TWAP adjust and bracket TP/SL price scaling.

5. **Cash dividends.** `DividendCredited` event + applier. New
   projection_dividend_record_holders. Reactor's first observation
   of `now >= record_date` takes the snapshot; pay-date applier
   credits cash from the snapshot. Two-tick test (record then pay)
   plus a "sold after record date" test confirming the seller still
   gets paid.

6. **Symbol changes.** `SymbolRenamed` + `SymbolMigrated` events
   and appliers. Orderbook aggregate stops accepting new orders
   post-rename. Portfolio key-rewrite in `Holdings` map. The new
   symbol's aggregate is created on first order against it (existing
   matching-engine factory pattern). Open orders + sagas cancel.

7. **SagaService.DeclareCorporateAction RPC + listing.** Operator
   declares via `saga.v1.SagaService.DeclareCorporateAction(plan)`
   — kept on SagaService because the action is itself a saga in
   the broader sense. List via existing diagnostics-style RPC on
   the corpaction package.

8. **Reactor wiring in cmd/xray + diagnostics.**
   `CorporateActionReactor.Status()` joins the operations card row
   on `/projections`. Env vars `CORPACTION_TICK_INTERVAL`
   (default 5m) and `CORPACTION_ENABLED` (default true) so the
   loop can be disabled.

9. **Webapp.** New `/corporate-actions` route with a declare form
   (symbol + type + type-specific fields + effective/record/pay
   dates) and a paginated list of declared actions with their
   status. Holdings rows that have been split-adjusted within the
   last 30 days show a small badge linking to the action.

## Edge cases — explicit tests

| Case | Expected |
|---|---|
| 2-for-1 split, 100 shares, $50 avg cost | 200 shares, $25 avg cost. TotalCost unchanged |
| 1-for-10 reverse split, 105 shares | 10 shares, 5 dropped (truncation). Audit event records the fractional residue |
| Split declared with effective_date in the past | Reactor applies on next tick — historical declarations work the same way |
| Replay applies same action twice | `AppliedActions[action_id]` short-circuits the second apply — no double effect |
| Dividend record-date snapshot occurs once | If the reactor ticks twice between record and pay, only the first writes the snapshot (PK collision = ignore) |
| Account sells all shares between record_date and pay_date | Still credited on pay_date from the record-date snapshot |
| Account opened position *after* record_date | Not in snapshot, not credited |
| Resting orders for the symbol exist at split time | All cancelled with `reason="corporate_action:split"`. UI surfaces them in the recent-cancellations list with the action ID |
| In-flight TWAP for the symbol | Remaining-qty scaled by ratio; limit_price scaled by inverse ratio. Future slices use the new values |
| Margin call active when split applies | The split's HoldingAdjusted event causes a mark-driven margin recheck on the next TradeExecuted — no special handling. (TODO follow-up: re-evaluate immediately) |
| Symbol change, then later split on the new symbol | Both apply independently — actions are keyed by action_id, not by symbol |
| Symbol change with open orders | Cancelled. Re-submission against the new symbol is operator's job |

## Tradeoffs and notes

- **Cancel open orders rather than re-prices on splits.** Real
  exchanges adjust open orders' qty + price on the morning of
  ex-date (open-order adjustment policy). For xray, the matching
  engine's time-priority semantics make adjustment fiddly — a
  pre-split limit at $150 becomes post-split $75, but a stop at
  $145 becomes $72.50 (still valid), and an order display at 25
  shares of a 1000-share order becomes 250 of 10,000 (legal, but
  the displayed slice's queue position is now meaningless).
  Cancelling with a structured reason is simpler, traceable in
  diagnostics, and matches what some retail brokers actually do for
  ambiguous corporate actions.

- **In-flight sagas cancel too, not adjust.** Original plan said
  sagas would adjust because "the math is clean"; on contact the
  math isn't actually clean — TWAP's `PlannedSliceQuantity` and
  `TotalFilled` are denominated in pre-split shares, brackets carry
  TP/SL trigger prices that need scaling, OCO carries shared
  share-cover holds. Each kind needs its own per-state-shape
  adjustment, and the user can re-submit a fresh saga against the
  post-split book trivially. We take the same out as we do for
  open orders: cancel with a structured reason and let the user
  re-submit. Proper adjustment is a worthwhile follow-up if
  cancellation friction becomes a real complaint.

- **No tax-lot accounting.** Splits adjust `avg_cost` proportionally
  (TotalCost preserved). A real broker tracks each tax lot
  separately so post-split lots retain their individual purchase
  dates and pre-split costs for capital-gains math. xray tracks
  only the aggregate position; splits scale the aggregate. Lot-level
  tracking is a separate (large) feature.

- **Dividends pay instantly, not T+1.** Real cash dividends settle on
  pay_date (which is already T+N from declaration). Layering xray's
  T+1 settlement on top of pay_date would be confusing — and ACH /
  wire credits to a brokerage account are typically considered
  "settled cash" immediately. So `DividendCredited` moves both
  `CashBalance` and `SettledCash`.

- **Reverse-split truncation logged, not paid.** A 1-for-10 reverse
  split of 105 shares yields 10 shares plus 5 "stranded" shares. Real
  brokers usually pay cash in lieu for the fractional residue. v1
  records the residue in the `HoldingAdjusted` event and drops the
  shares; cash-in-lieu is a follow-up.

- **Reactor as polling loop, not event-driven.** Same rationale as
  the settlement reactor: scheduling is wall-clock, and the
  projection is also the UI's "what's queued" answer. Periodic
  scans of a small table are cheap.

- **`corpaction.v1` as its own proto package, not under
  `portfolio.v1`.** The action *concept* spans portfolio, orderbook,
  and sagas; lifting it into its own package makes the dependency
  direction one-way (corpaction is consumed by everyone; corpaction
  itself imports nothing domain-specific). Keeps the import graph
  acyclic without ad-hoc interfaces.

## Follow-ups (not in v1)

- **Stock dividends.** Similar to splits but with a cost-basis
  twist: the dividend shares get their own cost basis (typically the
  ex-date market price) rather than diluting the existing avg_cost.
- **Mergers + spin-offs.** Multi-symbol: holders of A become holders
  of B (and sometimes cash). Architecturally: a `MultiLegAction`
  variant that fans out to multiple symbols per holder.
- **Cash in lieu for fractional residues** from reverse splits and
  spin-offs.
- **Open-order adjustment** instead of cancel. Especially useful
  for stop / trailing-stop orders that have meaningful trigger
  prices that should track the split.
- **Automatic re-margin-check** after splits and dividends — today
  the next TradeExecuted picks up the change; a forced recheck on
  the affected accounts would surface stale calls sooner.
- **Tax-lot accounting** as a substrate for splits to operate on.
- **Rights offerings.** A holder gets a tradeable instrument (the
  right) at declaration; the right expires worthless or is exercised
  into more shares. New aggregate kind.
