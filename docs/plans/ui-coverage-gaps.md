# UI Coverage Gaps

## Context

The Go server exposes capabilities through five Connect services
(OrderBookService, PortfolioService, SagaService, DiagnosticsService,
TraderService) plus the broader event/projection model. The React webapp
in `webapp/` covers most of it, but a deliberate audit (server protos +
aggregate features vs. every `*.tsx` consumer under `webapp/app/`)
turned up a handful of real gaps where backend behavior exists but no
UI surface reaches it.

This doc records the gaps and sketches the smallest plausible shape for
each so any one can be picked up and turned into its own plan-and-ship
unit. It is intentionally an inventory, not a single implementation
plan — pick from it and expand the chosen item before building.

The audit also surfaced a couple of items that look like gaps but are
intentional (saga-routed write path, policy-constant fees); those are
recorded at the bottom so the next audit doesn't re-flag them.

## Verified gaps

For each item: where it lives on the server, what's missing in the UI,
and the rough shape of the work.

### 1. Stop orders in the order form

**Backend.** `proto/orderbook/v1/events.proto:31-32` defines
`ORDER_TYPE_STOP_MARKET` and `ORDER_TYPE_STOP_LIMIT`; the matching
engine supports them; the stop-side bookkeeping is in
`internal/orderbook/stopside.go` and the trailing variant code path
already exercises stop placement end-to-end.

**UI gap.** `webapp/app/components/OrderForm.tsx:29` lists only MARKET,
LIMIT, TRAILING_STOP_MARKET, TRAILING_STOP_LIMIT. The two non-trailing
stop types are missing from the dropdown.

**Shape.**
- Add two entries to `ORDER_TYPES` and the order-type segmented control.
- For STOP_MARKET: require a `stop_price` input (same widget as the
  trailing-stop "initial stop price" picker), no limit price.
- For STOP_LIMIT: require both `stop_price` and a `limit_price` (the
  resting limit once the stop triggers).
- Replicate the existing validation pattern that gates iceberg/trailing
  on TIF combinations. Stop orders accept GTC/DAY/IOC; FOK is
  meaningless for stops — match the engine's actual acceptance.
- One short test in `OrderForm` validation if any exists; otherwise just
  a manual smoke against `cmd/xray` to verify the placed-then-triggered
  flow.

**Why first.** Smallest change in absolute size, real user value, and
the engine has been carrying this code for a while without a UI tap.

### 2. Replace order

**Backend.** `proto/saga/v1/saga.proto:70` puts a `replace_order_id` on
`SingleOrderPlan`; the saga reactor treats a non-empty value as
cancel-and-place atomically. The full code path exists.

**UI gap.** `OrderForm.tsx:437,500` hardcodes `replaceOrderId: ""` on
every saga submission. No "Replace" affordance exists in the orders
table either — `PortfolioOrders` only offers Cancel.

**Shape.**
- Add a row-action "Replace" in the orders table that opens the
  OrderForm pre-populated from the selected order's current params, with
  the form remembering `replaceOrderId`.
- The submit path is the same `sagaClient.place(SingleOrderPlan)` —
  only `replaceOrderId` differs.
- No new server work.
- Test: place, then replace via UI, observe the original cancelled and
  the new one resting.

### 3. Margin breakdown not rendered

**Backend.** `GetMarginSnapshotResponse` already returns
`long_maintenance_requirement`, `short_maintenance_requirement`,
`collateral_pool`, and `proceeds_pool`. The values are computed and on
the wire today.

**UI gap.** `PortfolioPanel.tsx` displays only the aggregate margin
requirement and buying power. The long/short split, the collateral
pool, and the proceeds pool are silently dropped.

**Shape.**
- Add a small four-row table (or stat strip) in the margin section of
  `PortfolioPanel.tsx` that renders the four already-fetched fields.
- Show the proceeds + collateral pools only when the account holds
  shorts (avoid noise for long-only accounts).
- Zero proto / server changes.

### 4. Per-account fee and interest history

**Backend.** Three event types land on the portfolio aggregate today:
`TransactionFeeCharged`, `MarginInterestAccrued`, `ShortBorrowFeeAccrued`.
The fees accruer (`internal/feesaccruer/`) emits the latter two
periodically; the transaction-fee path runs on every fill.

**UI gap.** Per-order `fees_paid` is shown in the orders table, but
there's no per-account history panel. The events are queryable through
`/events` filtered to `portfolio:<account>` but that's a debug tool, not
a polished account view.

**Shape.**
- New `PortfolioFees` panel (or a tab inside `PortfolioPanel`) that
  reads from a small new projection over the three event types, keyed
  by account_id, columns: timestamp, kind (fee/interest/borrow),
  amount, related order or position.
- Projection lives in `internal/portfolio/projection_fees.go`,
  PG-backed like the existing per-account projections, with its own
  durable cursor.
- New RPC `ListFeeHistory(account_id, from, to, limit)` on
  PortfolioService.
- UI hook + table.
- Test the projection in isolation (memstore is fine); the UI can be
  smoked manually.

### 5. Indicative auction state during the auction window

**Backend.** Post-uncross imbalance is exposed and shown
(`markets.tsx:425-428`). During the AUCTION/CLOSING_AUCTION phases
themselves, real exchanges publish an indicative clearing price + the
remaining imbalance roughly once a second. xray's matching engine has
all the inputs to compute this — the uncross algorithm in
`internal/orderbook/auction.go` would just be invoked as a *preview*
rather than a commit.

**UI gap.** Nothing surfaces between BeginClosingAuction and Uncross
beyond the phase badge.

**Shape.**
- New in-memory projection
  `internal/orderbook/projection_indicative.go` that, while a symbol is
  in an auction phase, re-runs the uncross math against the current
  auction book + continuous book on a timer (e.g. 1Hz) and broadcasts
  `(symbol, indicative_price, imbalance_qty, imbalance_side)`.
- New streaming RPC `StreamIndicativeAuctionState(symbol)`.
- `MarketPanel` subscribes during auction phases and renders the
  rolling clearing price + imbalance bar.
- Disclosed in `docs/plans/periodic-auction.md` as a follow-up, including
  being the prerequisite for IO (imbalance-only) orders.

**Why interesting.** Most "looks like a real exchange" feature on the
list; non-trivial but well-scoped because the math already exists.

## Lesser items

These are real gaps but smaller in user impact or further from the
critical path. Listed for completeness so the next audit doesn't keep
re-finding them.

- **Replace from orders table** — covered as part of (2) above, but the
  Cancel-only action menu is the visible symptom.
- **Fee-schedule transparency** — fees are policy constants in
  `internal/margin/`. There's no "what would I be charged for X" view
  beyond the per-order preview impact. A static info panel could
  render the schedule from generated constants.
- **Per-symbol short-interest aggregate** — `projection_shorts.go`
  computes shorts-by-symbol but the UI only surfaces an account's own
  shorts. A venue-wide "short interest by symbol" panel would
  complement the markets view.
- **Background-process introspection** — fees accruer, reconciler, and
  margin-call reactor have no UI surface beyond their entries in
  `/projections`. The accruer's "last accrued at" per account, the
  reconciler's last-tick counts, and the margin reactor's grace timers
  are all observable server-side but not exposed.

## Intentionally not exposed

Recording so re-audits don't re-flag:

- **`PlaceOrder` / `CancelOrder` / `ReplaceOrder` unary RPCs on
  OrderBookService.** The UI deliberately writes through the saga
  service so cash holds, share holds, and OCO/bracket coordination
  happen consistently. The unary RPCs are correct for programmatic
  clients (loadgen, strategies) but bypassing the saga from the UI
  would skip the portfolio bookkeeping.
- **Fee constants are not editable.** They're policy
  (`internal/margin/`), not configuration. Read-only display is fine;
  write surface is not.
- **No "trigger margin call manually" button.** Margin calls are
  reactor-emitted on equity breach; manual issuance would be a debug
  feature, and the existing `cmd/loadtest` plus stale-price drift
  already exercises the path.

## How to use this doc

Pick one item, copy its "Shape" subsection into a new
`docs/plans/<item-name>.md`, expand it the way `async-snapshotter.md`
or `periodic-auction.md` did (file list, phased rollout, edge cases,
test plan), and ship.
