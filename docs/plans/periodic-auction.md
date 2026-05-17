# Periodic Auction Implementation Plan

## Context

The xray orderbook is purely continuous: every `PlaceOrder` matches inline against resting liquidity. Real equity exchanges (NYSE, Nasdaq, LSE, Xetra, JPX) instead bracket the trading day with **call auctions** — an opening cross and a closing cross. Orders accumulate during an auction phase without crossing; at the uncross moment, a single equilibrium price is computed and all eligible orders trade at that one price. The closing cross is the authoritative end-of-day print used for fund NAVs, index levels, and mark-to-market.

This plan adds opening and closing auctions to xray as a first-class market-phase concept, plus auction-only order types (MOO/LOO/MOC/LOC) and an explicit "official close" output.

Non-goals for v1: intraday halt/reopen auctions (mechanically free once the machinery exists — follow-up), Imbalance-Only (IO) orders, parallel continuous + auction-collection during the last 10 minutes of the session, after-hours trading, multi-session bookkeeping.

## Architecture

The orderbook aggregate gains an explicit `Phase` field that gates how `PlaceOrder` behaves. Continuous matching only runs in `CONTINUOUS`. During `AUCTION` (opening) and `CLOSING_AUCTION`, orders rest in partitioned auction books without matching. A dedicated `Uncross` command runs the equilibrium-price algorithm and emits a batch of `TradeExecuted` events at one clearing price, plus an `OfficialCloseSet` event when it's the closing uncross.

```
                  Open                Uncross                BeginClosingAuction         Uncross
   AUCTION ──────────────▶ CONTINUOUS ────────────▶ CONTINUOUS ────────────────────▶ CLOSING_AUCTION ──────────▶ CLOSED
   (opening)               (continuous matching)                                     (continuous frozen;
                                                                                      AT_CLOSE + cancels only)
```

```
# Proto changes
proto/orderbook/v1/events.proto          # MarketPhase enum, MarketPhaseChanged, AuctionUncrossed,
                                         #   OfficialCloseSet, CrossType enum, cross_type on TradeExecuted,
                                         #   AT_OPEN / AT_CLOSE in TimeInForce
proto/orderbook/v1/service.proto         # OpenAuction, BeginClosingAuction, Uncross RPCs;
                                         #   GetOfficialClose, ListOfficialCloses;
                                         #   phase on GetOrderBook response
proto/orderbook/v1/snapshots.proto       # Phase field + auction-book partitions

# Aggregate + matching
internal/orderbook/aggregate.go          # Phase, OpeningBook, ClosingBook; Apply for new events
internal/orderbook/commands.go           # OpenAuction, BeginClosingAuction, Uncross commands;
                                         #   PlaceOrder phase-aware validation + auction-book routing
internal/orderbook/auction.go            # NEW: auctionBook type, uncross algorithm,
                                         #   clearing-price computation, allocation
internal/orderbook/auction_test.go       # NEW: table-driven uncross tests (the bug-prone part)
internal/orderbook/snapshot.go           # Persist/restore phase + auction books
internal/orderbook/server.go             # RPC handlers for the new commands + GetOfficialClose
internal/orderbook/projection_close_pg.go # NEW: daily_close projection
internal/orderbook/projection_pnl_pg.go  # Mark to OfficialCloseSet.close_price at session end

# Storage
pkg/es/pgstore/migrations/               # NEW migration: daily_close table

# Strategies (read phase before acting)
internal/trader/                         # Helper: GetMarketPhase / pause-during-auction utility
internal/mm/engine.go                    # Skip requote during auction
internal/noise/engine.go                 # Keep posting GTC limits during auction (builds indicative book)
internal/trend/engine.go                 # Suppress signal acting during auction

# Web UI
web/src/                                 # Phase badge in symbol header;
                                         #   closing-print highlight in trade tape;
                                         #   "official close: $X" footer
```

## Phase machine

```go
type MarketPhase int

const (
    PhaseContinuous     MarketPhase = iota  // default; matches existing behavior
    PhaseAuction                            // opening auction
    PhaseClosingAuction                     // closing auction (continuous frozen)
    PhaseClosed                             // terminal until next session
)
```

Transitions and accepted operations:

| Phase | New regular order | New AT_OPEN | New AT_CLOSE | Cancel | Match inline | Next valid command |
|---|---|---|---|---|---|---|
| `AUCTION` (opening) | accept, no match | accept | accept | yes | no | `Uncross` |
| `CONTINUOUS` | accept, match | reject (`missed_auction_window`) | accept until cutoff | yes | yes | `BeginClosingAuction` |
| `CLOSING_AUCTION` | reject (`closing_auction_active`) | reject | accept | yes | no | `Uncross` |
| `CLOSED` | reject (`market_closed`) | reject | reject | yes | no | `OpenAuction` (next session) |

Default phase for an aggregate that has no `MarketPhaseChanged` events in its history is `CONTINUOUS` — existing tests stay green without modification.

## New events

```proto
enum MarketPhase {
  MARKET_PHASE_UNSPECIFIED      = 0;
  MARKET_PHASE_CONTINUOUS       = 1;
  MARKET_PHASE_AUCTION          = 2;  // opening
  MARKET_PHASE_CLOSING_AUCTION  = 3;
  MARKET_PHASE_CLOSED           = 4;
}

enum CrossType {
  CROSS_TYPE_NONE        = 0;  // continuous trade
  CROSS_TYPE_OPENING     = 1;
  CROSS_TYPE_CLOSING     = 2;
  CROSS_TYPE_HALT_REOPEN = 3;  // reserved for follow-up
}

message MarketPhaseChanged {
  string                       symbol = 1;
  MarketPhase                  phase  = 2;
  string                       reason = 3;   // "session_open", "session_close", "manual", etc.
  google.protobuf.Timestamp    at     = 4;
}

message AuctionUncrossed {
  string                       symbol         = 1;
  int64                        clearing_price = 2;
  int64                        matched_qty    = 3;
  int64                        imbalance_qty  = 4;  // unmatched at clearing price
  Side                         imbalance_side = 5;
  google.protobuf.Timestamp    at             = 6;
}

message OfficialCloseSet {
  string                       symbol       = 1;
  string                       session_date = 2;   // "2026-05-16"
  int64                        close_price  = 3;
  int64                        close_volume = 4;   // = matched_qty of the closing uncross
  google.protobuf.Timestamp    at           = 5;
}

// TradeExecuted gains:
message TradeExecuted {
  // ...existing fields
  CrossType cross_type = 8;
}

// TimeInForce gains:
enum TimeInForce {
  // ...existing
  TIME_IN_FORCE_AT_OPEN  = 5;  // MOO/LOO depending on OrderType
  TIME_IN_FORCE_AT_CLOSE = 6;  // MOC/LOC depending on OrderType
}
```

`OrderPlaced` is unchanged — auction-binding is carried on `TimeInForce`. Whether an order is routed to the continuous book or an auction book is derived from `(OrderType, TimeInForce)` in `Apply`.

## Auction-only orders — MOO/LOO/MOC/LOC

Cleanest mapping is `(OrderType, TimeInForce)` orthogonal:

| Industry name | OrderType | TimeInForce |
|---|---|---|
| MOO | Market | AT_OPEN |
| LOO | Limit  | AT_OPEN |
| MOC | Market | AT_CLOSE |
| LOC | Limit  | AT_CLOSE |

Validation rules added in `ExecutePlaceOrder`:

- `TIME_IN_FORCE_AT_OPEN` accepted in phase `AUCTION` only (rejected in `CONTINUOUS` so AT_OPEN can't be staged days in advance — pragmatically simpler).
- `TIME_IN_FORCE_AT_CLOSE` accepted in `CONTINUOUS` (up to a cutoff) and in `CLOSING_AUCTION`.
- Auction TIF + Stop OrderType → `ErrAuctionStopNotAllowed`.
- Auction TIF orders never enter continuous matching — `matchAndAppend` is skipped for them.
- Unfilled auction orders are cancelled in the same batch as `AuctionUncrossed` with reason `"missed_auction"`.

Aggregate state grows two partitioned books so the continuous depth stream stays clean:

```go
type OrderBook struct {
    // ...existing
    Phase       MarketPhase
    OpeningBook *auctionBook  // AT_OPEN orders
    ClosingBook *auctionBook  // AT_CLOSE orders
}

type auctionBook struct {
    BuyOrders  []*Order  // price-time priority
    SellOrders []*Order
}
```

Routing in `applyOrderPlaced`: if `TimeInForce == AT_OPEN` → `OpeningBook`; if `AT_CLOSE` → `ClosingBook`; else existing logic.

## New commands

```go
type OpenAuction struct {
    Symbol string
    Reason string  // "session_open" by default
}

type BeginClosingAuction struct {
    Symbol string
    Reason string
}

type Uncross struct {
    Symbol string
}
```

- `OpenAuction`: only valid from `CLOSED` (or from a fresh aggregate with no phase events). Emits `MarketPhaseChanged(AUCTION)`.
- `BeginClosingAuction`: only valid from `CONTINUOUS`. Emits `MarketPhaseChanged(CLOSING_AUCTION)`. After this, continuous matching is frozen.
- `Uncross`: valid from `AUCTION` or `CLOSING_AUCTION`. Runs the algorithm below and emits one atomic event batch.

**Closing cutoff:** in v1 the cutoff coincides with `BeginClosingAuction` — there's no "limit window where only IO orders are accepted" since IO orders are deferred to a follow-up. This is enforced naturally: `AT_CLOSE` orders are accepted while `CONTINUOUS`, rejected once `CLOSING_AUCTION` begins for *new arrivals* (already-resting AT_CLOSE orders remain). The cutoff timing is the operator's responsibility — they call `BeginClosingAuction` when they want the cutoff to bite.

## Uncross algorithm

Single-price equilibrium uncross, standard NYSE/Nasdaq-style tie-breakers. Implemented in `internal/orderbook/auction.go`.

Inputs at uncross time:
- All limit orders from the continuous `Bids`/`Asks` plus the relevant auction book (`OpeningBook` for opening, `ClosingBook` for closing).
- All market orders from the auction book (AT_OPEN/AT_CLOSE Market).
- The previous session's reference price (for tie-breaking — use last continuous trade, or 0 if none).

Algorithm:

1. **Build cumulative curves.** For each distinct price `p` on the merged book:
   - `BuyQty(p)` = sum of all buy orders with `price ≥ p`, plus all buy market orders.
   - `SellQty(p)` = sum of all sell orders with `price ≤ p`, plus all sell market orders.
2. **Compute `Matched(p) = min(BuyQty(p), SellQty(p))`.** Walk every candidate price (every distinct limit price on either side) and find the set maximizing `Matched`.
3. **Tie-breakers,** applied in order:
   1. Minimize `|BuyQty(p) − SellQty(p)|`.
   2. If buy imbalance: pick the *highest* remaining candidate. If sell imbalance: pick the *lowest*. If balanced: pick the midpoint of the remaining range, snapped to the reference price if it lies within.
4. **Allocate fills** at the clearing price, price-time priority within each side, market orders first within their side:
   - Walk eligible buys (price ≥ clearing, plus all market buys) in priority order.
   - Walk eligible sells (price ≤ clearing, plus all market sells) in priority order.
   - Pair them; for each pair emit `TradeExecuted{price=clearing, cross_type=OPENING|CLOSING}`.
5. **Self-trade prevention:** when a pair has the same `account_id`, skip the pair (advance the priority queue on both sides). Do NOT cancel either order — that's a continuous-phase concept.
6. **OCO groups:** atomic-cancel still fires per trade as in continuous matching (reuses `cancelOCOSiblings`).
7. **Post-uncross cleanup:**
   - Unfilled auction-only orders (AT_OPEN/AT_CLOSE) → `OrderCancelled{reason:"missed_auction"}`.
   - Unfilled regular limits at the clearing price → remain resting on the continuous book (they were always there).
   - Run `triggerStops` once against `clearing_price` so any pre-existing stops fire on the print.

Event order in the batch:
```
MarketPhaseChanged(prev → new_phase)    // CONTINUOUS or CLOSED
AuctionUncrossed
TradeExecuted × N                       // all stamped cross_type=OPENING|CLOSING
OrderCancelled × M                      // missed_auction + oco_triggered siblings
StopTriggered × K                       // stops triggered by clearing_price
TradeExecuted × J                       // stops that activated and immediately matched (closing case only — opening keeps these for the first continuous tick)
OfficialCloseSet                        // closing uncross only
```

For the **closing** uncross specifically: phase flips to `CLOSED`, so stops can't activate (the continuous book is dead until next session). Either suppress `triggerStops` on closing uncross, or expire the activated orders immediately. Choose the former — simpler.

### Edge cases — explicit tests

| Case | Expected outcome |
|---|---|
| Empty book at uncross | `AuctionUncrossed{matched_qty:0}`, phase flips, no trades |
| All market orders, no limits | No reference price exists → cancel all market orders with reason `"auction_no_reference_price"`, `matched_qty:0` |
| Only one side has orders | `matched_qty:0`, imbalance = entire side's volume |
| Crossed limits without market orders | Clearing price between best bid and best ask |
| Two prices tie on matched and imbalance | Use reference price, else midpoint |
| Mid-auction cancellations | Allowed, normal `CancelOrder` path |
| Self-trade across the cross | Pair skipped, both orders remain in priority queue for next pair |
| OCO straddling clearing price | First-filled cancels sibling in same batch |
| Stop placed during auction | Rests; checked once against clearing price after opening uncross |
| Pre-existing GTC at clearing price | Remains on continuous book post-uncross |

## Official close — `OfficialCloseSet` and `daily_close` projection

`OfficialCloseSet` is redundant with the closing `TradeExecuted` events (you could derive it by scanning for the last `cross_type=CLOSING` trade), but it exists so downstream consumers don't have to. It's the canonical end-of-day mark.

New projection `internal/orderbook/projection_close_pg.go`:

```sql
CREATE TABLE daily_close (
    symbol       TEXT NOT NULL,
    session_date DATE NOT NULL,
    close_price  BIGINT NOT NULL,
    close_volume BIGINT NOT NULL,
    closed_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, session_date)
);
```

Single-purpose projection subscribed to `OfficialCloseSet` only. Its own NATS durable cursor like other PG projections.

New RPCs on `OrderBookService`:
- `GetOfficialClose(symbol, session_date)` → returns the row or NOT_FOUND.
- `ListOfficialCloses(symbol, from, to)` → for charting.

### Knock-on changes to existing projections

- **Candles** — informational only; the closing trade naturally becomes the bar's `close`. Optional: tag the day's last bar with `is_closing_bar=true` for UI.
- **P&L (`projection_pnl_pg.go`)** — mark unrealized positions to `OfficialCloseSet.close_price` at session end instead of "last trade I saw." This is the meaningful behavior change — realized vs. unrealized P&L gets a stable boundary.
- **Trades broker** — frontend renders `cross_type != NONE` trades with a badge.

## Strategy adjustments

All three strategies need to read `Phase` before acting. Add a small helper in `internal/trader/` that exposes the current phase (cached + invalidated on `MarketPhaseChanged` from the orderbook event stream).

- **xray-mm**: skip requote when phase ≠ CONTINUOUS. Cancel-on-shutdown should also cancel any of its AT_OPEN/AT_CLOSE orders. Future: opt in to quoting opening auctions with LOO orders.
- **xray-noise**: keep posting GTC limits during AUCTION (this is realistic — noise traders pile into the auction); skip market orders during AUCTION (would be rejected).
- **xray-trend**: suppress signal acting during AUCTION and CLOSING_AUCTION (can't get a fill). Resume on `MarketPhaseChanged(CONTINUOUS)`.

## Web UI

Minimal additions:
- Phase badge in symbol header: `CONTINUOUS` / `AUCTION 09:29:58` / `CLOSING AUCTION 15:59:55` / `CLOSED`. Drives off the depth/trades stream which now includes phase.
- Trade tape: visual highlight for `cross_type != NONE` trades — large badge for closing print ("OFFICIAL CLOSE: $192.45 · 1.2M shares").
- Symbol footer: "Last official close: $X on YYYY-MM-DD" sourced from `GetOfficialClose`.
- Order form: disable Market order type during AUCTION phases; offer `AT_OPEN` / `AT_CLOSE` TIFs as new dropdown options.

## Phased rollout

Each step is independently shippable and leaves the system in a working state.

1. **Phase machinery + opening auction (no AT_OPEN yet).**
   Proto: `MarketPhase`, `MarketPhaseChanged`, `AuctionUncrossed`, `CrossType`, `cross_type` on `TradeExecuted`. Aggregate `Phase` field with default `CONTINUOUS` (existing tests untouched). `OpenAuction` and `Uncross` commands + RPCs. Uncross algorithm operates on regular limit orders only. PlaceOrder accepts orders without matching during AUCTION; rejects IOC/FOK. Comprehensive table-driven uncross tests.

2. **AT_OPEN order type.**
   `TIME_IN_FORCE_AT_OPEN`, `OpeningBook` partition on the aggregate, lifecycle validation, "missed_auction" cancellations at uncross. Market orders allowed via AT_OPEN. Uncross algorithm extended to merge OpeningBook into the curves.

3. **Closing auction (+ AT_CLOSE).**
   `MARKET_PHASE_CLOSING_AUCTION`, `BeginClosingAuction` command + RPC, `TIME_IN_FORCE_AT_CLOSE`, `ClosingBook` partition, closing-side `Uncross` branch (flips to CLOSED). Validation that regular orders rejected during CLOSING_AUCTION.

4. **Official close output.**
   `OfficialCloseSet` event emitted on closing uncross. `daily_close` projection + migration. `GetOfficialClose` / `ListOfficialCloses` RPCs. P&L projection marks to `OfficialCloseSet.close_price`.

5. **Strategies.**
   Phase-aware behavior in mm, noise, trend. Trader-package helper for phase access.

6. **Web UI.**
   Phase badge, closing-print highlight, official-close footer, AT_OPEN/AT_CLOSE in order form.

## Follow-ups (not in v1)

- **Intraday halt/reopen auctions.** Use `CROSS_TYPE_HALT_REOPEN`. Trigger from LULD-style band logic or a manual `HaltSymbol` command. Reuses everything — just another phase transition into and out of `AUCTION`.
- **Imbalance Only (IO) orders.** Requires the imbalance projection (server-side `IndicativeAuctionState` recomputing clearing price every N ms during auction) as a prerequisite. Then add a TIF/flag for IO and gate acceptance on "this order shrinks the imbalance."
- **Parallel continuous + auction collection in the last 10 min.** Real exchanges keep continuous matching alive while accumulating MOC/LOC. Adds a `PRE_CLOSE` phase. Adds little to the educational story; defer.
- **After-hours phase (`AFTER_HOURS`).** ECN-style continuous matching post-CLOSED. Straightforward additional phase.
- **Market clock.** New `internal/marketclock/` service that fires `OpenAuction` / `Uncross` / `BeginClosingAuction` / `Uncross` on a schedule (e.g. `open=09:30, uncross=09:30:00.5, begin_close=15:59:55, uncross=16:00:00`). Until then, drive transitions manually via RPC.
- **Indicative auction state stream.** Server-side projection broadcasting (`indicative_price`, `imbalance_qty`, `imbalance_side`) every ~1s during AUCTION/CLOSING_AUCTION so the UI doesn't have to compute it client-side. Prereq for IO orders.
- **Session bookkeeping.** A `TradingSession` aggregate keyed by `(market, session_date)` that owns the phase transitions and emits session-open/close events. Lets you query historical sessions cleanly. Skipped in v1 — derive `session_date` from event timestamps.
