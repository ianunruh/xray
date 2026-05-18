# Indicative Auction State Implementation Plan

## Context

xray's auction lifecycle is OPEN-AUCTION → (orders rest, no matching) →
UNCROSS → CONTINUOUS, mirrored on the closing side. The post-uncross
result (`AuctionUncrossed`) carries the clearing price, matched
quantity, and standing imbalance, and the UI surfaces it as a one-shot
summary at `markets.tsx:425-428`. **During** the auction window itself
the UI shows only the phase badge — a black box for 30+ seconds.

Real exchanges publish an "indicative auction state" feed at roughly
1Hz during the auction: the clearing price the uncross would produce
*right now*, the matched quantity at that price, and the standing
imbalance. Market makers and arbitrageurs use it to decide whether to
add liquidity into the cross.

xray's `computeClearing` function in `internal/orderbook/auction.go`
already does exactly this math, with no side effects on the book — it
just reads aggregated price levels and returns an `auctionResult`. The
gap is exposure: a stream RPC that re-runs it on a timer while the
symbol is in an auction phase, plus a UI banner that renders the
running result.

Non-goals for v1: imbalance-only (IO) orders that consume this feed
(separate follow-up in `periodic-auction.md`); historical indicative
state replay (compute is cheap, recompute on demand); per-account
indicative buying-power preview.

## Architecture

```
write path (placed AT_OPEN/AT_CLOSE order)
   ↳ aggregate.Apply → broker.HandleEvents
       ↳ broker wakes StreamIndicativeAuctionState subscribers for symbol

stream handler loop
   ↳ on subscribe: compute and send initial state
   ↳ select:
       - ctx.Done           → return
       - broker channel     → compute and send (event-driven update)
       - 1Hz tick           → compute and send (heartbeat / phase poll)
   ↳ on each compute:
       handler.Load(symbol) → ComputeIndicative(book) → stream.Send
```

`handler.Load` reads the orderbook aggregate; the async snapshotter
keeps that load cheap (snapshot + replay of recent events). The
projection-style in-memory shadow that the depth projection uses isn't
needed — the math is small and computed on demand.

```
# Proto
proto/orderbook/v1/events.proto       # IndicativeAuctionState message
proto/orderbook/v1/service.proto      # StreamIndicativeAuctionState rpc

# Engine (orderbook package)
internal/orderbook/indicative.go      # NEW: ComputeIndicative wraps
                                      #   computeClearing for the projection
                                      #   case (returns nil outside auction)
internal/orderbook/indicative_test.go # NEW: table-driven tests covering
                                      #   the same edge cases as auction_test.go

# Server
internal/orderbook/server.go          # StreamIndicativeAuctionState handler

# Webapp
webapp/app/hooks/useIndicativeAuctionState.ts  # NEW
webapp/app/components/MarketPanel.tsx          # IndicativeAuctionBanner
                                               # rendered when phase is
                                               # AUCTION / CLOSING_AUCTION
```

## Proto changes

```protobuf
// In events.proto (so it ships alongside Side / MarketPhase that it
// references — service.proto already imports events.proto).
message IndicativeAuctionState {
  string symbol = 1;
  MarketPhase phase = 2;
  // The price the uncross would clear at right now. Zero when there
  // is no cross possible (one-sided book, no reference price for pure
  // market crosses, no overlap).
  int64 indicative_price = 3;
  // Quantity that would trade at indicative_price.
  int64 matched_qty = 4;
  // Standing imbalance at indicative_price (always >= 0; side carries
  // the direction). Zero matched_qty + non-zero imbalance is the
  // one-sided-book case.
  int64 imbalance_qty = 5;
  Side imbalance_side = 6;
  // When the snapshot was computed (server clock).
  google.protobuf.Timestamp computed_at = 7;
}

// In service.proto
rpc StreamIndicativeAuctionState(StreamIndicativeAuctionStateRequest)
    returns (stream IndicativeAuctionState);

message StreamIndicativeAuctionStateRequest {
  string symbol = 1;
}
```

The stream always sends the current phase, so the client can stop
rendering when phase transitions out of an auction (the server keeps
the subscription open across transitions — closing on transition would
race with the very response that announces it).

## `ComputeIndicative` helper

```go
// internal/orderbook/indicative.go

// IndicativeState is the live "what would uncross do right now" view
// for a symbol in an auction phase.
type IndicativeState struct {
    Symbol        string
    Phase         MarketPhase
    ClearingPrice int64
    MatchedQty    int64
    ImbalanceQty  int64
    ImbalanceSide Side
}

// ComputeIndicative runs computeClearing against the auction book
// appropriate for the current phase. Returns nil when the orderbook is
// not in an auction phase — callers can use that as a "no banner needed"
// signal.
func ComputeIndicative(book *OrderBook) *IndicativeState {
    var ct CrossType
    switch book.Phase {
    case PhaseAuction:
        ct = CrossOpening
    case PhaseClosingAuction:
        ct = CrossClosing
    default:
        return nil
    }
    res := computeClearing(book, ct)
    return &IndicativeState{
        Symbol:        book.Symbol,
        Phase:         book.Phase,
        ClearingPrice: res.ClearingPrice,
        MatchedQty:    res.MatchedQty,
        ImbalanceQty:  res.ImbalanceQty,
        ImbalanceSide: res.ImbalanceSide,
    }
}
```

No export of `computeClearing` is needed — the helper lives in the same
package. The math is purely a read, so concurrent calls during writes
are safe as long as the aggregate snapshot returned by `handler.Load`
is a consistent point-in-time view (it is — Load loads from store, not
from the in-flight in-memory aggregate).

## Server handler

```go
// internal/orderbook/server.go

func (s *Server) StreamIndicativeAuctionState(
    ctx context.Context,
    req *connect.Request[orderbookv1.StreamIndicativeAuctionStateRequest],
    stream *connect.ServerStream[orderbookv1.IndicativeAuctionState],
) error {
    symbol := req.Msg.Symbol

    id, ch := s.broker.Subscribe(symbol)
    defer s.broker.Unsubscribe(id)

    t := time.NewTicker(time.Second)
    defer t.Stop()

    send := func() error {
        book, err := s.handler.Load(ctx, AggregateID(symbol))
        if err != nil {
            return err
        }
        out := &orderbookv1.IndicativeAuctionState{
            Symbol:     symbol,
            Phase:      MarketPhaseToProto(book.Phase),
            ComputedAt: timestamppb.Now(),
        }
        if ind := ComputeIndicative(book); ind != nil {
            out.IndicativePrice = ind.ClearingPrice
            out.MatchedQty = ind.MatchedQty
            out.ImbalanceQty = ind.ImbalanceQty
            out.ImbalanceSide = SideToProto(ind.ImbalanceSide)
        }
        return stream.Send(out)
    }

    if err := send(); err != nil {
        return err
    }
    for {
        select {
        case <-ctx.Done():
            return nil
        case _, ok := <-ch:
            if !ok {
                return nil
            }
            if err := send(); err != nil {
                return err
            }
        case <-t.C:
            if err := send(); err != nil {
                return err
            }
        }
    }
}
```

The broker's existing filter already wakes subscribers on
`OrderPlaced`, `OrderCancelled`, `TradeExecuted`, and `StopTriggered`.
`MarketPhaseChanged` is *not* in that filter; phase transitions are
picked up on the next tick (<= 1s lag). Acceptable for v1; if it ever
matters, add the case to `broker.HandleEvents` rather than building a
second subscription channel.

## Webapp

### Hook

```ts
// webapp/app/hooks/useIndicativeAuctionState.ts

import { useEffect, useState } from "react";
import { orderBookClient } from "~/lib/client";
import type { IndicativeAuctionState } from "../../src/gen/orderbook/v1/events_pb";

// useIndicativeAuctionState subscribes to the indicative auction stream
// for `symbol`. Returns null when no message has arrived yet or when
// the latest message is from a non-auction phase. Pass `enabled=false`
// to skip the subscription entirely (typical: the page is in replay
// mode or the user hasn't picked a symbol).
export function useIndicativeAuctionState(symbol: string, enabled: boolean): IndicativeAuctionState | null {
    const [state, setState] = useState<IndicativeAuctionState | null>(null);

    useEffect(() => {
        if (!enabled || !symbol) {
            setState(null);
            return;
        }
        const ac = new AbortController();
        (async () => {
            try {
                const iter = orderBookClient.streamIndicativeAuctionState(
                    { symbol },
                    { signal: ac.signal },
                );
                for await (const msg of iter) {
                    setState(msg);
                }
            } catch (e) {
                if (!ac.signal.aborted) {
                    // Transient — leave state in place; the stream
                    // hook reconnects when `enabled` toggles or symbol
                    // changes.
                }
            }
        })();
        return () => ac.abort();
    }, [symbol, enabled]);

    return state;
}
```

### MarketPanel banner

A small card rendered in `LiveBody` (already inside the auction-aware
phase block) when `state?.phase` is `AUCTION` or `CLOSING_AUCTION`:

```
┌──────────────────────────────────────────────────┐
│  Indicative cross                                 │
│  Price       Match Qty       Imbalance            │
│  $150.42     1,250           480 buy ▶            │
│                                              1s ago │
└──────────────────────────────────────────────────┘
```

- Price formatted via `formatPrice`; "—" when `indicative_price == 0`.
- Imbalance side: shaded green for buy, red for sell, with the
  quantity. When `imbalance_qty == 0` show "—".
- `computed_at` becomes a small "Ns ago" label; refreshes locally on a
  500ms tick so it doesn't go stale-looking between server updates.

Renders only when `phase ∈ {AUCTION, CLOSING_AUCTION}`. The hook
subscribes whenever the user is viewing a symbol; the client-side phase
filter decides whether to show the card. This avoids dropping/reopening
the subscription on every phase change.

## Phased rollout

Each step shippable; system stays green between commits.

1. **Engine helper + tests.** New `internal/orderbook/indicative.go`
   with `IndicativeState` + `ComputeIndicative`. Table-driven test
   `indicative_test.go` mirroring the matrix from `auction_test.go`:
   empty book, one-sided, crossed limits, only market orders with no
   reference, balanced/buy-heavy/sell-heavy clearing. Covers the
   "returns nil when phase is CONTINUOUS/CLOSED" path. No proto, no
   wire surface yet.

2. **Proto + generated code.** Add `IndicativeAuctionState`,
   `StreamIndicativeAuctionState`, `StreamIndicativeAuctionStateRequest`
   to the two proto files. Run `buf generate` and `cd webapp && buf
   generate`. Verify the generated `*Connect` clients gain
   `streamIndicativeAuctionState`.

3. **Server handler.** Implement `Server.StreamIndicativeAuctionState`
   using broker + 1Hz ticker. Manual smoke: open an auction, place
   crossing AT_OPEN orders, subscribe via `grpcurl` or browser dev
   tools, observe state updates on each placement and on the timer
   tick.

4. **Webapp hook + banner.** New
   `useIndicativeAuctionState` hook. `MarketPanel.LiveBody` renders
   the banner when `state?.phase ∈ {AUCTION, CLOSING_AUCTION}`.
   Verify in browser through a full open → uncross cycle.

## Edge cases — explicit tests

| Case | Expected |
|---|---|
| Symbol in CONTINUOUS | `ComputeIndicative` returns nil; stream sends an `IndicativeAuctionState{phase=CONTINUOUS, indicative_price=0}` |
| Empty auction book | `matched_qty=0`, `imbalance_qty=0`, `indicative_price=0` |
| One-sided auction book (bids only) | `matched_qty=0`, `imbalance_qty=totalBids`, `imbalance_side=BUY` |
| Crossed limit book mid-auction | `indicative_price` matches `computeClearing` for that state |
| Market AT_OPEN orders with no reference price | `matched_qty=0` (same fallback as the real uncross) |
| Phase transition during stream | Next tick (≤ 1s) sends the new phase; client banner hides on its own |
| Stream client disconnects mid-auction | Server unsubscribes from broker, frees the channel; no leak |
| Aggregate load fails (e.g. context cancelled) | `send` propagates the error; handler returns; client retries via hook |

## Tradeoffs and notes

- **Why poll the aggregate every tick instead of maintaining an
  in-memory shadow.** The math reads aggregated price levels per side
  (≤ a few hundred entries for any realistic book), and `handler.Load`
  on the orderbook is snapshot-backed and cheap. A shadow projection
  would duplicate `aggregate.go` bookkeeping (resting orders, OCO
  groups, iceberg slices, auction-book partitions) just to feed the
  same math. Skip it until profiling says otherwise.
- **Why 1Hz heartbeat instead of pure event-driven.** Two reasons:
  picks up phase transitions without subscribing to
  `MarketPhaseChanged` separately, and matches real exchange cadence
  (clients expect a regular update even when nothing's moving). The
  cost is one aggregate load per active-auction symbol per second —
  trivial.
- **Why one stream per (symbol, subscriber) rather than a fan-out
  projection.** Symmetric to `StreamMarketDepth` and `StreamTrades`.
  Keeps the server stateless and means subscribers can come and go
  freely. A fan-out projection becomes worthwhile if dozens of clients
  subscribe to the same symbol — not relevant for xray's scale.

## Follow-ups (not in v1)

- **MarketPhaseChanged in broker filter.** Cuts the phase-transition
  lag from up to 1s to immediate. Two-line change in `broker.go`.
- **Imbalance-only (IO) orders.** Real prerequisite for this feed in
  production exchanges; see `docs/plans/periodic-auction.md`'s
  follow-ups section. IO orders need the indicative state computed
  server-side at order-placement time to gate "would this shrink the
  imbalance" acceptance.
- **Historical indicative replay.** Useful for post-trade analysis;
  needs to be a real projection (versioned snapshots of the auction
  state) since recomputing requires the historical aggregate state.
- **Per-account indicative buying-power preview.** "What would my
  margin look like if the cross happened at this price right now?" —
  a portfolio-side helper that consumes this stream.
