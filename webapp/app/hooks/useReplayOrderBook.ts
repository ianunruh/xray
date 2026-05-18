import { useEffect, useState } from "react";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { orderBookClient } from "~/lib/client";
import { MarketPhase } from "../../src/gen/orderbook/v1/events_pb";
import type {
  OrderSummary,
  PriceLevel,
  ReplayOrderBookResponse,
  Trade,
} from "../../src/gen/orderbook/v1/service_pb";

// ReplayTarget identifies a point in the event stream. Use {date} for
// timestamp-driven scrubbing (the natural axis for a time slider) and
// {version} for exact jumps like start/end buttons — version is precise,
// timestamps lose sub-millisecond detail in the JS Date round-trip and can
// undershoot real events.
export type ReplayTarget =
  | { kind: "date"; date: Date }
  | { kind: "version"; version: number };

export type ReplaySnapshot = {
  atVersion: number;
  atDate: Date | null;
  phase: MarketPhase;
  bids: PriceLevel[];
  asks: PriceLevel[];
  orders: OrderSummary[];
  recentTrades: Trade[];
};

const DEBOUNCE_MS = 120;

function targetKey(t: ReplayTarget | null): string | null {
  if (!t) return null;
  return t.kind === "date" ? `d:${t.date.getTime()}` : `v:${t.version}`;
}

// useReplayOrderBook fetches the orderbook state at the given target.
// Requests are debounced so dragging a slider doesn't spam the server.
export function useReplayOrderBook(
  symbol: string,
  target: ReplayTarget | null,
) {
  const [snapshot, setSnapshot] = useState<ReplaySnapshot | null>(null);
  const [loading, setLoading] = useState(false);

  const key = targetKey(target);

  useEffect(() => {
    if (!symbol || !target) {
      setSnapshot(null);
      return;
    }

    let cancelled = false;
    const handle = setTimeout(async () => {
      setLoading(true);
      try {
        const at =
          target.kind === "date"
            ? {
                case: "atTimestamp" as const,
                value: timestampFromDate(target.date),
              }
            : { case: "atVersion" as const, value: target.version };
        const resp: ReplayOrderBookResponse =
          await orderBookClient.replayOrderBook({
            symbol,
            at,
            tradeLimit: 50,
          });
        if (cancelled) return;
        setSnapshot({
          atVersion: resp.atVersion,
          atDate: resp.atTimestamp
            ? new Date(
                Number(resp.atTimestamp.seconds) * 1000 +
                  Math.floor(resp.atTimestamp.nanos / 1_000_000),
              )
            : null,
          phase:
            resp.phase === MarketPhase.UNSPECIFIED
              ? MarketPhase.CONTINUOUS
              : resp.phase,
          bids: resp.bids,
          asks: resp.asks,
          orders: resp.orders,
          recentTrades: resp.recentTrades,
        });
      } catch {
        // ignore transient errors — keep previous snapshot visible
      } finally {
        if (!cancelled) setLoading(false);
      }
    }, DEBOUNCE_MS);

    return () => {
      cancelled = true;
      clearTimeout(handle);
    };
  }, [symbol, key]);

  return { snapshot, loading };
}
