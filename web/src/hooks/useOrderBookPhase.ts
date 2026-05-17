import { useEffect, useState } from "react";
import { orderBookClient } from "../client";
import { MarketPhase } from "../gen/orderbook/v1/events_pb";

// useOrderBookPhase polls GetOrderBook every 5 seconds for the symbol's
// market phase. There's no server-pushed phase stream yet (a natural
// follow-up); for now the badge updates within a few seconds of any
// MarketPhaseChanged event.
export function useOrderBookPhase(symbol: string): MarketPhase {
  const [phase, setPhase] = useState<MarketPhase>(MarketPhase.CONTINUOUS);

  useEffect(() => {
    if (!symbol) return;
    let cancelled = false;

    const tick = async () => {
      try {
        const resp = await orderBookClient.getOrderBook({ symbol });
        if (!cancelled) {
          const p =
            resp.phase === MarketPhase.UNSPECIFIED
              ? MarketPhase.CONTINUOUS
              : resp.phase;
          setPhase(p);
        }
      } catch {
        // ignore transient errors — keep the previous phase
      }
    };

    tick();
    const id = setInterval(tick, 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [symbol]);

  return phase;
}

export function phaseLabel(phase: MarketPhase): string {
  switch (phase) {
    case MarketPhase.AUCTION:
      return "AUCTION";
    case MarketPhase.CLOSING_AUCTION:
      return "CLOSING AUCTION";
    case MarketPhase.CLOSED:
      return "CLOSED";
    case MarketPhase.CONTINUOUS:
    case MarketPhase.UNSPECIFIED:
    default:
      return "CONTINUOUS";
  }
}

export function phaseColor(phase: MarketPhase): string {
  switch (phase) {
    case MarketPhase.AUCTION:
      return "yellow";
    case MarketPhase.CLOSING_AUCTION:
      return "orange";
    case MarketPhase.CLOSED:
      return "red";
    case MarketPhase.CONTINUOUS:
    case MarketPhase.UNSPECIFIED:
    default:
      return "green";
  }
}
