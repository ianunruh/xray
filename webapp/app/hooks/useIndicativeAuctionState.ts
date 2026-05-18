import { useState } from "react";
import { orderBookClient } from "~/lib/client";
import type { IndicativeAuctionState } from "../../src/gen/orderbook/v1/service_pb";
import { useStream } from "./useStream";

// useIndicativeAuctionState subscribes to the live indicative auction
// feed for `symbol`. The server keeps the subscription open across
// phase transitions, so consumers check `state?.phase` to decide
// whether to render — the server doesn't unilaterally close when the
// auction ends.
//
// Returns null until the first message arrives (or when symbol is
// empty). Each subsequent message replaces the prior state.
export function useIndicativeAuctionState(symbol: string): IndicativeAuctionState | null {
  const [state, setState] = useState<IndicativeAuctionState | null>(null);

  useStream(
    (signal) =>
      orderBookClient.streamIndicativeAuctionState({ symbol }, { signal }),
    (msg) => setState(msg),
    [symbol],
  );

  return state;
}
