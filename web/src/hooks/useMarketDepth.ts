import { useState } from "react";
import { orderBookClient } from "../client";
import type { PriceLevel } from "../gen/orderbook/v1/service_pb";
import { useStream } from "./useStream";

export function useMarketDepth(symbol: string) {
  const [bids, setBids] = useState<PriceLevel[]>([]);
  const [asks, setAsks] = useState<PriceLevel[]>([]);

  useStream(
    (signal) =>
      orderBookClient.streamMarketDepth({ symbol, depth: 15 }, { signal }),
    (msg) => {
      setBids(msg.bids);
      setAsks(msg.asks);
    },
    [symbol],
  );

  const allLevels = [...bids, ...asks];
  const maxQuantity =
    allLevels.length > 0
      ? allLevels.reduce((max, l) => (l.quantity > max ? l.quantity : max), 0n)
      : 1n;

  return { bids, asks, maxQuantity };
}
