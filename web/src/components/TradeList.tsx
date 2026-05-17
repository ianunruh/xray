import { useCallback, useState } from "react";
import { orderBookClient } from "../client";
import type { Trade } from "../gen/orderbook/v1/service_pb";
import { useStream } from "../hooks/useStream";
import { TradeTable } from "./TradeTable";

const MAX_TRADES = 100;

export function TradeList({ symbol }: { symbol: string }) {
  const [trades, setTrades] = useState<Trade[]>([]);

  const onTrade = useCallback((trade: Trade) => {
    setTrades((prev) => [trade, ...prev].slice(0, MAX_TRADES));
  }, []);

  useStream(
    (signal) => orderBookClient.streamTrades({ symbol }, { signal }),
    onTrade,
    [symbol],
  );

  return <TradeTable trades={trades} />;
}
