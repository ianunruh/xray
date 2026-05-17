import { useCallback, useEffect, useState } from "react";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { orderBookClient } from "../client";
import { MarketPhase } from "../gen/orderbook/v1/events_pb";

export type ReplayBounds = {
  firstVersion: number;
  lastVersion: number;
  firstDate: Date;
  lastDate: Date;
  currentPhase: MarketPhase;
};

// useReplayBounds fetches the version/timestamp range of an aggregate's
// event stream. Returns null until the first response arrives. Call
// refresh() after the user expects new events.
export function useReplayBounds(symbol: string) {
  const [bounds, setBounds] = useState<ReplayBounds | null>(null);

  const refresh = useCallback(async () => {
    if (!symbol) return;
    const resp = await orderBookClient.getReplayBounds({ symbol });
    if (!resp.firstTimestamp || !resp.lastTimestamp || resp.lastVersion <= 0) {
      setBounds(null);
      return;
    }
    setBounds({
      firstVersion: resp.firstVersion,
      lastVersion: resp.lastVersion,
      firstDate: timestampDate(resp.firstTimestamp),
      lastDate: timestampDate(resp.lastTimestamp),
      currentPhase:
        resp.currentPhase === MarketPhase.UNSPECIFIED
          ? MarketPhase.CONTINUOUS
          : resp.currentPhase,
    });
  }, [symbol]);

  useEffect(() => {
    refresh().catch(() => {
      // ignore — caller can retry via refresh()
    });
  }, [refresh]);

  return { bounds, refresh };
}
