import { useEffect, useState } from "react";
import { ConnectError, Code } from "@connectrpc/connect";
import { orderBookClient } from "~/lib/client";
import type { GetOfficialCloseResponse } from "../../src/gen/orderbook/v1/service_pb";

// useOfficialClose returns the most recent official close for the
// symbol, or null if none exists yet. Refreshed every 30s — closes are
// emitted once per session so polling can be slow.
export function useOfficialClose(
  symbol: string,
): GetOfficialCloseResponse | null {
  const [close, setClose] = useState<GetOfficialCloseResponse | null>(null);

  useEffect(() => {
    if (!symbol) {
      setClose(null);
      return;
    }
    let cancelled = false;

    const tick = async () => {
      try {
        const resp = await orderBookClient.getOfficialClose({
          symbol,
          sessionDate: "",
        });
        if (!cancelled) setClose(resp);
      } catch (e) {
        if (
          e instanceof ConnectError &&
          (e.code === Code.NotFound || e.code === Code.Unimplemented)
        ) {
          if (!cancelled) setClose(null);
          return;
        }
        // ignore transient errors
      }
    };

    tick();
    const id = setInterval(tick, 30000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [symbol]);

  return close;
}
