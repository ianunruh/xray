import { useEffect, useState } from "react";
import { portfolioClient } from "../client";
import type { GetPnLResponse } from "../gen/portfolio/v1/service_pb";

const POLL_INTERVAL_MS = 3000;

/**
 * Polls PortfolioService.GetPnL for the per-symbol breakdown and the
 * recent realized-P&L history. The portfolio aggregate isn't streamed
 * — same poll cadence as useMarginCalls.
 */
export function usePnL(accountId: string): GetPnLResponse | null {
  const [pnl, setPnl] = useState<GetPnLResponse | null>(null);

  useEffect(() => {
    if (!accountId) {
      setPnl(null);
      return;
    }
    let cancelled = false;

    async function poll() {
      try {
        const resp = await portfolioClient.getPnL({ accountId });
        if (!cancelled) {
          setPnl(resp);
        }
      } catch {
        // Swallow transient errors; the next tick will retry.
      }
    }

    poll();
    const handle = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(handle);
    };
  }, [accountId]);

  return pnl;
}
