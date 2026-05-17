import { useEffect, useState } from "react";
import { portfolioClient } from "../client";
import type { MarginCallRecord } from "../gen/portfolio/v1/service_pb";

// useMarginCalls polls the audit log for the account. New calls
// arrive on the order of seconds at most (they fire from trade
// events), so a 3s poll is plenty.
export function useMarginCalls(
  accountId: string,
  limit = 20,
  intervalMs = 3000,
): MarginCallRecord[] {
  const [calls, setCalls] = useState<MarginCallRecord[]>([]);

  useEffect(() => {
    if (!accountId) {
      setCalls([]);
      return;
    }
    let cancelled = false;
    const fetchOnce = async () => {
      try {
        const resp = await portfolioClient.listMarginCalls({
          accountId,
          limit,
        });
        if (!cancelled) setCalls(resp.calls);
      } catch {
        // swallow transient
      }
    };
    fetchOnce();
    const id = window.setInterval(fetchOnce, intervalMs);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [accountId, limit, intervalMs]);

  return calls;
}
