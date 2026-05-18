import { useEffect, useState } from "react";
import { portfolioClient } from "~/lib/client";
import type { GetMarginSnapshotResponse } from "../../src/gen/portfolio/v1/service_pb";

// useMarginSnapshot polls the margin snapshot RPC. Mark changes don't
// come through the portfolio stream (they're orderbook events), so a
// short poll keeps the panel responsive without a dedicated stream.
export function useMarginSnapshot(accountId: string, intervalMs = 2000) {
  const [snapshot, setSnapshot] = useState<GetMarginSnapshotResponse | null>(
    null,
  );

  useEffect(() => {
    let cancelled = false;
    async function fetchSnapshot() {
      try {
        const resp = await portfolioClient.getMarginSnapshot({ accountId });
        if (!cancelled) setSnapshot(resp);
      } catch {
        // Swallow; next tick will retry.
      }
    }
    fetchSnapshot();
    const id = window.setInterval(fetchSnapshot, intervalMs);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [accountId, intervalMs]);

  return snapshot;
}
