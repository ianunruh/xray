import { useEffect, useState } from "react";
import { sagaClient } from "../client";
import { SagaKind, SagaStatus } from "../gen/saga/v1/saga_pb";
import type { GetSagaResponse } from "../gen/saga/v1/saga_pb";

const POLL_INTERVAL_MS = 2000;

/**
 * Polls SagaService.List for active OCO sagas owned by the account.
 * Only surfaces top-level OCOs — child OCO sagas spawned by brackets
 * are hidden by the server-side List filter.
 */
export function useOcos(accountId: string): GetSagaResponse[] {
  const [ocos, setOcos] = useState<GetSagaResponse[]>([]);

  useEffect(() => {
    if (!accountId) {
      setOcos([]);
      return;
    }
    let cancelled = false;

    async function poll() {
      try {
        const resp = await sagaClient.list({
          accountId,
          kind: SagaKind.OCO,
          status: SagaStatus.ACTIVE,
        });
        if (!cancelled) {
          setOcos(resp.sagas);
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

  return ocos;
}
