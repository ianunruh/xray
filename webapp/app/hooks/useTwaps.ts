import { useEffect, useState } from "react";
import { sagaClient } from "~/lib/client";
import { SagaKind, SagaStatus } from "../../src/gen/saga/v1/saga_pb";
import type { GetSagaResponse } from "../../src/gen/saga/v1/saga_pb";

const POLL_INTERVAL_MS = 2000;

/**
 * Polls SagaService.List for active TWAPs owned by the account.
 * Same poll-based approach as useBrackets — the unified saga
 * projection isn't streamed.
 */
export function useTwaps(accountId: string): GetSagaResponse[] {
  const [twaps, setTwaps] = useState<GetSagaResponse[]>([]);

  useEffect(() => {
    if (!accountId) {
      setTwaps([]);
      return;
    }
    let cancelled = false;

    async function poll() {
      try {
        const resp = await sagaClient.list({
          accountId,
          kind: SagaKind.TWAP,
          status: SagaStatus.ACTIVE,
        });
        if (!cancelled) {
          setTwaps(resp.sagas);
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

  return twaps;
}
