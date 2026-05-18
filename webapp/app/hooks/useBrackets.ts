import { useEffect, useState } from "react";
import { sagaClient } from "~/lib/client";
import { SagaKind, SagaStatus } from "../../src/gen/saga/v1/saga_pb";
import type { GetSagaResponse } from "../../src/gen/saga/v1/saga_pb";

const POLL_INTERVAL_MS = 2000;

/**
 * Polls SagaService.List for active brackets owned by the account.
 * The unified saga projection isn't streamed, so a short poll is the
 * simplest way to keep this list live.
 */
export function useBrackets(accountId: string): GetSagaResponse[] {
  const [brackets, setBrackets] = useState<GetSagaResponse[]>([]);

  useEffect(() => {
    if (!accountId) {
      setBrackets([]);
      return;
    }
    let cancelled = false;

    async function poll() {
      try {
        const resp = await sagaClient.list({
          accountId,
          kind: SagaKind.BRACKET,
          status: SagaStatus.ACTIVE,
        });
        if (!cancelled) {
          setBrackets(resp.sagas);
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

  return brackets;
}
