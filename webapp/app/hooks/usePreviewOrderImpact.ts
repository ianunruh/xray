import { useEffect, useState } from "react";
import { portfolioClient } from "~/lib/client";
import { Side, OrderType, PositionSide } from "../../src/gen/orderbook/v1/events_pb";
import type { PreviewOrderImpactResponse } from "../../src/gen/portfolio/v1/service_pb";

export type PreviewParams = {
  accountId: string;
  symbol: string;
  side: Side;
  positionSide: PositionSide;
  orderType: OrderType;
  price: bigint;
  quantity: bigint;
};

// usePreviewOrderImpact debounces the form inputs and calls the
// server's PreviewOrderImpact RPC. Returns null when the inputs aren't
// complete enough to preview (zero qty, missing limit price, etc.).
export function usePreviewOrderImpact(
  params: PreviewParams | null,
  debounceMs = 200,
): PreviewOrderImpactResponse | null {
  const [preview, setPreview] = useState<PreviewOrderImpactResponse | null>(
    null,
  );

  // Stable key for the effect: stringify the params (small object).
  const key = params
    ? `${params.accountId}|${params.symbol}|${params.side}|${params.positionSide}|${params.orderType}|${params.price}|${params.quantity}`
    : null;

  useEffect(() => {
    if (!params) {
      setPreview(null);
      return;
    }
    let cancelled = false;
    const t = window.setTimeout(async () => {
      try {
        const resp = await portfolioClient.previewOrderImpact({
          accountId: params.accountId,
          symbol: params.symbol,
          side: params.side,
          positionSide: params.positionSide,
          orderType: params.orderType,
          price: params.price,
          quantity: params.quantity,
        });
        if (!cancelled) setPreview(resp);
      } catch {
        if (!cancelled) setPreview(null);
      }
    }, debounceMs);
    return () => {
      cancelled = true;
      window.clearTimeout(t);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, debounceMs]);

  return preview;
}
