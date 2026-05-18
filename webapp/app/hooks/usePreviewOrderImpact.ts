import { useEffect } from "react";
import { useFetcher } from "react-router";
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

type PreviewResult = {
  ok: boolean;
  intent: string;
  preview?: PreviewOrderImpactResponse;
};

// usePreviewOrderImpact debounces the form inputs and submits to the
// trading route's preview-impact action. The action proxies to
// PortfolioService.PreviewOrderImpact server-side; this hook is the
// browser-side glue. Returns null when the inputs aren't complete
// enough to preview (zero qty, missing limit price, etc.).
export function usePreviewOrderImpact(
  params: PreviewParams | null,
  debounceMs = 200,
): PreviewOrderImpactResponse | null {
  const fetcher = useFetcher<PreviewResult>();

  // Stable key for the effect: stringify the params (small object).
  const key = params
    ? `${params.accountId}|${params.symbol}|${params.side}|${params.positionSide}|${params.orderType}|${params.price}|${params.quantity}`
    : null;

  useEffect(() => {
    if (!params) return;
    const t = window.setTimeout(() => {
      const fd = new FormData();
      fd.set("intent", "preview-impact");
      fd.set("accountId", params.accountId);
      fd.set("symbol", params.symbol);
      fd.set("side", String(params.side));
      fd.set("positionSide", String(params.positionSide));
      fd.set("orderType", String(params.orderType));
      fd.set("price", String(params.price));
      fd.set("quantity", String(params.quantity));
      fetcher.submit(fd, { method: "post", action: "/trading" });
    }, debounceMs);
    return () => window.clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, debounceMs]);

  // When params goes null the fetcher's last data is stale; suppress.
  if (!params) return null;
  return fetcher.data?.preview ?? null;
}
