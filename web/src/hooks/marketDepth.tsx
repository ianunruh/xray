import { createContext, useContext, type ReactNode } from "react";
import { useMarketDepth } from "./useMarketDepth";

// MarketDepth hoists the per-symbol depth stream so the Trade tab's
// MarketPanel and OrderForm share one subscription instead of opening
// duplicates. Mounted at the Trade-tab scope by App.tsx; consumed via
// useSharedMarketDepth.
type MarketDepth = ReturnType<typeof useMarketDepth>;

const Ctx = createContext<MarketDepth | null>(null);

export function MarketDepthProvider({
  symbol,
  children,
}: {
  symbol: string;
  children: ReactNode;
}) {
  const depth = useMarketDepth(symbol);
  return <Ctx.Provider value={depth}>{children}</Ctx.Provider>;
}

export function useSharedMarketDepth(): MarketDepth {
  const ctx = useContext(Ctx);
  if (!ctx) {
    throw new Error(
      "useSharedMarketDepth must be used inside <MarketDepthProvider>",
    );
  }
  return ctx;
}
