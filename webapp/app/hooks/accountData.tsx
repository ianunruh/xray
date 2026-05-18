import { createContext, useContext, type ReactNode } from "react";
import type {
  GetMarginSnapshotResponse,
  GetPortfolioResponse,
} from "../../src/gen/portfolio/v1/service_pb";
import { useMarginSnapshot } from "./useMarginSnapshot";
import { usePortfolio } from "./usePortfolio";

// AccountData hoists the per-account streams and polls into one
// subscription each so multiple consumers don't fan out duplicate
// requests. Mounted once at the trading-view scope and consumed via
// useAccountData by all account-bound panels.
type AccountData = {
  accountId: string;
  portfolio: GetPortfolioResponse | null;
  margin: GetMarginSnapshotResponse | null;
};

const Ctx = createContext<AccountData | null>(null);

export function AccountDataProvider({
  accountId,
  children,
}: {
  accountId: string;
  children: ReactNode;
}) {
  const portfolio = usePortfolio(accountId);
  const margin = useMarginSnapshot(accountId);
  return (
    <Ctx.Provider value={{ accountId, portfolio, margin }}>
      {children}
    </Ctx.Provider>
  );
}

export function useAccountData(): AccountData {
  const ctx = useContext(Ctx);
  if (!ctx) {
    throw new Error("useAccountData must be used inside <AccountDataProvider>");
  }
  return ctx;
}
