import { createContext, useContext, type ReactNode } from "react";
import type {
  GetMarginSnapshotResponse,
  GetPortfolioResponse,
  MarginCallRecord,
} from "../../src/gen/portfolio/v1/service_pb";
import { usePortfolio } from "./usePortfolio";

// AccountData provides per-account read state to the trading panels.
// Streamed state (portfolio) is subscribed once here; poll-driven state
// (margin snapshot, margin calls) is sourced from the route loader and
// passed in as props so the loader's revalidation cycle drives refreshes
// instead of independent setInterval loops scattered across hooks.
type AccountData = {
  accountId: string;
  portfolio: GetPortfolioResponse | null;
  margin: GetMarginSnapshotResponse | null;
  marginCalls: MarginCallRecord[];
};

const Ctx = createContext<AccountData | null>(null);

export function AccountDataProvider({
  accountId,
  margin,
  marginCalls,
  children,
}: {
  accountId: string;
  margin: GetMarginSnapshotResponse | null;
  marginCalls: MarginCallRecord[];
  children: ReactNode;
}) {
  const portfolio = usePortfolio(accountId);
  return (
    <Ctx.Provider value={{ accountId, portfolio, margin, marginCalls }}>
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
