import { useState } from "react";
import { portfolioClient } from "~/lib/client";
import type { GetPortfolioResponse } from "../../src/gen/portfolio/v1/service_pb";
import { useStream } from "./useStream";

export function usePortfolio(accountId: string) {
  const [portfolio, setPortfolio] = useState<GetPortfolioResponse | null>(null);

  useStream(
    (signal) => portfolioClient.streamPortfolio({ accountId }, { signal }),
    (msg) => setPortfolio(msg),
    [accountId],
  );

  return portfolio;
}
