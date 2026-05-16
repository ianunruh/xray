import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { DiagnosticsService } from "./gen/diagnostics/v1/service_pb";
import { OrderBookService } from "./gen/orderbook/v1/service_pb";
import { PortfolioService } from "./gen/portfolio/v1/service_pb";

const transport = createConnectTransport({
  baseUrl: "/",
});

export const orderBookClient = createClient(OrderBookService, transport);
export const portfolioClient = createClient(PortfolioService, transport);
export const diagnosticsClient = createClient(DiagnosticsService, transport);
