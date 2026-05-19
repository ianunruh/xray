import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { CorporateActionService } from "../../src/gen/corpaction/v1/service_pb";
import { DiagnosticsService } from "../../src/gen/diagnostics/v1/service_pb";
import { OrderBookService } from "../../src/gen/orderbook/v1/service_pb";
import { PortfolioService } from "../../src/gen/portfolio/v1/service_pb";
import { SagaService } from "../../src/gen/saga/v1/saga_pb";
import { TraderService } from "../../src/gen/trader/v1/service_pb";

// Server-side transport used by route loaders/actions to proxy unary
// Connect calls from the RR server to the Go backend. The `.server.ts`
// suffix tells Vite/RR to strip this module from the client bundle, so
// the upstream URL never reaches the browser.
const baseUrl = process.env.XRAY_API_URL ?? "http://localhost:8080";

const transport = createConnectTransport({
  baseUrl,
  httpVersion: "1.1",
});

export const orderBookClient = createClient(OrderBookService, transport);
export const portfolioClient = createClient(PortfolioService, transport);
export const diagnosticsClient = createClient(DiagnosticsService, transport);
export const sagaClient = createClient(SagaService, transport);
export const traderClient = createClient(TraderService, transport);
export const corpactionClient = createClient(CorporateActionService, transport);
