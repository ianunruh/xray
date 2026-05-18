import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { DiagnosticsService } from "../../src/gen/diagnostics/v1/service_pb";
import { OrderBookService } from "../../src/gen/orderbook/v1/service_pb";
import { PortfolioService } from "../../src/gen/portfolio/v1/service_pb";
import { SagaService } from "../../src/gen/saga/v1/saga_pb";
import { TraderService } from "../../src/gen/trader/v1/service_pb";

// Browser-side transport. Used by streaming hooks/components that hold
// long-lived Connect server-streams open via useEffect — loaders/actions
// have no primitive for ongoing subscriptions.
//
// In dev: Vite proxies /*.v1.* to the Go backend on :8080.
// In prod (once webapp/dist is embedded into Go): same-origin.
const transport = createConnectTransport({ baseUrl: "/" });

export const orderBookClient = createClient(OrderBookService, transport);
export const portfolioClient = createClient(PortfolioService, transport);
export const diagnosticsClient = createClient(DiagnosticsService, transport);
export const sagaClient = createClient(SagaService, transport);
export const traderClient = createClient(TraderService, transport);
