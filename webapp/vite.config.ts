import { reactRouter } from "@react-router/dev/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [reactRouter()],
  server: {
    port: 5174,
    proxy: {
      "/orderbook.v1.OrderBookService": "http://localhost:8080",
      "/portfolio.v1.PortfolioService": "http://localhost:8080",
      "/saga.v1.SagaService": "http://localhost:8080",
      "/diagnostics.v1.DiagnosticsService": "http://localhost:8080",
      "/trader.v1.TraderService": "http://localhost:8080",
    },
  },
});
