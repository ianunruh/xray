import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/orderbook.v1.OrderBookService": "http://localhost:8080",
      "/orderbook.v1.SagaService": "http://localhost:8080",
      "/portfolio.v1.PortfolioService": "http://localhost:8080",
      "/diagnostics.v1.DiagnosticsService": "http://localhost:8080",
    },
  },
  build: {
    outDir: "dist",
  },
});
