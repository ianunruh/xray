import { type RouteConfig, index, route } from "@react-router/dev/routes";

export default [
  index("routes/_index.tsx"),
  route("trading", "routes/trading.tsx"),
  route("traders", "routes/traders.tsx"),
  route("markets", "routes/markets.tsx"),
  route("events", "routes/events.tsx"),
  route("chain", "routes/chain.tsx"),
  route("corporate-actions", "routes/corporate-actions.tsx"),
  route("projections", "routes/projections.tsx"),
] satisfies RouteConfig;
