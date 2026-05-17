#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DATABASE_URL:-postgres://xray:xray@localhost:5432/xray?sslmode=disable}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"

echo "Truncating Postgres tables..."
psql "$DB_URL" -c "TRUNCATE
  events,
  snapshots,
  projection_active_user_sagas,
  projection_checkpoints,
  projection_daily_close,
  projection_holdings,
  projection_longs_by_symbol,
  projection_margin_calls,
  projection_orders,
  projection_pending_orders,
  projection_pnl,
  projection_pnl_positions,
  projection_portfolios,
  projection_sagas,
  projection_shorts_by_symbol,
  projection_trades"

echo "Deleting NATS stream..."
nats --server="$NATS_URL" stream delete EVENTS --force 2>/dev/null || true

echo "Done."
