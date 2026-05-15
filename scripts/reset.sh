#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DATABASE_URL:-postgres://xray:xray@localhost:5432/xray?sslmode=disable}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"

echo "Truncating Postgres tables..."
psql "$DB_URL" -c "TRUNCATE events, snapshots, projection_orders, projection_trades, projection_checkpoints, projection_portfolios, projection_holdings, projection_pending_orders"

echo "Deleting NATS stream..."
nats --server="$NATS_URL" stream delete EVENTS --force 2>/dev/null || true

echo "Done."
