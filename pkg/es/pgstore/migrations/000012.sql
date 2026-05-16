-- Reconciler queries trades by order ID to find unsettled fills.
CREATE INDEX IF NOT EXISTS idx_projection_trades_buy_order  ON projection_trades (buy_order_id);
CREATE INDEX IF NOT EXISTS idx_projection_trades_sell_order ON projection_trades (sell_order_id);
