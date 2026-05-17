-- Tag the trades table with cross_type so the UI/clients can distinguish
-- continuous trades from opening / closing cross prints. Defaults to 0
-- (CROSS_TYPE_NONE) for existing rows.
ALTER TABLE projection_trades
    ADD COLUMN IF NOT EXISTS cross_type INTEGER NOT NULL DEFAULT 0;
