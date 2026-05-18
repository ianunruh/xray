ALTER TABLE projection_pending_orders
    ADD COLUMN IF NOT EXISTS cash_settled BIGINT NOT NULL DEFAULT 0;
