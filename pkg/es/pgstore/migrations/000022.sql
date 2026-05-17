ALTER TABLE projection_pending_orders ADD COLUMN IF NOT EXISTS last_fill_price BIGINT NOT NULL DEFAULT 0;
