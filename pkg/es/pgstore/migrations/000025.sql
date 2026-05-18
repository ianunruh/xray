ALTER TABLE projection_pending_orders
    ADD COLUMN IF NOT EXISTS fees_paid BIGINT NOT NULL DEFAULT 0;
