ALTER TABLE projection_pending_orders
    ADD COLUMN fees_paid BIGINT NOT NULL DEFAULT 0;
