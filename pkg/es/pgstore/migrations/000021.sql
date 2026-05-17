-- Persist the grace-expiry instant for each margin call so the audit
-- view can show when auto-liquidation will fire (or fired). Computed
-- at issue time from issued_at + reactor grace; written by the
-- PgMarginCallsProjection from MarginCallIssued.grace_expires_at.
ALTER TABLE projection_margin_calls
    ADD COLUMN IF NOT EXISTS grace_expires_at TIMESTAMPTZ;
