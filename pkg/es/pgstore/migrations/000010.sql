ALTER TABLE projection_pending_orders ADD COLUMN IF NOT EXISTS reason TEXT NOT NULL DEFAULT '';
ALTER TABLE projection_pending_orders ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ;
