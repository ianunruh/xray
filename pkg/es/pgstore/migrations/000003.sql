ALTER TABLE events ADD COLUMN IF NOT EXISTS position BIGSERIAL;
CREATE INDEX IF NOT EXISTS idx_events_position ON events (position);
