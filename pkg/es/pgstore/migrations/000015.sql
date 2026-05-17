-- Causation: every event carries a CausationID (the parent event that
-- triggered the command that produced it) and a CorrelationID (the root of
-- the chain, propagated unchanged). Both are nullable so the existing rows
-- — written before this migration — remain valid; newly-appended rows are
-- always populated by the framework (Handler.tryHandle stamps them).
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS causation_id   UUID NULL,
    ADD COLUMN IF NOT EXISTS correlation_id UUID NULL;

CREATE INDEX IF NOT EXISTS idx_events_correlation_id ON events (correlation_id);
CREATE INDEX IF NOT EXISTS idx_events_causation_id   ON events (causation_id);
