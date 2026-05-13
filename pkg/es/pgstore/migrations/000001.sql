CREATE TABLE IF NOT EXISTS events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id  TEXT NOT NULL,
    type          TEXT NOT NULL,
    version       INT NOT NULL,
    data          BYTEA NOT NULL,
    timestamp     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, version)
);
