CREATE TABLE IF NOT EXISTS traders (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    name        TEXT NOT NULL,
    config      JSONB NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT FALSE,
    last_error  TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
