CREATE TABLE IF NOT EXISTS projection_checkpoints (
    name     TEXT PRIMARY KEY,
    sequence BIGINT NOT NULL DEFAULT 0
);
