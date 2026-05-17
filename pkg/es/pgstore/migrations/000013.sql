-- Official end-of-day close per (symbol, session_date), populated by the
-- daily_close projection from OfficialCloseSet events.
CREATE TABLE IF NOT EXISTS projection_daily_close (
    symbol       TEXT NOT NULL,
    session_date DATE NOT NULL,
    close_price  BIGINT NOT NULL,
    close_volume BIGINT NOT NULL,
    closed_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, session_date)
);

CREATE INDEX IF NOT EXISTS idx_projection_daily_close_symbol_date
    ON projection_daily_close (symbol, session_date DESC);
