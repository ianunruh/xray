CREATE TABLE IF NOT EXISTS projection_fees (
    id           BIGSERIAL PRIMARY KEY,
    account_id   TEXT NOT NULL,
    kind         INT  NOT NULL,
    amount       BIGINT NOT NULL,
    symbol       TEXT,
    charged_at   TIMESTAMPTZ NOT NULL,
    related_id   TEXT,
    rate_bps     BIGINT,
    notional     BIGINT,
    period_start TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fees_account_charged
    ON projection_fees(account_id, charged_at DESC);
