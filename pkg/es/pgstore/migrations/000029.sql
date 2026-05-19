CREATE TABLE IF NOT EXISTS projection_pending_settlements (
    account_id    TEXT        NOT NULL,
    trade_id      TEXT        NOT NULL,
    kind          INT         NOT NULL,
    order_saga_id TEXT        NOT NULL,
    symbol        TEXT        NOT NULL,
    cash_amount   BIGINT      NOT NULL,
    settles_at    TIMESTAMPTZ NOT NULL,
    emitted_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, trade_id, kind)
);

-- The reactor scans rows whose settles_at has passed; a partial
-- index keyed on settles_at makes that scan cheap regardless of how
-- many future-dated rows are queued behind it.
CREATE INDEX IF NOT EXISTS idx_pending_settlements_due
    ON projection_pending_settlements(settles_at);

-- For per-account credit/debit sums on the GetPortfolio path.
CREATE INDEX IF NOT EXISTS idx_pending_settlements_account
    ON projection_pending_settlements(account_id);
