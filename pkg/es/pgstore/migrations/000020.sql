-- Mirror of projection_shorts_by_symbol but for longs. Used by the
-- margin-call reactor to identify accounts whose long-on-margin
-- positions might breach maintenance when the mark moves.
CREATE TABLE IF NOT EXISTS projection_longs_by_symbol (
    symbol TEXT NOT NULL,
    account_id TEXT NOT NULL,
    quantity BIGINT NOT NULL,
    PRIMARY KEY (symbol, account_id)
);

CREATE INDEX IF NOT EXISTS idx_longs_account
    ON projection_longs_by_symbol(account_id);
