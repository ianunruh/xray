CREATE TABLE IF NOT EXISTS projection_shorts_by_symbol (
    symbol TEXT NOT NULL,
    account_id TEXT NOT NULL,
    quantity BIGINT NOT NULL,
    PRIMARY KEY (symbol, account_id)
);

CREATE INDEX IF NOT EXISTS idx_shorts_account
    ON projection_shorts_by_symbol(account_id);
