CREATE TABLE IF NOT EXISTS projection_pnl (
    id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side INT NOT NULL,
    quantity BIGINT NOT NULL,
    price BIGINT NOT NULL,
    realized_pnl BIGINT NOT NULL DEFAULT 0,
    settled_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pnl_account ON projection_pnl(account_id);
CREATE INDEX IF NOT EXISTS idx_pnl_account_symbol ON projection_pnl(account_id, symbol);

CREATE TABLE IF NOT EXISTS projection_pnl_positions (
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    quantity BIGINT NOT NULL DEFAULT 0,
    total_cost BIGINT NOT NULL DEFAULT 0,
    realized_pnl BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, symbol)
);
