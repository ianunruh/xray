CREATE TABLE IF NOT EXISTS projection_portfolios (
    account_id   TEXT PRIMARY KEY,
    cash_balance BIGINT NOT NULL DEFAULT 0,
    cash_held    BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS projection_holdings (
    account_id TEXT NOT NULL,
    symbol     TEXT NOT NULL,
    quantity   BIGINT NOT NULL DEFAULT 0,
    total_cost BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, symbol)
);

CREATE TABLE IF NOT EXISTS projection_pending_orders (
    saga_id       TEXT PRIMARY KEY,
    account_id    TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    side          INT NOT NULL,
    price         BIGINT NOT NULL,
    quantity      BIGINT NOT NULL,
    order_type    INT NOT NULL,
    time_in_force INT NOT NULL,
    filled_qty    BIGINT NOT NULL DEFAULT 0,
    status        INT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL
);
