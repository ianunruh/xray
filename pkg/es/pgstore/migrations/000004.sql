CREATE TABLE IF NOT EXISTS projection_orders (
    symbol             TEXT NOT NULL,
    order_id           TEXT NOT NULL,
    side               INT NOT NULL,
    price              BIGINT NOT NULL,
    quantity           BIGINT NOT NULL,
    remaining_quantity BIGINT NOT NULL,
    status             INT NOT NULL,
    placed_at          TIMESTAMPTZ NOT NULL,
    order_type         INT NOT NULL,
    time_in_force      INT NOT NULL,
    PRIMARY KEY (symbol, order_id)
);

CREATE TABLE IF NOT EXISTS projection_trades (
    trade_id      TEXT PRIMARY KEY,
    symbol        TEXT NOT NULL,
    buy_order_id  TEXT NOT NULL,
    sell_order_id TEXT NOT NULL,
    price         BIGINT NOT NULL,
    quantity      BIGINT NOT NULL,
    executed_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_projection_orders_symbol ON projection_orders (symbol);
CREATE INDEX IF NOT EXISTS idx_projection_trades_symbol ON projection_trades (symbol);
