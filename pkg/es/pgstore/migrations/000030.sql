ALTER TABLE projection_pending_settlements
    ADD COLUMN IF NOT EXISTS quantity BIGINT NOT NULL DEFAULT 0;

-- For the per-symbol "shares pending settlement" badge on
-- GetPortfolio.holdings.
CREATE INDEX IF NOT EXISTS idx_pending_settlements_account_symbol
    ON projection_pending_settlements(account_id, symbol)
    WHERE quantity > 0;
