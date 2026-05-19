ALTER TABLE projection_portfolios
    ADD COLUMN IF NOT EXISTS settled_cash BIGINT NOT NULL DEFAULT 0;

-- Seed existing rows: before T+1 settlement everything was instant,
-- so settled_cash equals the current cash_balance. The lazy seed in
-- the aggregate's Apply() handles the same case at runtime for any
-- pre-T+1 snapshot — this just keeps the PG projection consistent.
UPDATE projection_portfolios SET settled_cash = cash_balance
    WHERE settled_cash = 0 AND cash_balance <> 0;
