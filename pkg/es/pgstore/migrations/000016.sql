-- Split projection_pnl_positions to track long and short positions
-- separately. position_side: 1 = LONG, 2 = SHORT (matches the proto
-- enum POSITION_SIDE_LONG / POSITION_SIDE_SHORT).
ALTER TABLE projection_pnl_positions
    ADD COLUMN IF NOT EXISTS position_side INT NOT NULL DEFAULT 1;

ALTER TABLE projection_pnl_positions
    DROP CONSTRAINT projection_pnl_positions_pkey;
ALTER TABLE projection_pnl_positions
    ADD PRIMARY KEY (account_id, symbol, position_side);

ALTER TABLE projection_pnl
    ADD COLUMN IF NOT EXISTS position_side INT NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_pnl_account_side
    ON projection_pnl(account_id, position_side);
