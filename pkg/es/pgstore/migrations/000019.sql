-- Audit log of margin calls and the liquidations they triggered.
-- Populated from MarginCallIssued / MarginCallCovered (snapshot at
-- event time) and OrderSagaStarted (when Initiator=MARGIN_CALL, the
-- saga is appended to the call's liquidation_saga_ids).
CREATE TABLE IF NOT EXISTS projection_margin_calls (
    call_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    trigger_trade_id TEXT NOT NULL,
    trigger_symbol TEXT NOT NULL,
    mark_price BIGINT NOT NULL,
    equity_at_issue BIGINT NOT NULL,
    maintenance_requirement_at_issue BIGINT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    covered_at TIMESTAMPTZ,
    equity_at_cover BIGINT,
    maintenance_requirement_at_cover BIGINT,
    liquidation_saga_ids TEXT[] NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_margin_calls_account_issued
    ON projection_margin_calls(account_id, issued_at DESC);
