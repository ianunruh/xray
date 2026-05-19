CREATE TABLE IF NOT EXISTS projection_corporate_actions (
    action_id          TEXT PRIMARY KEY,
    symbol             TEXT NOT NULL,
    type               INT  NOT NULL,
    status             INT  NOT NULL,
    split_numerator    INT,
    split_denominator  INT,
    dividend_per_share BIGINT,
    new_symbol         TEXT,
    effective_date     TIMESTAMPTZ,
    record_date        TIMESTAMPTZ,
    pay_date           TIMESTAMPTZ,
    declared_at        TIMESTAMPTZ NOT NULL,
    applied_at         TIMESTAMPTZ,
    failed_reason      TEXT,
    holders_count      INT,
    orders_count       INT,
    sagas_count        INT,
    dividend_snapshotted BOOLEAN NOT NULL DEFAULT FALSE
);

-- Reactor's work-list scans for status=Declared (1) actions due now.
-- Two partial indexes — one per axis the scan checks — so neither
-- index has to include rows of the wrong status or wrong type.
CREATE INDEX IF NOT EXISTS idx_corp_actions_due
    ON projection_corporate_actions(effective_date)
    WHERE status = 1 AND effective_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_corp_actions_div_due
    ON projection_corporate_actions(pay_date)
    WHERE status = 1 AND type = 2;
CREATE INDEX IF NOT EXISTS idx_corp_actions_symbol
    ON projection_corporate_actions(symbol);

-- Per-action record-date holder snapshots — written when the reactor
-- first observes record_date for a dividend, read on pay_date.
CREATE TABLE IF NOT EXISTS projection_dividend_record_holders (
    action_id      TEXT   NOT NULL,
    account_id     TEXT   NOT NULL,
    shares         BIGINT NOT NULL,
    snapshotted_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (action_id, account_id)
);
