-- Unified saga projection: one row per saga, regardless of kind.
-- Powers SagaService.Get / List / Cancel dispatch (kind lookup).
-- Co-exists with projection_pending_orders, which the portfolio UI
-- still reads to render single-order pending orders inline.

CREATE TABLE IF NOT EXISTS projection_sagas (
    saga_id     TEXT PRIMARY KEY,
    kind        INT NOT NULL,            -- saga.v1.SagaKind
    status      INT NOT NULL,            -- saga.v1.SagaStatus
    account_id  TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    ended_at    TIMESTAMPTZ,
    fail_reason TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS projection_sagas_account_idx ON projection_sagas (account_id);
CREATE INDEX IF NOT EXISTS projection_sagas_symbol_idx  ON projection_sagas (symbol);
CREATE INDEX IF NOT EXISTS projection_sagas_status_idx  ON projection_sagas (status);
