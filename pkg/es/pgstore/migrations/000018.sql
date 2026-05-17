-- Tracks active user single-order sagas (including bracket-entry
-- children) so the margincall reactor can list them from the SAME
-- consumer it lives in, no cross-consumer lag. Insert on
-- OrderSagaStarted, delete on OrderSagaCompleted / OrderSagaFailed.
CREATE TABLE IF NOT EXISTS projection_active_user_sagas (
    saga_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_active_user_sagas_account
    ON projection_active_user_sagas(account_id);
