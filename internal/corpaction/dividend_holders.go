package corpaction

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DividendHoldersStore persists the per-action record-date snapshot
// of every holder entitled to a dividend. Decoupled from the
// projection ledger so the snapshot can be queried independently
// (and so it survives even if the action itself is rebuilt from
// the event stream).
type DividendHoldersStore interface {
	// SaveSnapshot writes one batch of (account, shares) rows for the
	// action's record-date snapshot. Idempotent: re-running with the
	// same action_id is a no-op (PK collision = ignore).
	SaveSnapshot(ctx context.Context, actionID string, holders []HolderShares, snapshottedAt time.Time) (int32, error)
	// LoadSnapshot returns the per-account share count for the
	// action at record-date.
	LoadSnapshot(ctx context.Context, actionID string) ([]HolderShares, error)
}

// HolderShares is the (account, shares) pair carried by the
// dividend snapshot — same shape as portfolio.HoldingRow but kept
// here so corpaction doesn't lock the portfolio type into its
// public interface.
type HolderShares struct {
	AccountID string
	Shares    int64
}

// PgDividendHoldersStore is the PG-backed implementation of
// DividendHoldersStore, talking to projection_dividend_record_holders.
type PgDividendHoldersStore struct {
	pool *pgxpool.Pool
}

func NewPgDividendHoldersStore(pool *pgxpool.Pool) *PgDividendHoldersStore {
	return &PgDividendHoldersStore{pool: pool}
}

func (s *PgDividendHoldersStore) SaveSnapshot(ctx context.Context, actionID string, holders []HolderShares, snapshottedAt time.Time) (int32, error) {
	if len(holders) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, h := range holders {
		batch.Queue(
			`INSERT INTO projection_dividend_record_holders
			 (action_id, account_id, shares, snapshotted_at)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (action_id, account_id) DO NOTHING`,
			actionID, h.AccountID, h.Shares, snapshottedAt,
		)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range batch.Len() {
		if _, err := br.Exec(); err != nil {
			return 0, fmt.Errorf("insert dividend holder: %w", err)
		}
	}
	return int32(len(holders)), nil
}

func (s *PgDividendHoldersStore) LoadSnapshot(ctx context.Context, actionID string) ([]HolderShares, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT account_id, shares FROM projection_dividend_record_holders
		 WHERE action_id = $1 ORDER BY account_id`,
		actionID,
	)
	if err != nil {
		return nil, fmt.Errorf("query dividend holders for %s: %w", actionID, err)
	}
	defer rows.Close()
	var out []HolderShares
	for rows.Next() {
		var h HolderShares
		if err := rows.Scan(&h.AccountID, &h.Shares); err != nil {
			return nil, fmt.Errorf("scan dividend holder: %w", err)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
