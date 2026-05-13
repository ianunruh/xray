package pgstore

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianunruh/xray/pkg/es"
)

//go:embed migrations/000001.sql
var migrationSQL string

const (
	queryLoad = `SELECT id, aggregate_id, type, version, data, timestamp
		FROM events
		WHERE aggregate_id = $1
		ORDER BY version`

	queryInsert = `INSERT INTO events (aggregate_id, type, version, data, timestamp)
		VALUES ($1, $2, $3, $4, $5)`
)

// compile-time check
var _ es.EventStore = (*Store)(nil)

// Store is a PostgreSQL-backed EventStore using pgxpool.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a new PostgreSQL event store.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Load returns all events for the given aggregate ID, ordered by version.
func (s *Store) Load(ctx context.Context, aggregateID string) ([]es.RawEvent, error) {
	rows, err := s.pool.Query(ctx, queryLoad, aggregateID)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var events []es.RawEvent
	for rows.Next() {
		var evt es.RawEvent
		if err := rows.Scan(&evt.ID, &evt.AggregateID, &evt.Type, &evt.Version, &evt.Data, &evt.Timestamp); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, evt)
	}

	return events, rows.Err()
}

// Append inserts new events in a transaction. A unique constraint violation
// on (aggregate_id, version) is mapped to ErrOptimisticConcurrency.
func (s *Store) Append(ctx context.Context, aggregateID string, expectedVersion int, events []es.RawEvent) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for i, evt := range events {
		version := expectedVersion + i + 1
		_, err := tx.Exec(ctx, queryInsert,
			aggregateID, evt.Type, version, evt.Data, evt.Timestamp)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return es.ErrOptimisticConcurrency
			}
			return fmt.Errorf("insert event: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// Migrate runs the schema migration. Call this on startup.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, migrationSQL)
	return err
}
