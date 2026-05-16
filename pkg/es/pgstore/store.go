package pgstore

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianunruh/xray/pkg/es"
)

//go:embed migrations/000001.sql
var migration001SQL string

//go:embed migrations/000002.sql
var migration002SQL string

//go:embed migrations/000003.sql
var migration003SQL string

//go:embed migrations/000004.sql
var migration004SQL string

//go:embed migrations/000005.sql
var migration005SQL string

//go:embed migrations/000006.sql
var migration006SQL string

//go:embed migrations/000007.sql
var migration007SQL string

//go:embed migrations/000008.sql
var migration008SQL string

//go:embed migrations/000009.sql
var migration009SQL string

//go:embed migrations/000010.sql
var migration010SQL string

const (
	queryLoad = `SELECT id, aggregate_id, type, version, data, timestamp, position
		FROM events
		WHERE aggregate_id = $1
		ORDER BY version`

	queryLoadFrom = `SELECT id, aggregate_id, type, version, data, timestamp, position
		FROM events
		WHERE aggregate_id = $1 AND version >= $2
		ORDER BY version`

	queryLoadAll = `SELECT id, aggregate_id, type, version, data, timestamp, position
		FROM events
		ORDER BY position`

	queryLoadAfter = `SELECT id, aggregate_id, type, version, data, timestamp, position
		FROM events
		WHERE position > $1
		ORDER BY position
		LIMIT $2`

	queryAppend = `INSERT INTO events (aggregate_id, type, version, data, timestamp)
		VALUES ($1, $2, $3, $4, $5)`

	queryLoadSnapshot = `SELECT aggregate_id, version, data
		FROM snapshots
		WHERE aggregate_id = $1`

	querySaveSnapshot = `INSERT INTO snapshots (aggregate_id, version, data)
		VALUES ($1, $2, $3)
		ON CONFLICT (aggregate_id) DO UPDATE SET version = $2, data = $3, updated_at = now()`

	queryLoadCheckpoint = `SELECT sequence FROM projection_checkpoints WHERE name = $1`

	querySaveCheckpoint = `INSERT INTO projection_checkpoints (name, sequence)
		VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET sequence = $2`

	queryListAggregates = `SELECT aggregate_id, count(*), min(timestamp), max(timestamp)
		FROM events
		WHERE ($1 = '' OR aggregate_id ILIKE '%' || $1 || '%')
		GROUP BY aggregate_id
		ORDER BY max(timestamp) DESC`
)

// AggregateSummary describes one aggregate's event stream stats.
type AggregateSummary struct {
	AggregateID  string
	EventCount   int
	FirstEventAt time.Time
	LastEventAt  time.Time
}

// compile-time checks
var (
	_ es.EventStore        = (*Store)(nil)
	_ es.SnapshotStore     = (*Store)(nil)
	_ es.GlobalEventLoader = (*Store)(nil)
	_ es.GlobalEventPoller = (*Store)(nil)
	_ es.CheckpointStore   = (*Store)(nil)
)

// Store is a PostgreSQL-backed EventStore and SnapshotStore using pgxpool.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a new PostgreSQL event store.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Load returns all events for the given aggregate ID, ordered by version.
func (s *Store) Load(ctx context.Context, aggregateID string) ([]es.RawEvent, error) {
	return s.queryEvents(ctx, queryLoad, aggregateID)
}

// LoadFrom returns events for the given aggregate starting from fromVersion (inclusive).
func (s *Store) LoadFrom(ctx context.Context, aggregateID string, fromVersion int) ([]es.RawEvent, error) {
	return s.queryEvents(ctx, queryLoadFrom, aggregateID, fromVersion)
}

// LoadAll returns all events across all aggregates, ordered by position.
func (s *Store) LoadAll(ctx context.Context) ([]es.RawEvent, error) {
	return s.queryEvents(ctx, queryLoadAll)
}

// LoadAfter returns up to limit events with position > afterPosition, ordered by position.
func (s *Store) LoadAfter(ctx context.Context, afterPosition int64, limit int) ([]es.RawEvent, error) {
	return s.queryEvents(ctx, queryLoadAfter, afterPosition, limit)
}

func (s *Store) queryEvents(ctx context.Context, query string, args ...any) ([]es.RawEvent, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var events []es.RawEvent
	for rows.Next() {
		var evt es.RawEvent
		if err := rows.Scan(&evt.ID, &evt.AggregateID, &evt.Type, &evt.Version, &evt.Data, &evt.Timestamp, &evt.Position); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, evt)
	}

	return events, rows.Err()
}

// Append inserts new events using a batch INSERT. A unique constraint
// violation on (aggregate_id, version) is mapped to ErrOptimisticConcurrency.
func (s *Store) Append(ctx context.Context, aggregateID string, expectedVersion int, events []es.RawEvent) error {
	batch := &pgx.Batch{}
	for i, evt := range events {
		version := expectedVersion + i + 1
		batch.Queue(queryAppend, aggregateID, evt.Type, version, evt.Data, evt.Timestamp)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range events {
		if _, err := br.Exec(); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return es.ErrOptimisticConcurrency
			}
			return fmt.Errorf("insert events: %w", err)
		}
	}

	return nil
}

// LoadSnapshot returns the most recent snapshot for the aggregate, or nil if none exists.
func (s *Store) LoadSnapshot(ctx context.Context, aggregateID string) (*es.Snapshot, error) {
	var snap es.Snapshot
	err := s.pool.QueryRow(ctx, queryLoadSnapshot, aggregateID).Scan(
		&snap.AggregateID, &snap.Version, &snap.Data,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query snapshot: %w", err)
	}
	return &snap, nil
}

// SaveSnapshot persists a snapshot, replacing any existing one for the aggregate.
func (s *Store) SaveSnapshot(ctx context.Context, snap es.Snapshot) error {
	_, err := s.pool.Exec(ctx, querySaveSnapshot, snap.AggregateID, snap.Version, snap.Data)
	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	return nil
}

// TruncateProjections clears all projection tables so they can be rebuilt
// from the event stream. Call before replaying events into projections.
func (s *Store) TruncateProjections(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE projection_orders, projection_trades`)
	return err
}

// ListAggregates returns a summary of every aggregate stream, optionally
// filtered by a substring match on aggregate_id (case-insensitive).
func (s *Store) ListAggregates(ctx context.Context, filter string) ([]AggregateSummary, error) {
	rows, err := s.pool.Query(ctx, queryListAggregates, filter)
	if err != nil {
		return nil, fmt.Errorf("query aggregates: %w", err)
	}
	defer rows.Close()

	var summaries []AggregateSummary
	for rows.Next() {
		var sum AggregateSummary
		if err := rows.Scan(&sum.AggregateID, &sum.EventCount, &sum.FirstEventAt, &sum.LastEventAt); err != nil {
			return nil, fmt.Errorf("scan aggregate: %w", err)
		}
		summaries = append(summaries, sum)
	}
	return summaries, rows.Err()
}

// LoadCheckpoint returns the last processed sequence for the named checkpoint.
// Returns 0 if no checkpoint exists.
func (s *Store) LoadCheckpoint(ctx context.Context, name string) (uint64, error) {
	var seq uint64
	err := s.pool.QueryRow(ctx, queryLoadCheckpoint, name).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}
	return seq, nil
}

// SaveCheckpoint persists the last processed sequence for the named checkpoint.
func (s *Store) SaveCheckpoint(ctx context.Context, name string, sequence uint64) error {
	_, err := s.pool.Exec(ctx, querySaveCheckpoint, name, sequence)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// Migrate runs the schema migrations. Call this on startup.
func (s *Store) Migrate(ctx context.Context) error {
	for _, sql := range []string{migration001SQL, migration002SQL, migration003SQL, migration004SQL, migration005SQL, migration006SQL, migration007SQL, migration008SQL, migration009SQL, migration010SQL} {
		if _, err := s.pool.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}
