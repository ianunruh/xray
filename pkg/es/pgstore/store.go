package pgstore

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianunruh/xray/pkg/es"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const (
	queryLoad = `SELECT id, causation_id, correlation_id, aggregate_id, type, version, data, timestamp, position
		FROM events
		WHERE aggregate_id = $1
		ORDER BY version`

	queryLoadLatest = `SELECT id, causation_id, correlation_id, aggregate_id, type, version, data, timestamp, position
		FROM events
		WHERE aggregate_id = $1
		ORDER BY version DESC
		LIMIT $2`

	queryLoadFrom = `SELECT id, causation_id, correlation_id, aggregate_id, type, version, data, timestamp, position
		FROM events
		WHERE aggregate_id = $1 AND version >= $2
		ORDER BY version`

	queryLoadRange = `SELECT id, causation_id, correlation_id, aggregate_id, type, version, data, timestamp, position
		FROM events
		WHERE aggregate_id = $1 AND version >= $2 AND version <= $3
		ORDER BY version`

	queryStreamMetadata = `SELECT
			min(version), max(version), min(timestamp), max(timestamp)
		FROM events
		WHERE aggregate_id = $1`

	queryVersionAtTimestamp = `SELECT max(version)
		FROM events
		WHERE aggregate_id = $1 AND timestamp <= $2`

	queryLoadAll = `SELECT id, causation_id, correlation_id, aggregate_id, type, version, data, timestamp, position
		FROM events
		ORDER BY position`

	queryLoadAfter = `SELECT id, causation_id, correlation_id, aggregate_id, type, version, data, timestamp, position
		FROM events
		WHERE position > $1
		ORDER BY position
		LIMIT $2`

	queryLoadByCorrelation = `SELECT id, causation_id, correlation_id, aggregate_id, type, version, data, timestamp, position
		FROM events
		WHERE correlation_id = $1::uuid
		ORDER BY timestamp, position`

	queryAppend = `INSERT INTO events (id, causation_id, correlation_id, aggregate_id, type, version, data, timestamp)
		VALUES ($1, NULLIF($2, '')::uuid, NULLIF($3, '')::uuid, $4, $5, $6, $7, $8)`

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

	queryDeleteCheckpoint = `DELETE FROM projection_checkpoints WHERE name = $1`

	queryListAggregates = `SELECT aggregate_id, count(*), min(timestamp), max(timestamp)
		FROM events
		WHERE ($1 = '' OR aggregate_id ILIKE '%' || $1 || '%')
		GROUP BY aggregate_id
		ORDER BY max(timestamp) DESC
		LIMIT $2`
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

// LoadLatest returns up to limit most recent events for the aggregate,
// ordered by version DESC. Used by the diagnostics panel where the
// full stream may be very large and only the tail is useful.
func (s *Store) LoadLatest(ctx context.Context, aggregateID string, limit int) ([]es.RawEvent, error) {
	return s.queryEvents(ctx, queryLoadLatest, aggregateID, limit)
}

// LoadFrom returns events for the given aggregate starting from fromVersion (inclusive).
func (s *Store) LoadFrom(ctx context.Context, aggregateID string, fromVersion int) ([]es.RawEvent, error) {
	return s.queryEvents(ctx, queryLoadFrom, aggregateID, fromVersion)
}

// LoadRange returns events with version in [fromVersion, toVersion].
// toVersion <= 0 means no upper bound (equivalent to LoadFrom).
func (s *Store) LoadRange(ctx context.Context, aggregateID string, fromVersion, toVersion int) ([]es.RawEvent, error) {
	if toVersion <= 0 {
		return s.queryEvents(ctx, queryLoadFrom, aggregateID, fromVersion)
	}
	return s.queryEvents(ctx, queryLoadRange, aggregateID, fromVersion, toVersion)
}

// VersionAtTimestamp returns the largest version with timestamp <= ts, or 0
// if no such event exists for the aggregate.
func (s *Store) VersionAtTimestamp(ctx context.Context, aggregateID string, ts time.Time) (int, error) {
	var v *int
	err := s.pool.QueryRow(ctx, queryVersionAtTimestamp, aggregateID, ts).Scan(&v)
	if err != nil {
		return 0, fmt.Errorf("query version at timestamp: %w", err)
	}
	if v == nil {
		return 0, nil
	}
	return *v, nil
}

// StreamMetadata returns version and timestamp bounds for the stream.
// Returns zero values if the aggregate has no events.
func (s *Store) StreamMetadata(ctx context.Context, aggregateID string) (es.StreamMetadata, error) {
	var (
		firstV, lastV   *int
		firstTS, lastTS *time.Time
	)
	err := s.pool.QueryRow(ctx, queryStreamMetadata, aggregateID).Scan(&firstV, &lastV, &firstTS, &lastTS)
	if err != nil {
		return es.StreamMetadata{}, fmt.Errorf("query stream metadata: %w", err)
	}
	if firstV == nil {
		return es.StreamMetadata{}, nil
	}
	return es.StreamMetadata{
		FirstVersion:   *firstV,
		LastVersion:    *lastV,
		FirstTimestamp: *firstTS,
		LastTimestamp:  *lastTS,
	}, nil
}

// LoadAll returns all events across all aggregates, ordered by position.
func (s *Store) LoadAll(ctx context.Context) ([]es.RawEvent, error) {
	return s.queryEvents(ctx, queryLoadAll)
}

// LoadAfter returns up to limit events with position > afterPosition, ordered by position.
func (s *Store) LoadAfter(ctx context.Context, afterPosition int64, limit int) ([]es.RawEvent, error) {
	return s.queryEvents(ctx, queryLoadAfter, afterPosition, limit)
}

// LoadByCorrelationID returns every event tagged with the given correlation
// ID, ordered by timestamp (ties broken by global position). Used by the
// diagnostics service to render the full causal chain triggered by one root
// command.
func (s *Store) LoadByCorrelationID(ctx context.Context, correlationID string) ([]es.RawEvent, error) {
	return s.queryEvents(ctx, queryLoadByCorrelation, correlationID)
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
		var causation, correlation *string
		if err := rows.Scan(&evt.ID, &causation, &correlation, &evt.AggregateID, &evt.Type, &evt.Version, &evt.Data, &evt.Timestamp, &evt.Position); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if causation != nil {
			evt.CausationID = *causation
		}
		if correlation != nil {
			evt.CorrelationID = *correlation
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
		batch.Queue(queryAppend, evt.ID, evt.CausationID, evt.CorrelationID, aggregateID, evt.Type, version, evt.Data, evt.Timestamp)
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

// ListAggregates returns up to limit aggregate-stream summaries ordered
// by most-recent activity, optionally filtered by a substring match on
// aggregate_id (case-insensitive).
func (s *Store) ListAggregates(ctx context.Context, filter string, limit int) ([]AggregateSummary, error) {
	rows, err := s.pool.Query(ctx, queryListAggregates, filter, limit)
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

// DeleteCheckpoint removes the named cursor row, forcing the next
// LoadCheckpoint to return 0.
func (s *Store) DeleteCheckpoint(ctx context.Context, name string) error {
	if _, err := s.pool.Exec(ctx, queryDeleteCheckpoint, name); err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}

// Migrate runs the schema migrations. Call this on startup.
func (s *Store) Migrate(ctx context.Context) error {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		sql, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := s.pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}
