package tradermgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("trader not found")

// Record is the persisted form of a trader. config is stored as JSONB and
// shape varies by type; callers decode it into the appropriate proto.
type Record struct {
	ID        string
	Type      string
	Name      string
	Config    json.RawMessage
	Enabled   bool
	LastError string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) List(ctx context.Context) ([]Record, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, name, config, enabled, last_error, created_at, updated_at
		FROM traders
		ORDER BY created_at
	`)
	if err != nil {
		return nil, fmt.Errorf("query traders: %w", err)
	}
	defer rows.Close()

	var out []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.ID, &r.Type, &r.Name, &r.Config, &r.Enabled, &r.LastError, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan trader: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Get(ctx context.Context, id string) (Record, error) {
	var r Record
	err := s.pool.QueryRow(ctx, `
		SELECT id, type, name, config, enabled, last_error, created_at, updated_at
		FROM traders
		WHERE id = $1
	`, id).Scan(&r.ID, &r.Type, &r.Name, &r.Config, &r.Enabled, &r.LastError, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, fmt.Errorf("get trader: %w", err)
	}
	return r, nil
}

func (s *Store) Insert(ctx context.Context, r Record) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO traders (id, type, name, config, enabled, last_error, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, now(), now())
	`, r.ID, r.Type, r.Name, r.Config, r.Enabled, r.LastError)
	if err != nil {
		return fmt.Errorf("insert trader: %w", err)
	}
	return nil
}

func (s *Store) UpdateConfig(ctx context.Context, id, name string, config json.RawMessage) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE traders SET name = $2, config = $3, updated_at = now()
		WHERE id = $1
	`, id, name, config)
	if err != nil {
		return fmt.Errorf("update trader: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetEnabled persists the desired auto-start flag and clears any stored
// last_error so a freshly-started trader doesn't carry a stale failure.
func (s *Store) SetEnabled(ctx context.Context, id string, enabled bool) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE traders SET enabled = $2, last_error = '', updated_at = now()
		WHERE id = $1
	`, id, enabled)
	if err != nil {
		return fmt.Errorf("set trader enabled: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) SetLastError(ctx context.Context, id, msg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE traders SET last_error = $2, updated_at = now()
		WHERE id = $1
	`, id, msg)
	if err != nil {
		return fmt.Errorf("set last error: %w", err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM traders WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete trader: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
