package orderbook

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// OfficialCloseReader is the read interface for the daily_close projection.
type OfficialCloseReader interface {
	// GetOfficialClose returns the close for the given (symbol, sessionDate).
	// When sessionDate is empty, the most recent close for symbol is
	// returned. Returns (nil, nil) when no close exists.
	GetOfficialClose(ctx context.Context, symbol, sessionDate string) (*orderbookv1.GetOfficialCloseResponse, error)
	ListOfficialCloses(ctx context.Context, symbol, from, to string) ([]*orderbookv1.GetOfficialCloseResponse, error)
	// LatestClosePrice returns the most recent close price for symbol, or
	// (0, false) if none exists. Used by P&L mark-to-market.
	LatestClosePrice(ctx context.Context, symbol string) (int64, bool, error)
}

// PgDailyCloseProjection persists OfficialCloseSet events to
// projection_daily_close. It's a tiny single-table projection so the
// P&L projection (and clients) can mark to a stable end-of-day price
// without re-scanning the trade history.
type PgDailyCloseProjection struct {
	pool *pgxpool.Pool
}

func NewPgDailyCloseProjection(pool *pgxpool.Pool) *PgDailyCloseProjection {
	return &PgDailyCloseProjection{pool: pool}
}

func (p *PgDailyCloseProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}

	for _, evt := range events {
		data, ok := evt.Data.(*orderbookv1.OfficialCloseSet)
		if !ok {
			continue
		}
		sessionDate, err := time.Parse("2006-01-02", data.SessionDate)
		if err != nil {
			return fmt.Errorf("parse session_date %q: %w", data.SessionDate, err)
		}
		batch.Queue(
			`INSERT INTO projection_daily_close (symbol, session_date, close_price, close_volume, closed_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (symbol, session_date) DO UPDATE SET
				close_price  = EXCLUDED.close_price,
				close_volume = EXCLUDED.close_volume,
				closed_at    = EXCLUDED.closed_at`,
			data.Symbol, sessionDate, data.ClosePrice, data.CloseVolume, data.At.AsTime(),
		)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := p.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range batch.Len() {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("daily close projection: %w", err)
		}
	}
	return nil
}

func (p *PgDailyCloseProjection) GetOfficialClose(ctx context.Context, symbol, sessionDate string) (*orderbookv1.GetOfficialCloseResponse, error) {
	var (
		row     orderbookv1.GetOfficialCloseResponse
		sessAt  time.Time
		closeAt time.Time
	)
	row.Symbol = symbol

	var err error
	if sessionDate == "" {
		err = p.pool.QueryRow(ctx,
			`SELECT session_date, close_price, close_volume, closed_at
			FROM projection_daily_close
			WHERE symbol = $1
			ORDER BY session_date DESC
			LIMIT 1`,
			symbol,
		).Scan(&sessAt, &row.ClosePrice, &row.CloseVolume, &closeAt)
	} else {
		parsed, perr := time.Parse("2006-01-02", sessionDate)
		if perr != nil {
			return nil, fmt.Errorf("invalid session_date: %w", perr)
		}
		err = p.pool.QueryRow(ctx,
			`SELECT session_date, close_price, close_volume, closed_at
			FROM projection_daily_close
			WHERE symbol = $1 AND session_date = $2`,
			symbol, parsed,
		).Scan(&sessAt, &row.ClosePrice, &row.CloseVolume, &closeAt)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query daily close: %w", err)
	}
	row.SessionDate = sessAt.UTC().Format("2006-01-02")
	row.ClosedAt = timestamppb.New(closeAt)
	return &row, nil
}

func (p *PgDailyCloseProjection) ListOfficialCloses(ctx context.Context, symbol, from, to string) ([]*orderbookv1.GetOfficialCloseResponse, error) {
	args := []any{symbol}
	q := `SELECT session_date, close_price, close_volume, closed_at
		FROM projection_daily_close
		WHERE symbol = $1`
	if from != "" {
		t, err := time.Parse("2006-01-02", from)
		if err != nil {
			return nil, fmt.Errorf("invalid from: %w", err)
		}
		args = append(args, t)
		q += fmt.Sprintf(" AND session_date >= $%d", len(args))
	}
	if to != "" {
		t, err := time.Parse("2006-01-02", to)
		if err != nil {
			return nil, fmt.Errorf("invalid to: %w", err)
		}
		args = append(args, t)
		q += fmt.Sprintf(" AND session_date <= $%d", len(args))
	}
	q += ` ORDER BY session_date`

	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list daily closes: %w", err)
	}
	defer rows.Close()

	var out []*orderbookv1.GetOfficialCloseResponse
	for rows.Next() {
		var (
			row     orderbookv1.GetOfficialCloseResponse
			sessAt  time.Time
			closeAt time.Time
		)
		row.Symbol = symbol
		if err := rows.Scan(&sessAt, &row.ClosePrice, &row.CloseVolume, &closeAt); err != nil {
			return nil, fmt.Errorf("scan daily close: %w", err)
		}
		row.SessionDate = sessAt.UTC().Format("2006-01-02")
		row.ClosedAt = timestamppb.New(closeAt)
		out = append(out, &row)
	}
	return out, rows.Err()
}

func (p *PgDailyCloseProjection) LatestClosePrice(ctx context.Context, symbol string) (int64, bool, error) {
	var price int64
	err := p.pool.QueryRow(ctx,
		`SELECT close_price
		FROM projection_daily_close
		WHERE symbol = $1
		ORDER BY session_date DESC
		LIMIT 1`,
		symbol,
	).Scan(&price)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("query latest close: %w", err)
	}
	return price, true, nil
}
