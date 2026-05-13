package pgstore

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

type queryTracerContextKey struct{}

type queryTracerData struct {
	startTime time.Time
	sql       string
	args      []any
}

// QueryTracer implements pgx.QueryTracer, logging every query with its
// SQL text, arguments, duration, and any error via slog.
type QueryTracer struct {
	log *slog.Logger
}

// NewQueryTracer returns a pgx.QueryTracer that logs queries using the given logger.
func NewQueryTracer(log *slog.Logger) pgx.QueryTracer {
	return &QueryTracer{log: log}
}

func (t *QueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, queryTracerContextKey{}, &queryTracerData{
		startTime: time.Now(),
		sql:       data.SQL,
		args:      data.Args,
	})
}

func (t *QueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	qd, ok := ctx.Value(queryTracerContextKey{}).(*queryTracerData)
	if !ok {
		return
	}

	elapsed := time.Since(qd.startTime)

	attrs := []slog.Attr{
		slog.String("sql", qd.sql),
		slog.Any("args", qd.args),
		slog.Duration("duration", elapsed),
	}

	if data.Err != nil {
		attrs = append(attrs, slog.String("error", data.Err.Error()))
	}

	t.log.LogAttrs(ctx, slog.LevelInfo, "query", attrs...)
}
