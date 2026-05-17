package es

import (
	"context"

	"github.com/google/uuid"
)

type causationKey struct{}

// Causation carries the IDs needed to stamp causation on emitted events:
// CauseID is the event that triggered the current command (one hop back),
// CorrelationID is the root of the chain (propagates unchanged).
type Causation struct {
	CauseID       string
	CorrelationID string
}

// WithCausation returns a context carrying causation derived from evt. Reactor
// HandleEvents implementations should call this once per triggering event so
// that any nested Handle calls inherit the cause/correlation automatically.
func WithCausation(ctx context.Context, evt Event) context.Context {
	return context.WithValue(ctx, causationKey{}, Causation{
		CauseID:       evt.ID,
		CorrelationID: evt.CorrelationID,
	})
}

// WithCorrelation returns a context carrying an explicit correlation ID and no
// cause. Useful for origin sites (RPC handlers, the reconciler) that want to
// reuse an existing correlation rather than minting a fresh one.
func WithCorrelation(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, causationKey{}, Causation{
		CorrelationID: correlationID,
	})
}

// CausationFrom returns the Causation stored in ctx, if any.
func CausationFrom(ctx context.Context) (Causation, bool) {
	c, ok := ctx.Value(causationKey{}).(Causation)
	return c, ok
}

// NewCorrelation mints a fresh correlation ID, attaches it to ctx, and
// returns both. Use at origin sites (RPC handlers, the reconciler) so the
// correlation ID is available for log lines and downstream Handle calls
// inherit it via ctx instead of each one auto-minting independently.
func NewCorrelation(ctx context.Context) (context.Context, string) {
	id := uuid.NewString()
	return WithCorrelation(ctx, id), id
}
