package es

import (
	"context"
	"errors"
	"time"
)

// ErrOptimisticConcurrency is returned when an append fails because the
// expected version does not match the current stream version.
var ErrOptimisticConcurrency = errors.New("optimistic concurrency conflict")

// StreamMetadata describes the bounds of an aggregate's event stream. When
// the stream is empty all fields are zero values.
type StreamMetadata struct {
	FirstVersion   int
	LastVersion    int
	FirstTimestamp time.Time
	LastTimestamp  time.Time
}

// EventStore is the interface for persisting and loading raw event streams.
type EventStore interface {
	// Load returns all raw events for the given aggregate, ordered by version.
	Load(ctx context.Context, aggregateID string) ([]RawEvent, error)

	// LoadFrom returns raw events for the given aggregate starting from the
	// specified version (inclusive), ordered by version.
	LoadFrom(ctx context.Context, aggregateID string, fromVersion int) ([]RawEvent, error)

	// LoadRange returns raw events for the given aggregate with version in
	// [fromVersion, toVersion] (both inclusive), ordered by version. A
	// toVersion <= 0 means "no upper bound" and is equivalent to LoadFrom.
	LoadRange(ctx context.Context, aggregateID string, fromVersion, toVersion int) ([]RawEvent, error)

	// StreamMetadata returns the version and timestamp bounds of the
	// aggregate's stream. Returns zero values if the stream is empty.
	StreamMetadata(ctx context.Context, aggregateID string) (StreamMetadata, error)

	// VersionAtTimestamp returns the largest version whose timestamp is at
	// or before ts. Returns 0 if no events for the aggregate exist at or
	// before ts.
	VersionAtTimestamp(ctx context.Context, aggregateID string, ts time.Time) (int, error)

	// Append persists new events to the stream. It must reject the append
	// if the stream's current version does not match expectedVersion.
	Append(ctx context.Context, aggregateID string, expectedVersion int, events []RawEvent) error
}
