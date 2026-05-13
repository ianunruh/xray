package es

import (
	"context"
	"errors"
)

// ErrOptimisticConcurrency is returned when an append fails because the
// expected version does not match the current stream version.
var ErrOptimisticConcurrency = errors.New("optimistic concurrency conflict")

// EventStore is the interface for persisting and loading raw event streams.
type EventStore interface {
	// Load returns all raw events for the given aggregate, ordered by version.
	Load(ctx context.Context, aggregateID string) ([]RawEvent, error)

	// Append persists new events to the stream. It must reject the append
	// if the stream's current version does not match expectedVersion.
	Append(ctx context.Context, aggregateID string, expectedVersion int, events []RawEvent) error
}
