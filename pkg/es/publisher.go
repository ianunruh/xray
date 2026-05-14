package es

import "context"

// EventPublisher is the pluggable transport for delivering events to projections.
// In-process fan-out is the default; NATS JetStream can replace it by implementing
// this interface.
type EventPublisher interface {
	Publish(ctx context.Context, events []Event) error
}

// GlobalEventLoader is an opt-in interface for replaying all events during
// startup hydration. Event stores that support cross-aggregate loading implement this.
type GlobalEventLoader interface {
	LoadAll(ctx context.Context) ([]RawEvent, error)
}

// GlobalEventPoller is an opt-in interface for incrementally polling new events
// by global position. Used by projection runners to consume events independently
// of the write path.
type GlobalEventPoller interface {
	LoadAfter(ctx context.Context, afterPosition int64, limit int) ([]RawEvent, error)
}

// CheckpointStore tracks the last processed sequence for resumable projections.
type CheckpointStore interface {
	LoadCheckpoint(ctx context.Context, name string) (uint64, error)
	SaveCheckpoint(ctx context.Context, name string, sequence uint64) error
}
