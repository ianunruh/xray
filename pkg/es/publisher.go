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
