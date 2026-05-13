package es

import (
	"context"
	"fmt"
	"log/slog"
)

// Projection consumes deserialized events to build read models.
type Projection interface {
	HandleEvents(ctx context.Context, events []Event) error
}

// FanOutPublisher dispatches events synchronously to all registered projections.
// Individual projection errors are logged but do not propagate.
type FanOutPublisher struct {
	log         *slog.Logger
	projections []Projection
}

// NewFanOutPublisher creates a FanOutPublisher that dispatches to the given projections.
func NewFanOutPublisher(log *slog.Logger, projections ...Projection) *FanOutPublisher {
	return &FanOutPublisher{
		log:         log,
		projections: projections,
	}
}

// Publish dispatches events to all projections. Errors from individual projections
// are logged but do not fail the call.
func (p *FanOutPublisher) Publish(ctx context.Context, events []Event) error {
	for _, proj := range p.projections {
		if err := proj.HandleEvents(ctx, events); err != nil {
			p.log.Error("projection failed to handle events", "error", err)
		}
	}
	return nil
}

// HydrateProjections loads all events from the store, deserializes them, and
// replays them through the given projections. Call this at startup before
// accepting traffic.
func HydrateProjections(ctx context.Context, loader GlobalEventLoader, registry *Registry, log *slog.Logger, projections ...Projection) error {
	rawEvents, err := loader.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("load all events: %w", err)
	}

	events := make([]Event, 0, len(rawEvents))
	for _, raw := range rawEvents {
		evt, err := registry.Deserialize(raw)
		if err != nil {
			return fmt.Errorf("deserialize event: %w", err)
		}
		events = append(events, evt)
	}

	for _, proj := range projections {
		if err := proj.HandleEvents(ctx, events); err != nil {
			return fmt.Errorf("hydrate projection: %w", err)
		}
	}

	log.Info("projections hydrated", "event_count", len(events))
	return nil
}
