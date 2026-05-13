package es

import (
	"context"
	"fmt"
	"log/slog"
)

// Command represents a domain command targeting a specific aggregate.
type Command interface {
	AggregateID() string
}

// Handler loads an aggregate from the event store, executes a function that
// produces new events, and persists them with optimistic concurrency control.
type Handler[A Aggregate] struct {
	store    EventStore
	registry *Registry
	factory  func(id string) A
	log      *slog.Logger
}

// NewHandler creates a Handler for the given aggregate type.
func NewHandler[A Aggregate](store EventStore, registry *Registry, factory func(id string) A, log *slog.Logger) *Handler[A] {
	return &Handler[A]{
		store:    store,
		registry: registry,
		factory:  factory,
		log:      log,
	}
}

// Handle loads the aggregate, calls execute to produce new events, and appends
// them to the store. The execute function receives the fully-hydrated aggregate
// and must return the new events to persist.
func (h *Handler[A]) Handle(ctx context.Context, cmd Command, execute func(A) ([]Event, error)) error {
	aggregateID := cmd.AggregateID()

	h.log.Info("handling command", "aggregate_id", aggregateID)

	rawEvents, err := h.store.Load(ctx, aggregateID)
	if err != nil {
		h.log.Error("failed to load events", "aggregate_id", aggregateID, "error", err)
		return fmt.Errorf("load events: %w", err)
	}

	agg := h.factory(aggregateID)

	for _, raw := range rawEvents {
		evt, err := h.registry.Deserialize(raw)
		if err != nil {
			return fmt.Errorf("deserialize event: %w", err)
		}
		if err := agg.Apply(evt); err != nil {
			return fmt.Errorf("apply event: %w", err)
		}
	}

	newEvents, err := execute(agg)
	if err != nil {
		h.log.Error("command execution failed", "aggregate_id", aggregateID, "error", err)
		return fmt.Errorf("execute command: %w", err)
	}

	if len(newEvents) == 0 {
		return nil
	}

	rawNew := make([]RawEvent, len(newEvents))
	for i, evt := range newEvents {
		raw, err := h.registry.Serialize(evt)
		if err != nil {
			return fmt.Errorf("serialize event: %w", err)
		}
		rawNew[i] = raw
	}

	expectedVersion := len(rawEvents)
	if err := h.store.Append(ctx, aggregateID, expectedVersion, rawNew); err != nil {
		h.log.Error("failed to append events", "aggregate_id", aggregateID, "expected_version", expectedVersion, "error", err)
		return fmt.Errorf("append events: %w", err)
	}

	h.log.Info("events appended", "aggregate_id", aggregateID, "new_event_count", len(newEvents), "new_version", expectedVersion+len(newEvents))

	return nil
}
