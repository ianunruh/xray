package es

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Projection consumes deserialized events to build read models.
type Projection interface {
	HandleEvents(ctx context.Context, events []Event) error
}

// FanOutPublisher dispatches events asynchronously to all registered projections.
// A buffered channel absorbs bursts; under extreme load, Publish blocks to apply
// back-pressure. Events are never dropped.
type FanOutPublisher struct {
	log         *slog.Logger
	projections []Projection
	ch          chan []Event
	done        chan struct{}
	closeOnce   sync.Once
}

// NewFanOutPublisher creates a FanOutPublisher that dispatches to the given projections.
// A background goroutine drains the channel and fans events out to each projection.
func NewFanOutPublisher(log *slog.Logger, projections ...Projection) *FanOutPublisher {
	p := &FanOutPublisher{
		log:         log,
		projections: projections,
		ch:          make(chan []Event, 64),
		done:        make(chan struct{}),
	}
	go p.loop()
	return p
}

// Publish enqueues events for async dispatch. It blocks only if the internal
// buffer is full, providing back-pressure under extreme load.
func (p *FanOutPublisher) Publish(_ context.Context, events []Event) error {
	p.ch <- events
	return nil
}

// Close stops the background goroutine and waits for all buffered events to be
// dispatched. It is safe to call multiple times.
func (p *FanOutPublisher) Close() {
	p.closeOnce.Do(func() {
		close(p.ch)
		<-p.done
	})
}

func (p *FanOutPublisher) loop() {
	defer close(p.done)
	for events := range p.ch {
		for _, proj := range p.projections {
			if err := proj.HandleEvents(context.Background(), events); err != nil {
				p.log.Error("projection failed to handle events", "error", err)
			}
		}
	}
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
