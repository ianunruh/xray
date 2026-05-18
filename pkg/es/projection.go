package es

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Projection consumes deserialized events to build read models.
type Projection interface {
	HandleEvents(ctx context.Context, events []Event) error
}

// Resettable is implemented by projections whose read-side storage can be
// wiped to its initial empty state. The ProjectionManager calls Reset before
// replaying a consumer's event stream from sequence 1.
type Resettable interface {
	Reset(ctx context.Context) error
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

const (
	defaultPollInterval = 100 * time.Millisecond
	defaultPollBatch    = 256
)

// ProjectionRunner polls the event store for new events and dispatches them to
// projections. It replaces both HydrateProjections (initial catch-up) and
// FanOutPublisher (ongoing updates) with a single pull-based mechanism.
type ProjectionRunner struct {
	poller       GlobalEventPoller
	registry     *Registry
	projections  []Projection
	log          *slog.Logger
	pollInterval time.Duration
	pollBatch    int
	position     int64
}

// NewProjectionRunner creates a runner that polls the given store for new events.
func NewProjectionRunner(poller GlobalEventPoller, registry *Registry, log *slog.Logger, projections ...Projection) *ProjectionRunner {
	return &ProjectionRunner{
		poller:       poller,
		registry:     registry,
		projections:  projections,
		log:          log,
		pollInterval: defaultPollInterval,
		pollBatch:    defaultPollBatch,
	}
}

// Start catches up from position 0 to the current head, then polls for new
// events in the background. The background goroutine stops when ctx is cancelled.
func (r *ProjectionRunner) Start(ctx context.Context) error {
	if err := r.catchUp(ctx); err != nil {
		return fmt.Errorf("projection catch-up: %w", err)
	}

	go r.run(ctx)
	return nil
}

// catchUp polls in a tight loop until no more events are returned.
func (r *ProjectionRunner) catchUp(ctx context.Context) error {
	total := 0
	for {
		n, err := r.poll(ctx)
		if err != nil {
			return err
		}
		total += n
		if n < r.pollBatch {
			break
		}
	}
	r.log.Info("projections caught up", "event_count", total, "position", r.position)
	return nil
}

// run polls on an interval until ctx is cancelled.
func (r *ProjectionRunner) run(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.poll(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				r.log.Error("projection poll failed", "error", err)
			}
		}
	}
}

// poll fetches and dispatches one batch of events. Returns the number of events processed.
func (r *ProjectionRunner) poll(ctx context.Context) (int, error) {
	rawEvents, err := r.poller.LoadAfter(ctx, r.position, r.pollBatch)
	if err != nil {
		return 0, fmt.Errorf("load events after position %d: %w", r.position, err)
	}

	if len(rawEvents) == 0 {
		return 0, nil
	}

	events := make([]Event, 0, len(rawEvents))
	for _, raw := range rawEvents {
		evt, err := r.registry.Deserialize(raw)
		if err != nil {
			return 0, fmt.Errorf("deserialize event: %w", err)
		}
		events = append(events, evt)
	}

	for _, proj := range r.projections {
		if err := proj.HandleEvents(ctx, events); err != nil {
			r.log.Error("projection failed to handle events", "error", err)
		}
	}

	r.position = rawEvents[len(rawEvents)-1].Position
	return len(events), nil
}
