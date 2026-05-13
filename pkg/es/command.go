package es

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"google.golang.org/protobuf/proto"
)

const maxRetries = 3

// aggregateLocks provides per-aggregate mutual exclusion so that the
// load→execute→append cycle is serialized for each aggregate ID.
type aggregateLocks struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newAggregateLocks() *aggregateLocks {
	return &aggregateLocks{locks: make(map[string]*sync.Mutex)}
}

func (a *aggregateLocks) Lock(id string) {
	a.mu.Lock()
	l, ok := a.locks[id]
	if !ok {
		l = &sync.Mutex{}
		a.locks[id] = l
	}
	a.mu.Unlock()
	l.Lock()
}

func (a *aggregateLocks) Unlock(id string) {
	a.mu.Lock()
	l := a.locks[id]
	a.mu.Unlock()
	l.Unlock()
}

// Command represents a domain command targeting a specific aggregate.
type Command interface {
	AggregateID() string
}

// Handler loads an aggregate from the event store, executes a function that
// produces new events, and persists them with optimistic concurrency control.
type Handler[A Aggregate] struct {
	store     EventStore
	registry  *Registry
	factory   func(id string) A
	log       *slog.Logger
	snapshots SnapshotStore
	publisher EventPublisher
	locks     *aggregateLocks
}

// NewHandler creates a Handler for the given aggregate type.
func NewHandler[A Aggregate](store EventStore, registry *Registry, factory func(id string) A, log *slog.Logger) *Handler[A] {
	return &Handler[A]{
		store:    store,
		registry: registry,
		factory:  factory,
		log:      log,
		locks:    newAggregateLocks(),
	}
}

// WithSnapshots returns a copy of the handler with snapshot support enabled.
func (h *Handler[A]) WithSnapshots(s SnapshotStore) *Handler[A] {
	cp := *h
	cp.snapshots = s
	return &cp
}

// WithPublisher returns a copy of the handler with event publishing enabled.
func (h *Handler[A]) WithPublisher(p EventPublisher) *Handler[A] {
	cp := *h
	cp.publisher = p
	return &cp
}

// loadResult holds the outcome of loading an aggregate from events/snapshots.
type loadResult struct {
	expectedVersion int // total events in the store for this aggregate
	snapshotVersion int // version of the snapshot used (0 if none)
}

// Load creates and hydrates an aggregate from the event store (and snapshots
// if configured). It is intended for read-only queries where no new events
// are produced.
func (h *Handler[A]) Load(ctx context.Context, aggregateID string) (A, error) {
	agg := h.factory(aggregateID)
	if _, err := h.loadAggregate(ctx, agg, aggregateID); err != nil {
		var zero A
		return zero, err
	}
	return agg, nil
}

// Handle loads the aggregate, calls execute to produce new events, and appends
// them to the store. On optimistic concurrency conflicts the entire cycle is
// retried up to maxRetries times.
func (h *Handler[A]) Handle(ctx context.Context, cmd Command, execute func(A) ([]Event, error)) error {
	aggregateID := cmd.AggregateID()

	h.locks.Lock(aggregateID)
	defer h.locks.Unlock(aggregateID)

	for attempt := range maxRetries {
		if attempt > 0 {
			h.log.Info("retrying command", "aggregate_id", aggregateID, "attempt", attempt+1)
		}

		newEvents, err := h.tryHandle(ctx, aggregateID, execute)
		if errors.Is(err, ErrOptimisticConcurrency) {
			h.log.Warn("optimistic concurrency conflict, will retry", "aggregate_id", aggregateID, "attempt", attempt+1)
			continue
		}
		if err != nil {
			return err
		}

		if h.publisher != nil && len(newEvents) > 0 {
			if err := h.publisher.Publish(ctx, newEvents); err != nil {
				h.log.Error("failed to publish events", "aggregate_id", aggregateID, "error", err)
			}
		}

		return nil
	}

	return fmt.Errorf("append events: %w", ErrOptimisticConcurrency)
}

// tryHandle performs a single load→execute→append attempt.
func (h *Handler[A]) tryHandle(ctx context.Context, aggregateID string, execute func(A) ([]Event, error)) ([]Event, error) {
	h.log.Info("handling command", "aggregate_id", aggregateID)

	agg := h.factory(aggregateID)

	lr, err := h.loadAggregate(ctx, agg, aggregateID)
	if err != nil {
		return nil, err
	}

	// Save a snapshot after loading if the aggregate has crossed the threshold.
	// This captures consistent, committed state before execute may mutate the aggregate.
	h.maybeSaveSnapshot(ctx, agg, aggregateID, lr.expectedVersion, lr.snapshotVersion)

	newEvents, err := execute(agg)
	if err != nil {
		h.log.Warn("command execution failed", "aggregate_id", aggregateID, "error", err)
		return nil, fmt.Errorf("execute command: %w", err)
	}

	if len(newEvents) == 0 {
		return nil, nil
	}

	rawNew := make([]RawEvent, len(newEvents))
	for i, evt := range newEvents {
		raw, err := h.registry.Serialize(evt)
		if err != nil {
			return nil, fmt.Errorf("serialize event: %w", err)
		}
		rawNew[i] = raw
	}

	if err := h.store.Append(ctx, aggregateID, lr.expectedVersion, rawNew); err != nil {
		return nil, err
	}

	newVersion := lr.expectedVersion + len(newEvents)
	h.log.Info("events appended", "aggregate_id", aggregateID, "new_event_count", len(newEvents), "new_version", newVersion)

	return newEvents, nil
}

// loadAggregate restores the aggregate from a snapshot (if available) plus
// remaining events, or from the full event stream.
func (h *Handler[A]) loadAggregate(ctx context.Context, agg A, aggregateID string) (loadResult, error) {
	if h.snapshots != nil {
		snap, err := h.snapshots.LoadSnapshot(ctx, aggregateID)
		if err != nil {
			h.log.Error("failed to load snapshot", "aggregate_id", aggregateID, "error", err)
			return loadResult{}, fmt.Errorf("load snapshot: %w", err)
		}

		if snap != nil {
			if sa, ok := Aggregate(agg).(Snapshotable); ok {
				msg, err := h.deserializeSnapshot(sa, snap.Data)
				if err != nil {
					return loadResult{}, fmt.Errorf("deserialize snapshot: %w", err)
				}
				if err := sa.RestoreSnapshot(msg); err != nil {
					return loadResult{}, fmt.Errorf("restore snapshot: %w", err)
				}

				// Set the version to match the snapshot so Apply increments correctly.
				if ab, ok := Aggregate(agg).(interface{ SetVersion(int) }); ok {
					ab.SetVersion(snap.Version)
				}

				// Load only events after the snapshot.
				rawEvents, err := h.store.LoadFrom(ctx, aggregateID, snap.Version+1)
				if err != nil {
					h.log.Error("failed to load events from version", "aggregate_id", aggregateID, "from_version", snap.Version+1, "error", err)
					return loadResult{}, fmt.Errorf("load events from: %w", err)
				}

				for _, raw := range rawEvents {
					evt, err := h.registry.Deserialize(raw)
					if err != nil {
						return loadResult{}, fmt.Errorf("deserialize event: %w", err)
					}
					if err := agg.Apply(evt); err != nil {
						return loadResult{}, fmt.Errorf("apply event: %w", err)
					}
				}

				h.log.Info("aggregate restored from snapshot", "aggregate_id", aggregateID, "snapshot_version", snap.Version, "events_replayed", len(rawEvents))
				return loadResult{
					expectedVersion: snap.Version + len(rawEvents),
					snapshotVersion: snap.Version,
				}, nil
			}
		}
	}

	// No snapshot: load all events.
	rawEvents, err := h.store.Load(ctx, aggregateID)
	if err != nil {
		h.log.Error("failed to load events", "aggregate_id", aggregateID, "error", err)
		return loadResult{}, fmt.Errorf("load events: %w", err)
	}

	for _, raw := range rawEvents {
		evt, err := h.registry.Deserialize(raw)
		if err != nil {
			return loadResult{}, fmt.Errorf("deserialize event: %w", err)
		}
		if err := agg.Apply(evt); err != nil {
			return loadResult{}, fmt.Errorf("apply event: %w", err)
		}
	}

	return loadResult{expectedVersion: len(rawEvents)}, nil
}

func (h *Handler[A]) deserializeSnapshot(sa Snapshotable, data []byte) (proto.Message, error) {
	// Use Snapshot() to get a zero-value message of the correct type,
	// then unmarshal the data into a fresh instance of that type.
	template, err := sa.Snapshot()
	if err != nil {
		return nil, err
	}
	msg := proto.Clone(template)
	proto.Reset(msg)
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return msg, nil
}

// maybeSaveSnapshot saves a snapshot if the aggregate supports it and the
// current version has crossed a snapshot interval threshold since the last snapshot.
func (h *Handler[A]) maybeSaveSnapshot(ctx context.Context, agg A, aggregateID string, currentVersion, snapshotVersion int) {
	if h.snapshots == nil {
		return
	}

	sa, ok := Aggregate(agg).(Snapshotable)
	if !ok {
		return
	}

	interval := sa.SnapshotInterval()
	if interval <= 0 {
		return
	}

	// Save a snapshot if we've crossed a threshold boundary since the last snapshot.
	if currentVersion/interval <= snapshotVersion/interval {
		return
	}

	msg, err := sa.Snapshot()
	if err != nil {
		h.log.Error("failed to create snapshot", "aggregate_id", aggregateID, "error", err)
		return
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		h.log.Error("failed to marshal snapshot", "aggregate_id", aggregateID, "error", err)
		return
	}

	snap := Snapshot{
		AggregateID: aggregateID,
		Version:     currentVersion,
		Data:        data,
	}

	if err := h.snapshots.SaveSnapshot(ctx, snap); err != nil {
		h.log.Error("failed to save snapshot", "aggregate_id", aggregateID, "error", err)
		return
	}

	h.log.Info("snapshot saved", "aggregate_id", aggregateID, "version", currentVersion)
}
