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

// aggregateCache stores recently-used aggregates so that sequential commands
// targeting the same aggregate can skip the DB round-trip. The per-aggregate
// lock in Handler guarantees single-writer access, so the cached state is
// always consistent with the store after a successful append.
type aggregateCache[A Aggregate] struct {
	mu    sync.Mutex
	items map[string]cachedEntry[A]
}

type cachedEntry[A Aggregate] struct {
	agg             A
	version         int
	snapshotVersion int
}

func newAggregateCache[A Aggregate]() *aggregateCache[A] {
	return &aggregateCache[A]{items: make(map[string]cachedEntry[A])}
}

// Take returns and removes the cached entry for the given aggregate ID.
func (c *aggregateCache[A]) Take(id string) (cachedEntry[A], bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.items[id]
	if ok {
		delete(c.items, id)
	}
	return entry, ok
}

// Put stores an aggregate in the cache.
func (c *aggregateCache[A]) Put(id string, agg A, version, snapshotVersion int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[id] = cachedEntry[A]{agg: agg, version: version, snapshotVersion: snapshotVersion}
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
	cache     *aggregateCache[A]
}

// NewHandler creates a Handler for the given aggregate type.
func NewHandler[A Aggregate](store EventStore, registry *Registry, factory func(id string) A, log *slog.Logger) *Handler[A] {
	return &Handler[A]{
		store:    store,
		registry: registry,
		factory:  factory,
		log:      log,
		locks:    newAggregateLocks(),
		cache:    newAggregateCache[A](),
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

// pendingSnapshot holds snapshot data captured under the lock so that the
// expensive proto.Marshal + SaveSnapshot can happen outside the lock.
type pendingSnapshot struct {
	msg     proto.Message
	version int
}

// Handle loads the aggregate, calls execute to produce new events, and appends
// them to the store. On optimistic concurrency conflicts the entire cycle is
// retried up to maxRetries times.
func (h *Handler[A]) Handle(ctx context.Context, cmd Command, execute func(A) ([]Event, error)) error {
	aggregateID := cmd.AggregateID()

	h.locks.Lock(aggregateID)
	newEvents, snap, err := h.handleLocked(ctx, aggregateID, execute)
	h.locks.Unlock(aggregateID)

	if err != nil {
		return err
	}

	// Both happen outside the per-aggregate lock:
	h.saveSnapshot(ctx, aggregateID, snap)
	h.publishEvents(ctx, aggregateID, newEvents)
	return nil
}

// handleLocked runs the retry loop while holding the per-aggregate lock.
func (h *Handler[A]) handleLocked(ctx context.Context, aggregateID string, execute func(A) ([]Event, error)) ([]Event, *pendingSnapshot, error) {
	for attempt := range maxRetries {
		if attempt > 0 {
			h.log.Info("retrying command", "aggregate_id", aggregateID, "attempt", attempt+1)
		}

		newEvents, snap, err := h.tryHandle(ctx, aggregateID, execute)
		if errors.Is(err, ErrOptimisticConcurrency) {
			h.log.Warn("optimistic concurrency conflict, will retry", "aggregate_id", aggregateID, "attempt", attempt+1)
			continue
		}
		if err != nil {
			return nil, nil, err
		}

		return newEvents, snap, nil
	}

	return nil, nil, fmt.Errorf("append events: %w", ErrOptimisticConcurrency)
}

// saveSnapshot marshals and persists a pending snapshot if one was captured.
func (h *Handler[A]) saveSnapshot(ctx context.Context, aggregateID string, snap *pendingSnapshot) {
	if snap == nil {
		return
	}

	data, err := proto.Marshal(snap.msg)
	if err != nil {
		h.log.Error("failed to marshal snapshot", "aggregate_id", aggregateID, "error", err)
		return
	}

	s := Snapshot{
		AggregateID: aggregateID,
		Version:     snap.version,
		Data:        data,
	}

	if err := h.snapshots.SaveSnapshot(ctx, s); err != nil {
		h.log.Error("failed to save snapshot", "aggregate_id", aggregateID, "error", err)
		return
	}

	h.log.Info("snapshot saved", "aggregate_id", aggregateID, "version", snap.version)
}

// publishEvents publishes new events if a publisher is configured.
func (h *Handler[A]) publishEvents(ctx context.Context, aggregateID string, newEvents []Event) {
	if h.publisher != nil && len(newEvents) > 0 {
		if err := h.publisher.Publish(ctx, newEvents); err != nil {
			h.log.Error("failed to publish events", "aggregate_id", aggregateID, "error", err)
		}
	}
}

// tryHandle performs a single load→execute→append attempt.
func (h *Handler[A]) tryHandle(ctx context.Context, aggregateID string, execute func(A) ([]Event, error)) ([]Event, *pendingSnapshot, error) {
	h.log.Debug("handling command", "aggregate_id", aggregateID)

	var agg A
	var expectedVersion int
	var snapshotVersion int

	if cached, ok := h.cache.Take(aggregateID); ok {
		agg = cached.agg
		expectedVersion = cached.version
		snapshotVersion = cached.snapshotVersion
		h.log.Debug("using cached aggregate", "aggregate_id", aggregateID, "version", expectedVersion)
	} else {
		agg = h.factory(aggregateID)

		lr, err := h.loadAggregate(ctx, agg, aggregateID)
		if err != nil {
			return nil, nil, err
		}
		expectedVersion = lr.expectedVersion
		snapshotVersion = lr.snapshotVersion
	}

	newEvents, err := execute(agg)
	if err != nil {
		h.log.Warn("command execution failed", "aggregate_id", aggregateID, "error", err)
		return nil, nil, fmt.Errorf("execute command: %w", err)
	}

	if len(newEvents) == 0 {
		// No mutation occurred, safe to put back unchanged.
		h.cache.Put(aggregateID, agg, expectedVersion, snapshotVersion)
		return nil, nil, nil
	}

	rawNew := make([]RawEvent, len(newEvents))
	for i, evt := range newEvents {
		raw, err := h.registry.Serialize(evt)
		if err != nil {
			return nil, nil, fmt.Errorf("serialize event: %w", err)
		}
		rawNew[i] = raw
	}

	if err := h.store.Append(ctx, aggregateID, expectedVersion, rawNew); err != nil {
		// Append failed — don't cache, aggregate may be partially mutated.
		return nil, nil, err
	}

	newVersion := expectedVersion + len(newEvents)

	for i := range newEvents {
		newEvents[i].Version = expectedVersion + i + 1
	}

	h.log.Debug("events appended", "aggregate_id", aggregateID, "new_event_count", len(newEvents), "new_version", newVersion)

	// Capture snapshot under the lock (cheap — just creates proto objects).
	// The expensive marshal + save happens outside the lock in Handle().
	snap := h.maybeCaptureSnapshot(agg, aggregateID, newVersion, snapshotVersion)
	if snap != nil {
		snapshotVersion = snap.version
	}

	// Cache the aggregate at its new version for the next command.
	h.cache.Put(aggregateID, agg, newVersion, snapshotVersion)

	return newEvents, snap, nil
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

// maybeCaptureSnapshot captures snapshot data if the aggregate supports it and
// the current version has crossed a snapshot interval threshold since the last
// snapshot. It returns a pendingSnapshot (cheap — just proto objects, no
// serialization) or nil if no snapshot is needed.
func (h *Handler[A]) maybeCaptureSnapshot(agg A, aggregateID string, currentVersion, snapshotVersion int) *pendingSnapshot {
	if h.snapshots == nil {
		return nil
	}

	sa, ok := Aggregate(agg).(Snapshotable)
	if !ok {
		return nil
	}

	interval := sa.SnapshotInterval()
	if interval <= 0 {
		return nil
	}

	// Capture a snapshot if we've crossed a threshold boundary since the last snapshot.
	if currentVersion/interval <= snapshotVersion/interval {
		return nil
	}

	msg, err := sa.Snapshot()
	if err != nil {
		h.log.Error("failed to create snapshot", "aggregate_id", aggregateID, "error", err)
		return nil
	}

	return &pendingSnapshot{
		msg:     msg,
		version: currentVersion,
	}
}
