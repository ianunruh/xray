package es

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	"github.com/ianunruh/xray/internal/metrics"
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
	agg     A
	version int
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
func (c *aggregateCache[A]) Put(id string, agg A, version int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[id] = cachedEntry[A]{agg: agg, version: version}
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

// LoadAt rehydrates the aggregate as it existed at exactly atVersion. It uses
// the snapshot store as a starting point when the latest snapshot is at or
// before atVersion; otherwise it replays from the beginning of the stream up
// to atVersion. Intended for read-only "as-of" queries (e.g. time-machine
// replay). atVersion must be > 0.
func (h *Handler[A]) LoadAt(ctx context.Context, aggregateID string, atVersion int) (A, error) {
	var zero A
	if atVersion <= 0 {
		return zero, fmt.Errorf("LoadAt: atVersion must be > 0, got %d", atVersion)
	}

	agg := h.factory(aggregateID)
	startFrom := 1

	if h.snapshots != nil {
		if sa, ok := Aggregate(agg).(Snapshotable); ok {
			snap, err := h.snapshots.LoadSnapshot(ctx, aggregateID)
			if err != nil {
				return zero, fmt.Errorf("load snapshot: %w", err)
			}
			if snap != nil && snap.Version <= atVersion {
				msg, err := h.deserializeSnapshot(sa, snap.Data)
				if err != nil {
					return zero, fmt.Errorf("deserialize snapshot: %w", err)
				}
				if err := sa.RestoreSnapshot(msg); err != nil {
					return zero, fmt.Errorf("restore snapshot: %w", err)
				}
				if ab, ok := Aggregate(agg).(interface{ SetVersion(int) }); ok {
					ab.SetVersion(snap.Version)
				}
				startFrom = snap.Version + 1
			}
		}
	}

	if startFrom > atVersion {
		return agg, nil
	}

	rawEvents, err := h.store.LoadRange(ctx, aggregateID, startFrom, atVersion)
	if err != nil {
		return zero, fmt.Errorf("load events range [%d, %d]: %w", startFrom, atVersion, err)
	}

	for _, raw := range rawEvents {
		evt, err := h.registry.Deserialize(raw)
		if err != nil {
			return zero, fmt.Errorf("deserialize event: %w", err)
		}
		if err := agg.Apply(evt); err != nil {
			return zero, fmt.Errorf("apply event: %w", err)
		}
	}

	return agg, nil
}

// StreamMetadata returns version/timestamp bounds for the aggregate's stream.
// Pass-through to the underlying EventStore.
func (h *Handler[A]) StreamMetadata(ctx context.Context, aggregateID string) (StreamMetadata, error) {
	return h.store.StreamMetadata(ctx, aggregateID)
}

// VersionAtTimestamp returns the largest version with timestamp <= ts, or 0
// if no such event exists. Pass-through to the underlying EventStore.
func (h *Handler[A]) VersionAtTimestamp(ctx context.Context, aggregateID string, ts time.Time) (int, error) {
	return h.store.VersionAtTimestamp(ctx, aggregateID, ts)
}

// LoadEvents returns the deserialized events for an aggregate in the version
// range [fromVersion, toVersion] (inclusive). A toVersion <= 0 means no upper
// bound. Intended for read-only queries that need to inspect the event stream
// directly (e.g. extracting trade events for a replay UI).
func (h *Handler[A]) LoadEvents(ctx context.Context, aggregateID string, fromVersion, toVersion int) ([]Event, error) {
	rawEvents, err := h.store.LoadRange(ctx, aggregateID, fromVersion, toVersion)
	if err != nil {
		return nil, fmt.Errorf("load events range [%d, %d]: %w", fromVersion, toVersion, err)
	}

	events := make([]Event, 0, len(rawEvents))
	for _, raw := range rawEvents {
		evt, err := h.registry.Deserialize(raw)
		if err != nil {
			return nil, fmt.Errorf("deserialize event: %w", err)
		}
		events = append(events, evt)
	}
	return events, nil
}

// stampCausation mints a fresh ID for every event and stamps causation +
// correlation derived from ctx. All events in the batch share the same
// causation (the command is the unit of causation; multi-event splits are a
// serialization detail). If ctx has no Causation, a fresh correlation is
// minted — this is the "origin" case for commands entering from RPC handlers
// or the reconciler.
func stampCausation(ctx context.Context, events []Event) {
	cause, _ := CausationFrom(ctx)
	correlationID := cause.CorrelationID
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	for i := range events {
		if events[i].ID == "" {
			events[i].ID = uuid.NewString()
		}
		events[i].CausationID = cause.CauseID
		events[i].CorrelationID = correlationID
	}
}

// Handle loads the aggregate, calls execute to produce new events, and appends
// them to the store. On optimistic concurrency conflicts the entire cycle is
// retried up to maxRetries times.
func (h *Handler[A]) Handle(ctx context.Context, cmd Command, execute func(A) ([]Event, error)) error {
	aggregateID := cmd.AggregateID()
	aggType := metrics.AggregateType(aggregateID)
	typeAttr := metric.WithAttributes(attribute.String("aggregate_type", aggType))

	start := time.Now()
	h.locks.Lock(aggregateID)
	if metrics.CommandLockWaitSeconds != nil {
		metrics.CommandLockWaitSeconds.Record(ctx, time.Since(start).Seconds(), typeAttr)
	}

	newEvents, err := h.handleLocked(ctx, aggregateID, aggType, execute)
	h.locks.Unlock(aggregateID)

	if err != nil {
		if metrics.CommandHandleSeconds != nil {
			metrics.CommandHandleSeconds.Record(ctx, time.Since(start).Seconds(),
				metric.WithAttributes(
					attribute.String("aggregate_type", aggType),
					attribute.String("result", "error"),
				))
		}
		return err
	}

	// Snapshotting is an async projection (pkg/es/snapshotter); the write
	// path is responsible for events only.
	h.publishEvents(ctx, aggregateID, newEvents)

	if metrics.CommandHandleSeconds != nil {
		metrics.CommandHandleSeconds.Record(ctx, time.Since(start).Seconds(),
			metric.WithAttributes(
				attribute.String("aggregate_type", aggType),
				attribute.String("result", "ok"),
			))
	}
	return nil
}

// handleLocked runs the retry loop while holding the per-aggregate lock.
func (h *Handler[A]) handleLocked(ctx context.Context, aggregateID, aggType string, execute func(A) ([]Event, error)) ([]Event, error) {
	typeAttr := metric.WithAttributes(attribute.String("aggregate_type", aggType))
	for attempt := range maxRetries {
		if attempt > 0 {
			h.log.Info("retrying command", "aggregate_id", aggregateID, "attempt", attempt+1)
			if metrics.CommandRetriesTotal != nil {
				metrics.CommandRetriesTotal.Add(ctx, 1, typeAttr)
			}
		}

		newEvents, err := h.tryHandle(ctx, aggregateID, aggType, execute)
		if errors.Is(err, ErrOptimisticConcurrency) {
			h.log.Warn("optimistic concurrency conflict, will retry", "aggregate_id", aggregateID, "attempt", attempt+1)
			continue
		}
		if err != nil {
			return nil, err
		}

		return newEvents, nil
	}

	return nil, fmt.Errorf("append events: %w", ErrOptimisticConcurrency)
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
func (h *Handler[A]) tryHandle(ctx context.Context, aggregateID, aggType string, execute func(A) ([]Event, error)) ([]Event, error) {
	h.log.Debug("handling command", "aggregate_id", aggregateID)
	typeAttr := metric.WithAttributes(attribute.String("aggregate_type", aggType))

	var agg A
	var expectedVersion int

	if cached, ok := h.cache.Take(aggregateID); ok {
		agg = cached.agg
		expectedVersion = cached.version
		h.log.Debug("using cached aggregate", "aggregate_id", aggregateID, "version", expectedVersion)
		if metrics.AggregateCacheHitsTotal != nil {
			metrics.AggregateCacheHitsTotal.Add(ctx, 1, typeAttr)
		}
	} else {
		agg = h.factory(aggregateID)

		if metrics.AggregateCacheMissTotal != nil {
			metrics.AggregateCacheMissTotal.Add(ctx, 1, typeAttr)
		}
		v, err := h.loadAggregate(ctx, agg, aggregateID)
		if err != nil {
			return nil, err
		}
		expectedVersion = v
	}

	newEvents, err := execute(agg)
	if err != nil {
		h.log.Warn("command execution failed", "aggregate_id", aggregateID, "error", err)
		return nil, fmt.Errorf("execute command: %w", err)
	}

	if len(newEvents) == 0 {
		// No mutation occurred, safe to put back unchanged.
		h.cache.Put(aggregateID, agg, expectedVersion)
		return nil, nil
	}

	stampCausation(ctx, newEvents)

	rawNew := make([]RawEvent, len(newEvents))
	for i, evt := range newEvents {
		raw, err := h.registry.Serialize(evt)
		if err != nil {
			return nil, fmt.Errorf("serialize event: %w", err)
		}
		rawNew[i] = raw
	}

	if err := h.store.Append(ctx, aggregateID, expectedVersion, rawNew); err != nil {
		// Append failed — don't cache, aggregate may be partially mutated.
		return nil, err
	}

	newVersion := expectedVersion + len(newEvents)

	for i := range newEvents {
		newEvents[i].Version = expectedVersion + i + 1
	}

	h.log.Debug("events appended", "aggregate_id", aggregateID, "new_event_count", len(newEvents), "new_version", newVersion)

	// Cache the aggregate at its new version for the next command.
	h.cache.Put(aggregateID, agg, newVersion)

	return newEvents, nil
}

// loadAggregate restores the aggregate from a snapshot (if available) plus
// remaining events, or from the full event stream. Returns the resulting
// stream version.
func (h *Handler[A]) loadAggregate(ctx context.Context, agg A, aggregateID string) (int, error) {
	if h.snapshots != nil {
		snap, err := h.snapshots.LoadSnapshot(ctx, aggregateID)
		if err != nil {
			h.log.Error("failed to load snapshot", "aggregate_id", aggregateID, "error", err)
			return 0,fmt.Errorf("load snapshot: %w", err)
		}

		if snap != nil {
			if sa, ok := Aggregate(agg).(Snapshotable); ok {
				msg, err := h.deserializeSnapshot(sa, snap.Data)
				if err != nil {
					return 0,fmt.Errorf("deserialize snapshot: %w", err)
				}
				if err := sa.RestoreSnapshot(msg); err != nil {
					return 0,fmt.Errorf("restore snapshot: %w", err)
				}

				// Set the version to match the snapshot so Apply increments correctly.
				if ab, ok := Aggregate(agg).(interface{ SetVersion(int) }); ok {
					ab.SetVersion(snap.Version)
				}

				// Load only events after the snapshot.
				rawEvents, err := h.store.LoadFrom(ctx, aggregateID, snap.Version+1)
				if err != nil {
					h.log.Error("failed to load events from version", "aggregate_id", aggregateID, "from_version", snap.Version+1, "error", err)
					return 0,fmt.Errorf("load events from: %w", err)
				}

				for _, raw := range rawEvents {
					evt, err := h.registry.Deserialize(raw)
					if err != nil {
						return 0,fmt.Errorf("deserialize event: %w", err)
					}
					if err := agg.Apply(evt); err != nil {
						return 0,fmt.Errorf("apply event: %w", err)
					}
				}

				h.log.Info("aggregate restored from snapshot", "aggregate_id", aggregateID, "snapshot_version", snap.Version, "events_replayed", len(rawEvents))
				return snap.Version + len(rawEvents), nil
			}
		}
	}

	// No snapshot: load all events.
	rawEvents, err := h.store.Load(ctx, aggregateID)
	if err != nil {
		h.log.Error("failed to load events", "aggregate_id", aggregateID, "error", err)
		return 0,fmt.Errorf("load events: %w", err)
	}

	for _, raw := range rawEvents {
		evt, err := h.registry.Deserialize(raw)
		if err != nil {
			return 0,fmt.Errorf("deserialize event: %w", err)
		}
		if err := agg.Apply(evt); err != nil {
			return 0,fmt.Errorf("apply event: %w", err)
		}
	}

	return len(rawEvents), nil
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

