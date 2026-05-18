// Package snapshotter persists aggregate snapshots asynchronously, off the
// command write path. It is an es.Projection: it subscribes to the event
// stream via the existing ProjectionConsumer machinery, maintains an
// in-memory aggregate per aggregate_id, and writes snapshots on the same
// version/interval boundary the inline producer uses.
package snapshotter

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ianunruh/xray/pkg/es"
)

// Factory creates a fresh aggregate for hydration. The aggregate returned
// must implement es.Snapshotable; factories whose aggregates don't are
// skipped at the first event without surfacing an error.
type Factory func(id string) es.Aggregate

// Snapshotter is an es.Projection that produces snapshots in the
// background. Register one factory per aggregate-ID prefix (the portion
// before ':' in IDs like "orderbook:AAPL"), then wire it as a persistent
// projection consumer.
type Snapshotter struct {
	log       *slog.Logger
	store     es.EventStore
	snapshots es.SnapshotStore
	registry  *es.Registry
	factories map[string]Factory

	mu      sync.Mutex
	aggs    map[string]*entry
	maxIdle time.Duration  // 0 = never evict
	now     func() time.Time
}

// entry holds the live in-memory state for one aggregate.
type entry struct {
	agg              es.Aggregate
	version          int
	lastSavedVersion int
	interval         int
	lastSeen         time.Time
}

// New constructs a Snapshotter. store is used for lazy hydration
// (LoadSnapshot + LoadFrom); snapshots is the write target; registry is
// needed to deserialize events read back from the store during catch-up.
func New(store es.EventStore, snapshots es.SnapshotStore, registry *es.Registry, log *slog.Logger) *Snapshotter {
	return &Snapshotter{
		log:       log,
		store:     store,
		snapshots: snapshots,
		registry:  registry,
		factories: make(map[string]Factory),
		aggs:      make(map[string]*entry),
		now:       time.Now,
	}
}

// Register associates a factory with an aggregate-ID prefix.
func (s *Snapshotter) Register(prefix string, factory Factory) {
	s.factories[prefix] = factory
}

// WithMaxIdle configures eviction: in-memory aggregates that haven't seen
// an event in d are dropped by Sweep. Zero (the default) disables
// eviction. Evicted aggregates lazy-rehydrate on the next event, so
// eviction never loses data — only in-memory state.
func (s *Snapshotter) WithMaxIdle(d time.Duration) *Snapshotter {
	s.maxIdle = d
	return s
}

// WithClock overrides the time source. Used in tests to drive eviction
// deterministically. Production callers shouldn't need this.
func (s *Snapshotter) WithClock(now func() time.Time) *Snapshotter {
	s.now = now
	return s
}

// HandleEvents applies each event to its in-memory aggregate and saves a
// snapshot when the version crosses an interval boundary since the last
// save. Events for unregistered prefixes or non-Snapshotable aggregates are
// ignored.
//
// During catch-up a single batch may carry many events for one aggregate
// crossing many boundaries; the save is deferred until after the batch so
// only one save per aggregate fires regardless of how many boundaries were
// crossed.
func (s *Snapshotter) HandleEvents(ctx context.Context, events []es.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pending := make(map[string]*entry)
	now := s.now()

	for _, evt := range events {
		e, err := s.ensureAggregate(ctx, evt.AggregateID)
		if err != nil {
			s.log.Error("snapshotter hydrate failed",
				"aggregate_id", evt.AggregateID, "error", err)
			continue
		}
		if e == nil {
			continue
		}

		if evt.Version <= e.version {
			// Already applied during lazy hydrate.
			e.lastSeen = now
			continue
		}
		if evt.Version != e.version+1 {
			return fmt.Errorf("snapshotter: gap on %s (have v%d, got v%d)",
				evt.AggregateID, e.version, evt.Version)
		}

		if err := e.agg.Apply(evt); err != nil {
			return fmt.Errorf("apply v%d to %s: %w", evt.Version, evt.AggregateID, err)
		}
		e.version = evt.Version
		e.lastSeen = now

		if e.version/e.interval > e.lastSavedVersion/e.interval {
			pending[evt.AggregateID] = e
		}
	}

	for id, e := range pending {
		if err := s.save(ctx, id, e); err != nil {
			s.log.Error("snapshotter save failed",
				"aggregate_id", id, "version", e.version, "error", err)
			// Don't advance lastSavedVersion; next boundary retries.
		}
	}
	return nil
}

// Sweep drops in-memory aggregates whose lastSeen is older than the
// configured maxIdle. Returns the number of entries evicted. Safe to
// call concurrently with HandleEvents — the next event for an evicted
// aggregate triggers a fresh lazy hydrate.
func (s *Snapshotter) Sweep() int {
	if s.maxIdle <= 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := s.now().Add(-s.maxIdle)
	n := 0
	for id, e := range s.aggs {
		if e.lastSeen.Before(cutoff) {
			delete(s.aggs, id)
			n++
		}
	}
	if n > 0 {
		s.log.Info("snapshotter swept idle aggregates", "evicted", n, "remaining", len(s.aggs))
	}
	return n
}

// Len returns the number of in-memory aggregates the snapshotter is
// currently holding. Useful for telemetry and tests.
func (s *Snapshotter) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.aggs)
}

// ensureAggregate returns the in-memory entry for id, lazy-hydrating from
// the last persisted snapshot and tail events on first sight. Returns
// (nil, nil) if the prefix is unregistered or the aggregate isn't
// Snapshotable — both are silent skips, not errors.
func (s *Snapshotter) ensureAggregate(ctx context.Context, id string) (*entry, error) {
	if e, ok := s.aggs[id]; ok {
		return e, nil
	}

	factory, ok := s.factoryFor(id)
	if !ok {
		return nil, nil
	}

	agg := factory(id)
	sa, ok := agg.(es.Snapshotable)
	if !ok {
		return nil, nil
	}

	interval := sa.SnapshotInterval()
	if interval <= 0 {
		return nil, nil
	}

	startFrom := 1
	lastSaved := 0

	snap, err := s.snapshots.LoadSnapshot(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	if snap != nil {
		msg, err := deserializeSnapshot(sa, snap.Data)
		if err != nil {
			return nil, fmt.Errorf("deserialize snapshot: %w", err)
		}
		if err := sa.RestoreSnapshot(msg); err != nil {
			return nil, fmt.Errorf("restore snapshot: %w", err)
		}
		if vs, ok := agg.(interface{ SetVersion(int) }); ok {
			vs.SetVersion(snap.Version)
		}
		startFrom = snap.Version + 1
		lastSaved = snap.Version
	}

	raw, err := s.store.LoadFrom(ctx, id, startFrom)
	if err != nil {
		return nil, fmt.Errorf("load events from v%d: %w", startFrom, err)
	}
	for _, r := range raw {
		evt, err := s.registry.Deserialize(r)
		if err != nil {
			return nil, fmt.Errorf("deserialize v%d: %w", r.Version, err)
		}
		if err := agg.Apply(evt); err != nil {
			return nil, fmt.Errorf("apply v%d during hydrate: %w", evt.Version, err)
		}
	}

	version := lastSaved + len(raw)
	e := &entry{
		agg:              agg,
		version:          version,
		lastSavedVersion: lastSaved,
		interval:         interval,
		lastSeen:         s.now(),
	}
	s.aggs[id] = e

	// If catch-up alone advanced us past a boundary, save now so the
	// snapshot isn't left stale until the next boundary's worth of live
	// events arrives. Failure is non-fatal — next boundary retries.
	if e.version/e.interval > e.lastSavedVersion/e.interval {
		if err := s.save(ctx, id, e); err != nil {
			s.log.Error("snapshotter post-hydrate save failed",
				"aggregate_id", id, "version", e.version, "error", err)
		}
	}

	return e, nil
}

// save marshals and persists a snapshot, advancing lastSavedVersion only
// on success.
func (s *Snapshotter) save(ctx context.Context, id string, e *entry) error {
	sa := e.agg.(es.Snapshotable)
	msg, err := sa.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	if err := s.snapshots.SaveSnapshot(ctx, es.Snapshot{
		AggregateID: id,
		Version:     e.version,
		Data:        data,
	}); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	e.lastSavedVersion = e.version
	s.log.Info("snapshot saved", "aggregate_id", id, "version", e.version)
	return nil
}

// factoryFor returns the factory registered for the prefix of id (the
// portion before the first ':').
func (s *Snapshotter) factoryFor(id string) (Factory, bool) {
	prefix, _, ok := strings.Cut(id, ":")
	if !ok {
		return nil, false
	}
	f, ok := s.factories[prefix]
	return f, ok
}

// deserializeSnapshot mirrors the helper in pkg/es/command.go.
func deserializeSnapshot(sa es.Snapshotable, data []byte) (proto.Message, error) {
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
