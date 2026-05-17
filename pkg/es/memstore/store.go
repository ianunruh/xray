package memstore

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ianunruh/xray/pkg/es"
)

// Store is an in-memory EventStore and SnapshotStore implementation, suitable for testing.
type Store struct {
	mu           sync.Mutex
	streams      map[string][]es.RawEvent
	snapshots    map[string]es.Snapshot
	nextPosition atomic.Int64
}

// New creates a new in-memory event store.
func New() *Store {
	return &Store{
		streams:   make(map[string][]es.RawEvent),
		snapshots: make(map[string]es.Snapshot),
	}
}

// Load returns all events for the given aggregate ID, ordered by version.
func (s *Store) Load(_ context.Context, aggregateID string) ([]es.RawEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.streams[aggregateID]
	// Return a copy to prevent mutation.
	out := make([]es.RawEvent, len(events))
	copy(out, events)
	return out, nil
}

// LoadFrom returns events for the given aggregate starting from fromVersion (inclusive).
func (s *Store) LoadFrom(_ context.Context, aggregateID string, fromVersion int) ([]es.RawEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.streams[aggregateID]
	// Versions are 1-based; find the slice index where Version >= fromVersion.
	start := 0
	for i, evt := range events {
		if evt.Version >= fromVersion {
			start = i
			break
		}
		start = i + 1
	}

	out := make([]es.RawEvent, len(events)-start)
	copy(out, events[start:])
	return out, nil
}

// LoadRange returns events with version in [fromVersion, toVersion].
// toVersion <= 0 means no upper bound.
func (s *Store) LoadRange(_ context.Context, aggregateID string, fromVersion, toVersion int) ([]es.RawEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.streams[aggregateID]
	out := make([]es.RawEvent, 0, len(events))
	for _, evt := range events {
		if evt.Version < fromVersion {
			continue
		}
		if toVersion > 0 && evt.Version > toVersion {
			break
		}
		out = append(out, evt)
	}
	return out, nil
}

// VersionAtTimestamp returns the largest version with timestamp <= ts, or 0
// if no such event exists.
func (s *Store) VersionAtTimestamp(_ context.Context, aggregateID string, ts time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.streams[aggregateID]
	best := 0
	for _, evt := range events {
		if evt.Timestamp.After(ts) {
			break
		}
		best = evt.Version
	}
	return best, nil
}

// StreamMetadata returns version and timestamp bounds for the stream.
func (s *Store) StreamMetadata(_ context.Context, aggregateID string) (es.StreamMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.streams[aggregateID]
	if len(events) == 0 {
		return es.StreamMetadata{}, nil
	}
	return es.StreamMetadata{
		FirstVersion:   events[0].Version,
		LastVersion:    events[len(events)-1].Version,
		FirstTimestamp: events[0].Timestamp,
		LastTimestamp:  events[len(events)-1].Timestamp,
	}, nil
}

// Append adds new events to the stream. It returns ErrOptimisticConcurrency
// if the expected version doesn't match the current stream length.
func (s *Store) Append(_ context.Context, aggregateID string, expectedVersion int, events []es.RawEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream := s.streams[aggregateID]
	if len(stream) != expectedVersion {
		return es.ErrOptimisticConcurrency
	}

	for i, evt := range events {
		evt.AggregateID = aggregateID
		evt.Version = expectedVersion + i + 1
		evt.Position = s.nextPosition.Add(1)
		stream = append(stream, evt)
	}

	s.streams[aggregateID] = stream
	return nil
}

// LoadAll returns all events across all aggregates, sorted by position.
func (s *Store) LoadAll(_ context.Context) ([]es.RawEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var all []es.RawEvent
	for _, events := range s.streams {
		all = append(all, events...)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Position < all[j].Position
	})

	return all, nil
}

// LoadAfter returns up to limit events with position > afterPosition, sorted by position.
func (s *Store) LoadAfter(_ context.Context, afterPosition int64, limit int) ([]es.RawEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var matching []es.RawEvent
	for _, events := range s.streams {
		for _, evt := range events {
			if evt.Position > afterPosition {
				matching = append(matching, evt)
			}
		}
	}

	sort.Slice(matching, func(i, j int) bool {
		return matching[i].Position < matching[j].Position
	})

	if limit > 0 && len(matching) > limit {
		matching = matching[:limit]
	}

	return matching, nil
}

// LoadSnapshot returns the most recent snapshot for the aggregate, or nil if none exists.
func (s *Store) LoadSnapshot(_ context.Context, aggregateID string) (*es.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, ok := s.snapshots[aggregateID]
	if !ok {
		return nil, nil
	}
	return &snap, nil
}

// SaveSnapshot persists a snapshot, replacing any existing one for the aggregate.
func (s *Store) SaveSnapshot(_ context.Context, snap es.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshots[snap.AggregateID] = snap
	return nil
}
