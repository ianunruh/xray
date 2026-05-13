package memstore

import (
	"context"
	"sync"

	"github.com/ianunruh/xray/pkg/es"
)

// Store is an in-memory EventStore and SnapshotStore implementation, suitable for testing.
type Store struct {
	mu        sync.Mutex
	streams   map[string][]es.RawEvent
	snapshots map[string]es.Snapshot
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
		stream = append(stream, evt)
	}

	s.streams[aggregateID] = stream
	return nil
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
