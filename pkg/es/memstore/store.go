package memstore

import (
	"context"
	"sync"

	"github.com/ianunruh/xray/pkg/es"
)

// Store is an in-memory EventStore implementation, suitable for testing.
type Store struct {
	mu      sync.Mutex
	streams map[string][]es.RawEvent
}

// New creates a new in-memory event store.
func New() *Store {
	return &Store{
		streams: make(map[string][]es.RawEvent),
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
