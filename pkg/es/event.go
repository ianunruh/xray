package es

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

// Event is the deserialized domain event used by aggregates.
type Event struct {
	ID          string
	AggregateID string
	Type        string
	Version     int
	Position    int64
	Timestamp   time.Time
	Data        proto.Message
}

// RawEvent is the store-level representation with serialized data.
type RawEvent struct {
	ID          string
	AggregateID string
	Type        string
	Version     int
	Position    int64
	Timestamp   time.Time
	Data        []byte
}

// Registry maps event type strings to proto.Message factories for
// serialization and deserialization.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]func() proto.Message
}

// NewRegistry creates an empty event type registry.
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]func() proto.Message),
	}
}

// Register adds a mapping from an event type string to a factory function
// that creates a zero-value proto.Message of the corresponding type.
func (r *Registry) Register(eventType string, factory func() proto.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[eventType] = factory
}

// Serialize converts an Event to a RawEvent by marshaling the Data field.
func (r *Registry) Serialize(e Event) (RawEvent, error) {
	data, err := proto.Marshal(e.Data)
	if err != nil {
		return RawEvent{}, fmt.Errorf("marshal event %s: %w", e.Type, err)
	}
	return RawEvent{
		ID:          e.ID,
		AggregateID: e.AggregateID,
		Type:        e.Type,
		Version:     e.Version,
		Timestamp:   e.Timestamp,
		Data:        data,
	}, nil
}

// Deserialize converts a RawEvent to an Event by looking up the factory
// for the event type and unmarshaling the data.
func (r *Registry) Deserialize(raw RawEvent) (Event, error) {
	r.mu.RLock()
	factory, ok := r.factories[raw.Type]
	r.mu.RUnlock()

	if !ok {
		return Event{}, fmt.Errorf("unknown event type: %s", raw.Type)
	}

	msg := factory()
	if err := proto.Unmarshal(raw.Data, msg); err != nil {
		return Event{}, fmt.Errorf("unmarshal event %s: %w", raw.Type, err)
	}

	return Event{
		ID:          raw.ID,
		AggregateID: raw.AggregateID,
		Type:        raw.Type,
		Version:     raw.Version,
		Position:    raw.Position,
		Timestamp:   raw.Timestamp,
		Data:        msg,
	}, nil
}
