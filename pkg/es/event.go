package es

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

// Event is the deserialized domain event used by aggregates.
type Event struct {
	ID            string
	CausationID   string
	CorrelationID string
	AggregateID   string
	Type          string
	Version       int
	Position      int64
	Timestamp     time.Time
	Data          proto.Message
}

// RawEvent is the store-level representation with serialized data.
type RawEvent struct {
	ID            string
	CausationID   string
	CorrelationID string
	AggregateID   string
	Type          string
	Version       int
	Position      int64
	Timestamp     time.Time
	Data          []byte
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
		ID:            e.ID,
		CausationID:   e.CausationID,
		CorrelationID: e.CorrelationID,
		AggregateID:   e.AggregateID,
		Type:          e.Type,
		Version:       e.Version,
		Timestamp:     e.Timestamp,
		Data:          data,
	}, nil
}

// SerializeInto marshals e.Data by appending to buf, so multiple events
// from the same command can share one pooled buffer. Returns the
// RawEvent (whose Data sub-slices the buffer) and the new buffer with
// the appended bytes. Caller owns the buffer's lifetime and must keep
// it alive at least until the resulting RawEvent.Data has been consumed
// (e.g. the store Append returns).
func (r *Registry) SerializeInto(e Event, buf []byte) (RawEvent, []byte, error) {
	start := len(buf)
	appended, err := proto.MarshalOptions{}.MarshalAppend(buf, e.Data)
	if err != nil {
		return RawEvent{}, buf, fmt.Errorf("marshal event %s: %w", e.Type, err)
	}
	return RawEvent{
		ID:            e.ID,
		CausationID:   e.CausationID,
		CorrelationID: e.CorrelationID,
		AggregateID:   e.AggregateID,
		Type:          e.Type,
		Version:       e.Version,
		Timestamp:     e.Timestamp,
		Data:          appended[start:],
	}, appended, nil
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
		ID:            raw.ID,
		CausationID:   raw.CausationID,
		CorrelationID: raw.CorrelationID,
		AggregateID:   raw.AggregateID,
		Type:          raw.Type,
		Version:       raw.Version,
		Position:      raw.Position,
		Timestamp:     raw.Timestamp,
		Data:          msg,
	}, nil
}
