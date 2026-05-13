package es

// Aggregate is the interface that domain aggregates must implement.
type Aggregate interface {
	AggregateID() string
	Apply(Event) error
}

// AggregateBase provides common aggregate bookkeeping: ID and version tracking.
// Embed this in domain aggregates.
type AggregateBase struct {
	id      string
	version int
}

// SetID initializes the aggregate's ID. Call this from the aggregate factory.
func (b *AggregateBase) SetID(id string) {
	b.id = id
}

// AggregateID returns the aggregate's ID.
func (b *AggregateBase) AggregateID() string {
	return b.id
}

// Version returns the current version (number of events applied).
func (b *AggregateBase) Version() int {
	return b.version
}

// SetVersion sets the version directly. Used when restoring from a snapshot.
func (b *AggregateBase) SetVersion(v int) {
	b.version = v
}

// IncrementVersion bumps the version by one. Called after each event is applied.
func (b *AggregateBase) IncrementVersion() {
	b.version++
}
