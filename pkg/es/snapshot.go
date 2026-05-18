package es

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Snapshot holds a serialized point-in-time capture of an aggregate's state.
type Snapshot struct {
	AggregateID string
	Version     int
	Data        []byte
}

// SnapshotStore persists and loads aggregate snapshots.
type SnapshotStore interface {
	// LoadSnapshot returns the most recent snapshot for the aggregate, or nil if none exists.
	LoadSnapshot(ctx context.Context, aggregateID string) (*Snapshot, error)

	// SaveSnapshot persists a snapshot, replacing any existing one for the aggregate.
	SaveSnapshot(ctx context.Context, snap Snapshot) error
}

// Snapshotable is implemented by aggregates whose state can be captured as a
// proto message and rehydrated from one. The async snapshotter in
// pkg/es/snapshotter writes a snapshot every SnapshotInterval events; the
// read-side Load and LoadAt paths consume them for fast hydration.
type Snapshotable interface {
	Snapshot() (proto.Message, error)
	RestoreSnapshot(proto.Message) error
	SnapshotInterval() int
}
