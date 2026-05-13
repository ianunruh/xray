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

// Snapshotable is implemented by aggregates that support periodic snapshotting.
type Snapshotable interface {
	Snapshot() (proto.Message, error)
	RestoreSnapshot(proto.Message) error
	SnapshotInterval() int
}
