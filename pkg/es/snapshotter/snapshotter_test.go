package snapshotter_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
	"github.com/ianunruh/xray/pkg/es/snapshotter"
)

// snapshotAggregate is a Snapshotable test aggregate counting OrderPlaced events.
type snapshotAggregate struct {
	es.AggregateBase
	orderCount int
	interval   int
}

func newSnapshotAggregate(id string, interval int) *snapshotAggregate {
	a := &snapshotAggregate{interval: interval}
	a.SetID(id)
	return a
}

func (a *snapshotAggregate) Apply(evt es.Event) error {
	switch evt.Data.(type) {
	case *orderbookv1.OrderPlaced:
		a.orderCount++
	}
	a.IncrementVersion()
	return nil
}

func (a *snapshotAggregate) Snapshot() (proto.Message, error) {
	return &orderbookv1.OrderBookSnapshot{Symbol: fmt.Sprintf("count:%d", a.orderCount)}, nil
}

func (a *snapshotAggregate) RestoreSnapshot(msg proto.Message) error {
	snap := msg.(*orderbookv1.OrderBookSnapshot)
	var count int
	fmt.Sscanf(snap.Symbol, "count:%d", &count)
	a.orderCount = count
	return nil
}

func (a *snapshotAggregate) SnapshotInterval() int { return a.interval }

// plainAggregate is a non-Snapshotable test aggregate.
type plainAggregate struct {
	es.AggregateBase
}

func newPlainAggregate(id string) *plainAggregate {
	a := &plainAggregate{}
	a.SetID(id)
	return a
}

func (a *plainAggregate) Apply(_ es.Event) error {
	a.IncrementVersion()
	return nil
}

// saveCountingStore wraps memstore.Store and counts SaveSnapshot calls.
type saveCountingStore struct {
	*memstore.Store
	saves atomic.Int64
}

func (s *saveCountingStore) SaveSnapshot(ctx context.Context, snap es.Snapshot) error {
	s.saves.Add(1)
	return s.Store.SaveSnapshot(ctx, snap)
}

func newRegistry() *es.Registry {
	r := es.NewRegistry()
	r.Register("OrderPlaced", func() proto.Message { return new(orderbookv1.OrderPlaced) })
	return r
}

// makeRawEvent serializes an OrderPlaced event and appends it to the store
// at the next version for the given aggregate. Returns the dispatched
// es.Event (with Version populated).
func appendEvent(t *testing.T, store *memstore.Store, registry *es.Registry, id string, version int) es.Event {
	t.Helper()
	evt := es.Event{
		AggregateID: id,
		Type:        "OrderPlaced",
		Version:     version,
		Timestamp:   time.Now(),
		Data: &orderbookv1.OrderPlaced{
			OrderId:  fmt.Sprintf("order-%d", version),
			Symbol:   "AAPL",
			PlacedAt: timestamppb.Now(),
		},
	}
	raw, err := registry.Serialize(evt)
	require.NoError(t, err)
	raw.Version = version
	require.NoError(t, store.Append(context.Background(), id, version-1, []es.RawEvent{raw}))
	return evt
}

// dispatch builds an es.Event slice with a given range of versions for an
// aggregate, WITHOUT touching the store. Use when you want to drive
// HandleEvents directly without going through Append (e.g. simulating live
// dispatch where catch-up reads the store separately).
func dispatch(id string, fromVersion, toVersion int) []es.Event {
	events := make([]es.Event, 0, toVersion-fromVersion+1)
	for v := fromVersion; v <= toVersion; v++ {
		events = append(events, es.Event{
			AggregateID: id,
			Type:        "OrderPlaced",
			Version:     v,
			Timestamp:   time.Now(),
			Data: &orderbookv1.OrderPlaced{
				OrderId:  fmt.Sprintf("order-%d", v),
				Symbol:   "AAPL",
				PlacedAt: timestamppb.Now(),
			},
		})
	}
	return events
}

// fakeClock is a controllable monotonic clock for eviction tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// feed dispatches events one-by-one into the snapshotter, appending each to
// the store first so that lazy hydration (if it kicks in) finds a consistent
// stream.
func feed(t *testing.T, snap *snapshotter.Snapshotter, store *memstore.Store, registry *es.Registry, id string, fromVersion, toVersion int) {
	t.Helper()
	ctx := context.Background()
	for v := fromVersion; v <= toVersion; v++ {
		evt := appendEvent(t, store, registry, id, v)
		require.NoError(t, snap.HandleEvents(ctx, []es.Event{evt}))
	}
}

func TestSnapshotter_SavesAtIntervalBoundary(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	snap := snapshotter.New(store, counting, registry, slog.Default())
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	feed(t, snap, store, registry, "orderbook:AAPL", 1, 9)

	// Snapshots fire at v3, v6, v9 (3 boundary crossings).
	assert.Equal(t, int64(3), counting.saves.Load())

	persisted, err := counting.LoadSnapshot(context.Background(), "orderbook:AAPL")
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, 9, persisted.Version)

	// Round-trip the persisted snapshot to confirm aggregate state is right.
	restored := newSnapshotAggregate("orderbook:AAPL", 3)
	var msg orderbookv1.OrderBookSnapshot
	require.NoError(t, proto.Unmarshal(persisted.Data, &msg))
	require.NoError(t, restored.RestoreSnapshot(&msg))
	assert.Equal(t, 9, restored.orderCount)
}

func TestSnapshotter_NoSaveBeforeFirstBoundary(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	snap := snapshotter.New(store, counting, registry, slog.Default())
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 5)
	})

	feed(t, snap, store, registry, "orderbook:AAPL", 1, 4)

	assert.Equal(t, int64(0), counting.saves.Load())
	persisted, err := counting.LoadSnapshot(context.Background(), "orderbook:AAPL")
	require.NoError(t, err)
	assert.Nil(t, persisted)
}

func TestSnapshotter_UnregisteredPrefixIgnored(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	snap := snapshotter.New(store, counting, registry, slog.Default())
	// No registration.

	// HandleEvents shouldn't error and shouldn't try to hydrate or save.
	require.NoError(t, snap.HandleEvents(context.Background(), dispatch("unknown:foo", 1, 10)))
	assert.Equal(t, int64(0), counting.saves.Load())
}

func TestSnapshotter_NonSnapshotableIgnored(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	snap := snapshotter.New(store, counting, registry, slog.Default())
	snap.Register("plain", func(id string) es.Aggregate {
		return newPlainAggregate(id)
	})

	require.NoError(t, snap.HandleEvents(context.Background(), dispatch("plain:foo", 1, 10)))
	assert.Equal(t, int64(0), counting.saves.Load())
}

func TestSnapshotter_IndependentAggregates(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	snap := snapshotter.New(store, counting, registry, slog.Default())
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	// Interleave two aggregates.
	feed(t, snap, store, registry, "orderbook:AAPL", 1, 6) // 2 saves
	feed(t, snap, store, registry, "orderbook:GOOG", 1, 4) // 1 save (v3)

	assert.Equal(t, int64(3), counting.saves.Load())

	a, err := counting.LoadSnapshot(context.Background(), "orderbook:AAPL")
	require.NoError(t, err)
	require.NotNil(t, a)
	assert.Equal(t, 6, a.Version)

	g, err := counting.LoadSnapshot(context.Background(), "orderbook:GOOG")
	require.NoError(t, err)
	require.NotNil(t, g)
	assert.Equal(t, 3, g.Version)
}

func TestSnapshotter_RestartLazyHydrates(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	// First instance: feed v1-v6, snapshot lands at v3 and v6.
	snap1 := snapshotter.New(store, counting, registry, slog.Default())
	snap1.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})
	feed(t, snap1, store, registry, "orderbook:AAPL", 1, 6)
	require.Equal(t, int64(2), counting.saves.Load())

	// Append v7-v9 directly to the store (live writes happened while
	// snapshotter was down).
	appendEvent(t, store, registry, "orderbook:AAPL", 7)
	appendEvent(t, store, registry, "orderbook:AAPL", 8)
	appendEvent(t, store, registry, "orderbook:AAPL", 9)

	// Second instance: simulates restart. Cursor would resume from a
	// projection consumer; here we simulate by directly dispatching v9.
	// The snapshotter should: load snapshot at v6, LoadFrom(7) → v7,v8
	// (assuming JetStream caught the snapshotter up to v8 via v9 being
	// the first new event… actually the snapshotter would process v7,v8,v9
	// from JetStream. Simulate that by dispatching all three).
	snap2 := snapshotter.New(store, counting, registry, slog.Default())
	snap2.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	require.NoError(t, snap2.HandleEvents(context.Background(), dispatch("orderbook:AAPL", 7, 9)))

	// v9 crosses the next boundary → one more save.
	assert.Equal(t, int64(3), counting.saves.Load())

	persisted, err := counting.LoadSnapshot(context.Background(), "orderbook:AAPL")
	require.NoError(t, err)
	require.Equal(t, 9, persisted.Version)

	restored := newSnapshotAggregate("orderbook:AAPL", 3)
	var msg orderbookv1.OrderBookSnapshot
	require.NoError(t, proto.Unmarshal(persisted.Data, &msg))
	require.NoError(t, restored.RestoreSnapshot(&msg))
	assert.Equal(t, 9, restored.orderCount, "restored count includes all 9 events")
}

func TestSnapshotter_LiveEventBehindStoreHead(t *testing.T) {
	// Lazy hydrate reads LoadFrom(snap.Version+1). If the store has more
	// events than what the snapshotter is about to apply (because the
	// snapshotter's cursor is well behind head), the incoming live event
	// may have a version <= e.version after hydration. That event must be
	// skipped, not double-applied. Hydration that crosses a boundary
	// triggers a post-hydrate save so the snapshot doesn't sit stale.
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	// Populate v1-v10 directly.
	for v := 1; v <= 10; v++ {
		appendEvent(t, store, registry, "orderbook:AAPL", v)
	}

	snap := snapshotter.New(store, counting, registry, slog.Default())
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	// First live event the snapshotter sees is v5. Hydration catches the
	// in-memory aggregate up to v10 and (since 10 crossed multiple
	// boundaries past lastSaved=0) issues one save at v10.
	require.NoError(t, snap.HandleEvents(context.Background(), []es.Event{
		dispatch("orderbook:AAPL", 5, 5)[0],
	}))
	assert.Equal(t, int64(1), counting.saves.Load())

	persisted, err := counting.LoadSnapshot(context.Background(), "orderbook:AAPL")
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, 10, persisted.Version)

	// Now dispatch v6-v10: all skipped (version <= 10), no new save.
	require.NoError(t, snap.HandleEvents(context.Background(), dispatch("orderbook:AAPL", 6, 10)))
	assert.Equal(t, int64(1), counting.saves.Load())

	// v11: 11/3=3, lastSaved/3=10/3=3 → no boundary crossed, no save.
	evt := appendEvent(t, store, registry, "orderbook:AAPL", 11)
	require.NoError(t, snap.HandleEvents(context.Background(), []es.Event{evt}))
	assert.Equal(t, int64(1), counting.saves.Load())

	// v12: 12/3=4 > 3 → save.
	evt = appendEvent(t, store, registry, "orderbook:AAPL", 12)
	require.NoError(t, snap.HandleEvents(context.Background(), []es.Event{evt}))
	assert.Equal(t, int64(2), counting.saves.Load())
}

func TestSnapshotter_GapErrors(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	snap := snapshotter.New(store, counting, registry, slog.Default())
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 100)
	})

	// Feed v1 normally.
	feed(t, snap, store, registry, "orderbook:AAPL", 1, 1)

	// Append v2 and v3 to the store but dispatch only v3 — simulates a
	// dropped/out-of-order delivery. The snapshotter should error.
	appendEvent(t, store, registry, "orderbook:AAPL", 2)
	evt3 := appendEvent(t, store, registry, "orderbook:AAPL", 3)

	err := snap.HandleEvents(context.Background(), []es.Event{evt3})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gap")
}

// failingSnapshotStore wraps memstore but fails the first SaveSnapshot call.
type failingSnapshotStore struct {
	*memstore.Store
	failed atomic.Bool
}

func (s *failingSnapshotStore) SaveSnapshot(ctx context.Context, snap es.Snapshot) error {
	if !s.failed.Swap(true) {
		return fmt.Errorf("simulated save failure")
	}
	return s.Store.SaveSnapshot(ctx, snap)
}

func TestSnapshotter_SaveFailureRetriesNextBoundary(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	failing := &failingSnapshotStore{Store: store}

	snap := snapshotter.New(store, failing, registry, slog.Default())
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	// Feed v1-v6. Save at v3 fails; save at v6 succeeds.
	feed(t, snap, store, registry, "orderbook:AAPL", 1, 6)

	persisted, err := failing.LoadSnapshot(context.Background(), "orderbook:AAPL")
	require.NoError(t, err)
	require.NotNil(t, persisted, "second boundary save should land")
	assert.Equal(t, 6, persisted.Version)
}

func TestSnapshotter_CatchUpCoalescesSaves(t *testing.T) {
	// A single HandleEvents batch that crosses many boundaries for one
	// aggregate should produce exactly one save per aggregate per
	// invocation, not one per boundary. Mirrors the catch-up path that
	// runs after the snapshotter restarts and drains a long backlog.
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	snap := snapshotter.New(store, counting, registry, slog.Default())
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 100)
	})

	const events = 10_000

	for v := 1; v <= events; v++ {
		appendEvent(t, store, registry, "orderbook:AAPL", v)
	}

	// Dispatch v1..events as one batch. lastSavedVersion=0, version goes
	// 0 → events, crosses events/100 boundaries — but should save once.
	require.NoError(t, snap.HandleEvents(context.Background(), dispatch("orderbook:AAPL", 1, events)))
	assert.Equal(t, int64(1), counting.saves.Load(), "one save per aggregate per batch")

	persisted, err := counting.LoadSnapshot(context.Background(), "orderbook:AAPL")
	require.NoError(t, err)
	assert.Equal(t, events, persisted.Version)

	// Two aggregates in one batch → exactly two saves.
	for v := 1; v <= 500; v++ {
		appendEvent(t, store, registry, "orderbook:GOOG", v)
		appendEvent(t, store, registry, "orderbook:MSFT", v)
	}
	counting.saves.Store(0)
	batch := append(dispatch("orderbook:GOOG", 1, 500), dispatch("orderbook:MSFT", 1, 500)...)
	require.NoError(t, snap.HandleEvents(context.Background(), batch))
	assert.Equal(t, int64(2), counting.saves.Load(), "two aggregates → two saves")
}

func TestSnapshotter_EvictsIdleAggregates(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	clock := newFakeClock(time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	snap := snapshotter.New(store, counting, registry, slog.Default()).
		WithMaxIdle(10 * time.Minute).
		WithClock(clock.Now)
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	feed(t, snap, store, registry, "orderbook:AAPL", 1, 3)
	feed(t, snap, store, registry, "orderbook:GOOG", 1, 3)
	require.Equal(t, 2, snap.Len(), "both aggregates in memory")

	// Advance time past the idle threshold without touching either.
	clock.Advance(11 * time.Minute)
	evicted := snap.Sweep()
	assert.Equal(t, 2, evicted)
	assert.Equal(t, 0, snap.Len(), "both aggregates evicted")
}

func TestSnapshotter_SweepKeepsRecent(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	clock := newFakeClock(time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	snap := snapshotter.New(store, counting, registry, slog.Default()).
		WithMaxIdle(10 * time.Minute).
		WithClock(clock.Now)
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	feed(t, snap, store, registry, "orderbook:AAPL", 1, 3)

	// Less than maxIdle elapsed → no eviction.
	clock.Advance(9 * time.Minute)
	assert.Equal(t, 0, snap.Sweep())
	assert.Equal(t, 1, snap.Len())
}

func TestSnapshotter_DisabledByDefault(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	clock := newFakeClock(time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	snap := snapshotter.New(store, counting, registry, slog.Default()).
		WithClock(clock.Now)
	// No WithMaxIdle call.
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	feed(t, snap, store, registry, "orderbook:AAPL", 1, 3)
	clock.Advance(100 * time.Hour)
	assert.Equal(t, 0, snap.Sweep(), "eviction disabled → Sweep is a no-op")
	assert.Equal(t, 1, snap.Len())
}

func TestSnapshotter_EvictedAggregateLazyRehydrates(t *testing.T) {
	// After eviction, the next event for the same aggregate must
	// re-hydrate from the persisted snapshot rather than apply against
	// stale state. The boundary check fires correctly from the rehydrated
	// state.
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	clock := newFakeClock(time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	snap := snapshotter.New(store, counting, registry, slog.Default()).
		WithMaxIdle(time.Minute).
		WithClock(clock.Now)
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	// Feed v1-v3 → saves at v3, in-memory at v3.
	feed(t, snap, store, registry, "orderbook:AAPL", 1, 3)
	require.Equal(t, int64(1), counting.saves.Load())

	// Sweep evicts the entry.
	clock.Advance(2 * time.Minute)
	require.Equal(t, 1, snap.Sweep())
	require.Equal(t, 0, snap.Len())

	// Append + dispatch v4. Lazy hydrate from snapshot at v3 → apply v4.
	// 4/3=1, lastSaved/3=3/3=1 → no boundary crossed, no save.
	evt := appendEvent(t, store, registry, "orderbook:AAPL", 4)
	require.NoError(t, snap.HandleEvents(context.Background(), []es.Event{evt}))
	assert.Equal(t, int64(1), counting.saves.Load())
	assert.Equal(t, 1, snap.Len(), "rehydrated entry back in memory")

	// Dispatch v5, v6 → crosses 6/3=2 > 1 → save at v6.
	for v := 5; v <= 6; v++ {
		evt := appendEvent(t, store, registry, "orderbook:AAPL", v)
		require.NoError(t, snap.HandleEvents(context.Background(), []es.Event{evt}))
	}
	assert.Equal(t, int64(2), counting.saves.Load())

	persisted, err := counting.LoadSnapshot(context.Background(), "orderbook:AAPL")
	require.NoError(t, err)
	assert.Equal(t, 6, persisted.Version)
}

func TestSnapshotter_MalformedIDIgnored(t *testing.T) {
	registry := newRegistry()
	store := memstore.New()
	counting := &saveCountingStore{Store: store}

	snap := snapshotter.New(store, counting, registry, slog.Default())
	snap.Register("orderbook", func(id string) es.Aggregate {
		return newSnapshotAggregate(id, 3)
	})

	// ID with no ':' has no prefix → ignored.
	require.NoError(t, snap.HandleEvents(context.Background(), dispatch("no-prefix", 1, 5)))
	assert.Equal(t, int64(0), counting.saves.Load())
}
