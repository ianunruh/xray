package natsstore_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
	"github.com/ianunruh/xray/pkg/es/natsstore"
)

func startNATS(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()

	opts := &server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err)

	ns.Start()
	t.Cleanup(ns.Shutdown)

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(func() { nc.Drain() })

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	return nc, js
}

func newTestRegistry() *es.Registry {
	registry := es.NewRegistry()
	registry.Register("OrderPlaced", func() proto.Message { return new(orderbookv1.OrderPlaced) })
	registry.Register("TradeExecuted", func() proto.Message { return new(orderbookv1.TradeExecuted) })
	registry.Register("OrderCancelled", func() proto.Message { return new(orderbookv1.OrderCancelled) })
	return registry
}

func TestPublisher(t *testing.T) {
	_, js := startNATS(t)
	ctx := context.Background()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	publisher := natsstore.NewPublisher(js, slog.Default())

	now := time.Now()
	events := []es.Event{
		{
			AggregateID: "orderbook:AAPL",
			Type:        "OrderPlaced",
			Version:     1,
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 10,
				PlacedAt: timestamppb.New(now),
			},
		},
		{
			AggregateID: "orderbook:AAPL",
			Type:        "TradeExecuted",
			Version:     2,
			Timestamp:   now,
			Data: &orderbookv1.TradeExecuted{
				TradeId:     "trade-1",
				BuyOrderId:  "order-1",
				SellOrderId: "order-2",
				Symbol:      "AAPL",
				Price:       1500000,
				Quantity:    5,
				ExecutedAt:  timestamppb.New(now),
			},
		},
	}

	err = publisher.Publish(ctx, events)
	require.NoError(t, err)

	stream, err := js.Stream(ctx, natsstore.StreamName)
	require.NoError(t, err)

	info, err := stream.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), info.State.Msgs)
}

func TestPublisher_Dedup(t *testing.T) {
	_, js := startNATS(t)
	ctx := context.Background()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	publisher := natsstore.NewPublisher(js, slog.Default())

	evt := es.Event{
		AggregateID: "orderbook:AAPL",
		Type:        "OrderPlaced",
		Version:     1,
		Timestamp:   time.Now(),
		Data: &orderbookv1.OrderPlaced{
			OrderId: "order-1",
			Symbol:  "AAPL",
		},
	}

	require.NoError(t, publisher.Publish(ctx, []es.Event{evt}))
	require.NoError(t, publisher.Publish(ctx, []es.Event{evt}))

	stream, err := js.Stream(ctx, natsstore.StreamName)
	require.NoError(t, err)

	info, err := stream.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.State.Msgs)
}

func TestProjectionConsumer_CatchUp(t *testing.T) {
	_, js := startNATS(t)
	ctx := context.Background()
	registry := newTestRegistry()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	publisher := natsstore.NewPublisher(js, slog.Default())
	now := time.Now()

	for i := range 5 {
		err := publisher.Publish(ctx, []es.Event{{
			AggregateID: "orderbook:AAPL",
			Type:        "OrderPlaced",
			Version:     i + 1,
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  fmt.Sprintf("order-%d", i+1),
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 10,
				PlacedAt: timestamppb.New(now),
			},
		}})
		require.NoError(t, err)
	}

	proj := newCollectingProjection()

	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "test-replay").
		WithEphemeral(proj)
	err = consumer.Start(ctx)
	require.NoError(t, err)

	received := proj.Snapshot()
	assert.Len(t, received, 5)
	for i, evt := range received {
		data := evt.Data.(*orderbookv1.OrderPlaced)
		assert.Equal(t, fmt.Sprintf("order-%d", i+1), data.OrderId)
	}
}

func TestProjectionConsumer_Ongoing(t *testing.T) {
	_, js := startNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := newTestRegistry()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	proj := newCollectingProjection()

	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "test-ongoing").
		WithEphemeral(proj)
	err = consumer.Start(ctx)
	require.NoError(t, err)
	assert.Zero(t, proj.Len())

	publisher := natsstore.NewPublisher(js, slog.Default())
	now := time.Now()
	err = publisher.Publish(ctx, []es.Event{{
		AggregateID: "orderbook:AAPL",
		Type:        "OrderPlaced",
		Version:     1,
		Timestamp:   now,
		Data: &orderbookv1.OrderPlaced{
			OrderId:  "order-1",
			Symbol:   "AAPL",
			Price:    1500000,
			Quantity: 10,
			PlacedAt: timestamppb.New(now),
		},
	}})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return proj.Len() == 1
	}, 2*time.Second, 10*time.Millisecond)

	received := proj.Snapshot()
	data := received[0].Data.(*orderbookv1.OrderPlaced)
	assert.Equal(t, "order-1", data.OrderId)
}

func TestBackfill(t *testing.T) {
	_, js := startNATS(t)
	ctx := context.Background()
	registry := newTestRegistry()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	store := memstore.New()

	now := time.Now()
	for i := range 3 {
		evt := es.Event{
			AggregateID: "orderbook:AAPL",
			Type:        "OrderPlaced",
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  fmt.Sprintf("order-%d", i+1),
				Symbol:   "AAPL",
				Price:    1500000,
				Quantity: 10,
				PlacedAt: timestamppb.New(now),
			},
		}
		raw, err := registry.Serialize(evt)
		require.NoError(t, err)
		require.NoError(t, store.Append(ctx, "orderbook:AAPL", i, []es.RawEvent{raw}))
	}

	err = natsstore.Backfill(ctx, store, js, slog.Default())
	require.NoError(t, err)

	stream, err := js.Stream(ctx, natsstore.StreamName)
	require.NoError(t, err)

	info, err := stream.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), info.State.Msgs)

	// Running backfill again should be a no-op.
	err = natsstore.Backfill(ctx, store, js, slog.Default())
	require.NoError(t, err)

	info, err = stream.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), info.State.Msgs)
}

func TestSubject(t *testing.T) {
	assert.Equal(t, "events.orderbook.AAPL.OrderPlaced", natsstore.Subject("orderbook:AAPL", "OrderPlaced"))
	assert.Equal(t, "events.orderbook.MSFT.TradeExecuted", natsstore.Subject("orderbook:MSFT", "TradeExecuted"))
}

func publishOrderPlaced(t *testing.T, ctx context.Context, publisher *natsstore.Publisher, n, startVersion int) {
	t.Helper()
	now := time.Now()
	for i := range n {
		err := publisher.Publish(ctx, []es.Event{{
			AggregateID: "orderbook:AAPL",
			Type:        "OrderPlaced",
			Version:     startVersion + i,
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  fmt.Sprintf("order-%d", startVersion+i),
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 10,
				PlacedAt: timestamppb.New(now),
			},
		}})
		require.NoError(t, err)
	}
}

func TestProjectionConsumer_PersistentResumesFromCheckpoint(t *testing.T) {
	// A persistent consumer keeps its JetStream cursor across restarts:
	// the second boot only sees events past the saved checkpoint.
	_, js := startNATS(t)
	ctx := context.Background()
	registry := newTestRegistry()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	publisher := natsstore.NewPublisher(js, slog.Default())
	publishOrderPlaced(t, ctx, publisher, 5, 1)

	checkpoint := newMemCheckpointStore()

	proj := newCollectingProjection()
	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "test-resume").
		WithPersistent(checkpoint, proj)
	require.NoError(t, consumer.Start(ctx))

	assert.Equal(t, 5, proj.Len())
	assert.Equal(t, uint64(5), checkpoint.seqs["test-resume"])

	publishOrderPlaced(t, ctx, publisher, 2, 6)

	proj.Reset()
	consumer2 := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "test-resume").
		WithPersistent(checkpoint, proj)
	require.NoError(t, consumer2.Start(ctx))

	assert.Equal(t, 2, proj.Len())
	assert.Equal(t, uint64(7), checkpoint.seqs["test-resume"])
}

func TestProjectionConsumer_EphemeralReplaysFromZero(t *testing.T) {
	// An ephemeral consumer (no checkpoint store) discards its JetStream
	// cursor on every boot so in-memory projections rebuild from scratch.
	_, js := startNATS(t)
	ctx := context.Background()
	registry := newTestRegistry()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	publisher := natsstore.NewPublisher(js, slog.Default())
	publishOrderPlaced(t, ctx, publisher, 5, 1)

	proj := newCollectingProjection()
	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "test-replay-boot").
		WithEphemeral(proj)
	require.NoError(t, consumer.Start(ctx))
	assert.Equal(t, 5, proj.Len())

	publishOrderPlaced(t, ctx, publisher, 2, 6)

	proj.Reset()
	consumer2 := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "test-replay-boot").
		WithEphemeral(proj)
	require.NoError(t, consumer2.Start(ctx))
	assert.Equal(t, 7, proj.Len()) // sees all 7 — cursor was reset on boot
}

func TestProjectionConsumer_IndependentCursorsPerName(t *testing.T) {
	// Two consumers with different names must hold independent cursors:
	// advancing "A" past some events must not skip those events for "B".
	_, js := startNATS(t)
	ctx := context.Background()
	registry := newTestRegistry()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	publisher := natsstore.NewPublisher(js, slog.Default())
	now := time.Now()

	for i := range 3 {
		err := publisher.Publish(ctx, []es.Event{{
			AggregateID: "orderbook:AAPL",
			Type:        "OrderPlaced",
			Version:     i + 1,
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  fmt.Sprintf("order-%d", i+1),
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 10,
				PlacedAt: timestamppb.New(now),
			},
		}})
		require.NoError(t, err)
	}

	checkpoint := newMemCheckpointStore()

	// Run consumer A: catches up through all 3 events, checkpoints.
	projA := newCollectingProjection()
	consumerA := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "consumer-a").
		WithPersistent(checkpoint, projA)
	require.NoError(t, consumerA.Start(ctx))
	assert.Equal(t, 3, projA.Len())
	assert.Equal(t, uint64(3), checkpoint.seqs["consumer-a"])

	// Consumer B starts fresh on the same store but with a different name:
	// must see all 3 events regardless of consumer A's progress.
	projB := newCollectingProjection()
	consumerB := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "consumer-b").
		WithPersistent(checkpoint, projB)
	require.NoError(t, consumerB.Start(ctx))
	assert.Equal(t, 3, projB.Len())
	assert.Equal(t, uint64(3), checkpoint.seqs["consumer-b"])
	assert.Equal(t, uint64(3), checkpoint.seqs["consumer-a"]) // unaffected
}

// collectingProjection records every event it receives. HandleEvents
// runs on the consumer's goroutine while assertions read the slice
// from the test goroutine, so all access goes through the mutex.
type collectingProjection struct {
	mu     sync.Mutex
	events []es.Event
}

func newCollectingProjection() *collectingProjection {
	return &collectingProjection{}
}

func (p *collectingProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, events...)
	return nil
}

func (p *collectingProjection) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

// Snapshot returns a copy of the collected events. Use this instead of
// retaining the slice across multiple assertions, since the consumer
// goroutine may keep appending.
func (p *collectingProjection) Snapshot() []es.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]es.Event, len(p.events))
	copy(out, p.events)
	return out
}

func (p *collectingProjection) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = nil
}

type memCheckpointStore struct {
	seqs map[string]uint64
}

func newMemCheckpointStore() *memCheckpointStore {
	return &memCheckpointStore{seqs: make(map[string]uint64)}
}

func (s *memCheckpointStore) LoadCheckpoint(_ context.Context, name string) (uint64, error) {
	return s.seqs[name], nil
}

func (s *memCheckpointStore) SaveCheckpoint(_ context.Context, name string, seq uint64) error {
	s.seqs[name] = seq
	return nil
}

func (s *memCheckpointStore) DeleteCheckpoint(_ context.Context, name string) error {
	delete(s.seqs, name)
	return nil
}
