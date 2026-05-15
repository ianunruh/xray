package natsstore_test

import (
	"context"
	"fmt"
	"log/slog"
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

	var received []es.Event
	proj := &collectingProjection{events: &received}

	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default()).
		WithEphemeral(proj)
	err = consumer.Start(ctx)
	require.NoError(t, err)

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

	var received []es.Event
	proj := &collectingProjection{events: &received}

	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default()).
		WithEphemeral(proj)
	err = consumer.Start(ctx)
	require.NoError(t, err)
	assert.Empty(t, received)

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
		return len(received) == 1
	}, 2*time.Second, 10*time.Millisecond)

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

func TestProjectionConsumer_Checkpoint(t *testing.T) {
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

	checkpoint := &memCheckpointStore{}

	// First run: persistent projection sees all 5 events.
	var ephemeralEvents, persistentEvents []es.Event
	ephemeral := &collectingProjection{events: &ephemeralEvents}
	persistent := &collectingProjection{events: &persistentEvents}

	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default()).
		WithEphemeral(ephemeral).
		WithPersistent(checkpoint, persistent)
	err = consumer.Start(ctx)
	require.NoError(t, err)

	assert.Len(t, ephemeralEvents, 5)
	assert.Len(t, persistentEvents, 5)
	assert.NotZero(t, checkpoint.seq)

	// Publish 2 more events.
	for i := range 2 {
		err := publisher.Publish(ctx, []es.Event{{
			AggregateID: "orderbook:AAPL",
			Type:        "OrderPlaced",
			Version:     6 + i,
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  fmt.Sprintf("order-%d", 6+i),
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 10,
				PlacedAt: timestamppb.New(now),
			},
		}})
		require.NoError(t, err)
	}

	// Second run: ephemeral sees all 7, persistent sees only 2 new.
	ephemeralEvents = nil
	persistentEvents = nil

	consumer2 := natsstore.NewProjectionConsumer(js, registry, slog.Default()).
		WithEphemeral(ephemeral).
		WithPersistent(checkpoint, persistent)
	err = consumer2.Start(ctx)
	require.NoError(t, err)

	assert.Len(t, ephemeralEvents, 7)
	assert.Len(t, persistentEvents, 2)
}

type collectingProjection struct {
	events *[]es.Event
}

func (p *collectingProjection) HandleEvents(_ context.Context, events []es.Event) error {
	*p.events = append(*p.events, events...)
	return nil
}

type memCheckpointStore struct {
	seq uint64
}

func (s *memCheckpointStore) LoadCheckpoint(_ context.Context, _ string) (uint64, error) {
	return s.seq, nil
}

func (s *memCheckpointStore) SaveCheckpoint(_ context.Context, _ string, seq uint64) error {
	s.seq = seq
	return nil
}
