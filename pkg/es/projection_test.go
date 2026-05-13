package es_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

type recordingProjection struct {
	events []es.Event
}

func (p *recordingProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.events = append(p.events, events...)
	return nil
}

type failingProjection struct {
	err error
}

func (p *failingProjection) HandleEvents(_ context.Context, _ []es.Event) error {
	return p.err
}

func TestFanOutPublisher_DispatchesToAll(t *testing.T) {
	p1 := &recordingProjection{}
	p2 := &recordingProjection{}

	publisher := es.NewFanOutPublisher(slog.Default(), p1, p2)

	events := []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId: "order-1",
				Symbol:  "AAPL",
			},
		},
	}

	err := publisher.Publish(context.Background(), events)
	require.NoError(t, err)

	publisher.Close()

	assert.Len(t, p1.events, 1)
	assert.Len(t, p2.events, 1)
	assert.Equal(t, "order-1", p1.events[0].Data.(*orderbookv1.OrderPlaced).OrderId)
}

func TestFanOutPublisher_FailingProjectionDoesNotBlockOthers(t *testing.T) {
	failing := &failingProjection{err: errors.New("projection failed")}
	recording := &recordingProjection{}

	publisher := es.NewFanOutPublisher(slog.Default(), failing, recording)

	events := []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId: "order-1",
			},
		},
	}

	err := publisher.Publish(context.Background(), events)
	require.NoError(t, err)

	publisher.Close()

	// The recording projection still received the events.
	assert.Len(t, recording.events, 1)
}

func TestFanOutPublisher_CloseIsIdempotent(t *testing.T) {
	publisher := es.NewFanOutPublisher(slog.Default())
	publisher.Close()
	publisher.Close() // should not panic
}

func TestHydrateProjections(t *testing.T) {
	registry := newTestRegistry()

	raw1, err := registry.Serialize(es.Event{
		Type: "OrderPlaced",
		Data: &orderbookv1.OrderPlaced{
			OrderId:  "order-1",
			Symbol:   "AAPL",
			PlacedAt: timestamppb.Now(),
		},
	})
	require.NoError(t, err)

	raw2, err := registry.Serialize(es.Event{
		Type: "TradeExecuted",
		Data: &orderbookv1.TradeExecuted{
			TradeId:    "trade-1",
			Symbol:     "AAPL",
			ExecutedAt: timestamppb.Now(),
		},
	})
	require.NoError(t, err)

	loader := &stubLoader{events: []es.RawEvent{raw1, raw2}}
	p := &recordingProjection{}

	err = es.HydrateProjections(context.Background(), loader, registry, slog.Default(), p)
	require.NoError(t, err)
	assert.Len(t, p.events, 2)
}

type stubLoader struct {
	events []es.RawEvent
}

func (l *stubLoader) LoadAll(_ context.Context) ([]es.RawEvent, error) {
	return l.events, nil
}
