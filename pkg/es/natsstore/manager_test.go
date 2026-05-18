package natsstore_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/natsstore"
)

// resettableProjection records every event it sees in a slice. Reset wipes
// the slice; mirrors what a real PG projection does with TRUNCATE.
type resettableProjection struct {
	mu     sync.Mutex
	events []es.Event
	resets int
}

func (p *resettableProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, events...)
	return nil
}

func (p *resettableProjection) Reset(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = nil
	p.resets++
	return nil
}

func (p *resettableProjection) snapshot() ([]es.Event, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]es.Event, len(p.events))
	copy(out, p.events)
	return out, p.resets
}

func TestProjectionManager_RebuildReplaysFromZero(t *testing.T) {
	_, js := startNATS(t)
	ctx := context.Background()
	registry := newTestRegistry()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	publisher := natsstore.NewPublisher(js, slog.Default())
	publishOrderPlaced(t, ctx, publisher, 5, 1)

	checkpoint := newMemCheckpointStore()
	proj := &resettableProjection{}
	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "test-rebuild").
		WithPersistent(checkpoint, proj)
	require.NoError(t, consumer.Start(ctx))

	events, resets := proj.snapshot()
	require.Len(t, events, 5)
	require.Equal(t, 0, resets)
	require.Equal(t, uint64(5), checkpoint.seqs["test-rebuild"])

	mgr := natsstore.NewProjectionManager(js, slog.Default())
	mgr.Add(consumer)

	// Status before rebuild: 5 events processed, 0 lag.
	statuses, err := mgr.List(ctx)
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	require.Equal(t, "test-rebuild", statuses[0].Name)
	require.Equal(t, uint64(5), statuses[0].Checkpoint)
	require.Equal(t, uint64(5), statuses[0].HeadSequence)
	require.Equal(t, uint64(0), statuses[0].Lag)
	require.True(t, statuses[0].Rebuildable)
	require.Equal(t, 1, statuses[0].ResettableCount)

	// Subscribe for progress before kicking off the rebuild.
	progressCh, cancelSub, err := mgr.Subscribe("test-rebuild")
	require.NoError(t, err)
	defer cancelSub()

	require.NoError(t, mgr.Rebuild(ctx, "test-rebuild"))

	events, resets = proj.snapshot()
	assert.Equal(t, 1, resets, "Reset should fire exactly once")
	assert.Len(t, events, 5, "all 5 events should replay")
	assert.Equal(t, uint64(5), checkpoint.seqs["test-rebuild"], "checkpoint should land back at 5")

	// We should have seen at least two progress events: one or more batch
	// ticks during catch-up plus the terminal Running tick.
	collected := drainProgress(progressCh, 250*time.Millisecond)
	require.NotEmpty(t, collected)
	var sawRebuilding, sawRunning bool
	for _, evt := range collected {
		switch evt.Phase {
		case natsstore.PhaseRebuilding:
			sawRebuilding = true
		case natsstore.PhaseRunning:
			sawRunning = true
		}
	}
	assert.True(t, sawRebuilding, "expected at least one rebuilding tick")
	assert.True(t, sawRunning, "expected terminal running tick")
}

func TestProjectionManager_RejectsReactorConsumer(t *testing.T) {
	_, js := startNATS(t)
	ctx := context.Background()
	registry := newTestRegistry()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	checkpoint := newMemCheckpointStore()
	proj := &resettableProjection{}
	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "reactor").
		WithPersistent(checkpoint, proj).
		WithReactor()
	require.NoError(t, consumer.Start(ctx))

	mgr := natsstore.NewProjectionManager(js, slog.Default())
	mgr.Add(consumer)

	statuses, err := mgr.List(ctx)
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.False(t, statuses[0].Rebuildable)
	assert.Contains(t, statuses[0].ReasonNotRebuildable, "reactor")

	err = mgr.Rebuild(ctx, "reactor")
	require.ErrorIs(t, err, natsstore.ErrNotRebuildable)
}

func TestProjectionManager_AfterRebuildNewEventsStillFlow(t *testing.T) {
	_, js := startNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := newTestRegistry()

	_, err := natsstore.EnsureStream(ctx, js)
	require.NoError(t, err)

	publisher := natsstore.NewPublisher(js, slog.Default())
	publishOrderPlaced(t, ctx, publisher, 3, 1)

	checkpoint := newMemCheckpointStore()
	proj := &resettableProjection{}
	consumer := natsstore.NewProjectionConsumer(js, registry, slog.Default(), "after-rebuild").
		WithPersistent(checkpoint, proj)
	require.NoError(t, consumer.Start(ctx))

	mgr := natsstore.NewProjectionManager(js, slog.Default())
	mgr.Add(consumer)
	require.NoError(t, mgr.Rebuild(ctx, "after-rebuild"))

	// Publish a new event after the rebuild; the restarted consumer
	// should pick it up on its next poll.
	publishOrderPlaced(t, ctx, publisher, 1, 4)

	require.Eventually(t, func() bool {
		events, _ := proj.snapshot()
		// 3 replayed + 1 new = 4
		for _, evt := range events {
			data := evt.Data.(*orderbookv1.OrderPlaced)
			if data.OrderId == fmt.Sprintf("order-%d", 4) {
				return true
			}
		}
		return false
	}, 3*time.Second, 50*time.Millisecond)
}

func drainProgress(ch <-chan natsstore.ProgressEvent, wait time.Duration) []natsstore.ProgressEvent {
	deadline := time.After(wait)
	var out []natsstore.ProgressEvent
	for {
		select {
		case evt, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, evt)
		case <-deadline:
			return out
		}
	}
}

