package asyncpublisher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ianunruh/xray/pkg/es"
)

type fakeInner struct {
	mu       sync.Mutex
	received [][]es.Event
	fail     atomic.Int32 // number of Publish calls to fail
}

func (f *fakeInner) Publish(_ context.Context, events []es.Event) error {
	if n := f.fail.Load(); n > 0 {
		f.fail.Add(-1)
		return errors.New("simulated publish failure")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// Copy the slice so the test can inspect it safely.
	batch := make([]es.Event, len(events))
	copy(batch, events)
	f.received = append(f.received, batch)
	return nil
}

func (f *fakeInner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.received)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPublishReturnsImmediately(t *testing.T) {
	inner := &fakeInner{}
	p := New(inner, Config{QueueDepth: 16}, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	const n = 8
	start := time.Now()
	for i := 0; i < n; i++ {
		require.NoError(t, p.Publish(ctx, []es.Event{{ID: "e", Type: "T"}}))
	}
	// Should be near-instant since we never wait on the inner publisher.
	require.Less(t, time.Since(start), 10*time.Millisecond)

	// Wait for the drain goroutine to catch up.
	require.Eventually(t, func() bool { return inner.count() == n }, time.Second, time.Millisecond)
}

func TestPublishPreservesOrder(t *testing.T) {
	inner := &fakeInner{}
	p := New(inner, Config{QueueDepth: 32}, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	const n = 16
	for i := 0; i < n; i++ {
		require.NoError(t, p.Publish(ctx, []es.Event{{ID: "e", Type: "T", Version: i}}))
	}
	require.Eventually(t, func() bool { return inner.count() == n }, time.Second, time.Millisecond)

	inner.mu.Lock()
	defer inner.mu.Unlock()
	for i, batch := range inner.received {
		require.Equal(t, i, batch[0].Version)
	}
}

func TestInnerErrorDoesNotPropagate(t *testing.T) {
	inner := &fakeInner{}
	inner.fail.Store(1) // first publish fails

	p := New(inner, Config{QueueDepth: 4}, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	require.NoError(t, p.Publish(ctx, []es.Event{{ID: "a"}}))
	require.NoError(t, p.Publish(ctx, []es.Event{{ID: "b"}}))

	require.Eventually(t, func() bool { return inner.count() == 1 }, time.Second, time.Millisecond)
}

func TestPublishBlocksWhenQueueFull(t *testing.T) {
	inner := &fakeInner{}
	// Capacity 1, and we never Run, so the queue stays full.
	p := New(inner, Config{QueueDepth: 1}, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, p.Publish(ctx, []es.Event{{ID: "a"}})) // fills the queue

	done := make(chan error, 1)
	go func() {
		done <- p.Publish(ctx, []es.Event{{ID: "b"}})
	}()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Publish did not unblock on ctx cancel")
	}
}

func TestRunDrainsQueueOnShutdown(t *testing.T) {
	inner := &fakeInner{}
	p := New(inner, Config{QueueDepth: 8}, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 4; i++ {
		require.NoError(t, p.Publish(ctx, []es.Event{{ID: "e", Version: i}}))
	}
	// Now start Run with an already-cancelled context: it should still
	// drain the queued batches before returning.
	cancel()
	p.Run(ctx)
	<-p.Stopped()

	require.Equal(t, 4, inner.count())
}

func TestEmptyPublishIsNoop(t *testing.T) {
	inner := &fakeInner{}
	p := New(inner, Config{QueueDepth: 4}, quiet())

	require.NoError(t, p.Publish(context.Background(), nil))
	require.Equal(t, 0, inner.count())
}
