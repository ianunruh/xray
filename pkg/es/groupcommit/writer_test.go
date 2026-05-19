package groupcommit

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
	"github.com/ianunruh/xray/pkg/es/memstore"
	"github.com/ianunruh/xray/pkg/es/pgstore"
)

// fakeBatcher routes AppendMulti to per-aggregate Appends on a backing
// memstore. AppendMulti's "either all or none" contract is honored by
// running the inserts inside a mutex; on first error the rest are
// skipped and the whole call returns that error.
type fakeBatcher struct {
	mu       sync.Mutex
	mem      *memstore.Store
	failNext atomic.Int32 // number of AppendMulti calls to fail before succeeding
	batches  atomic.Int64 // count of AppendMulti calls (success or fail)
}

func newFakeBatcher() *fakeBatcher {
	return &fakeBatcher{mem: memstore.New()}
}

func (f *fakeBatcher) AppendMulti(ctx context.Context, reqs []pgstore.AppendRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches.Add(1)

	if n := f.failNext.Load(); n > 0 {
		f.failNext.Add(-1)
		return errors.New("simulated batched commit failure")
	}

	for _, r := range reqs {
		if err := f.mem.Append(ctx, r.AggregateID, r.ExpectedVersion, r.Events); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeBatcher) Append(ctx context.Context, aggregateID string, expectedVersion int, events []es.RawEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mem.Append(ctx, aggregateID, expectedVersion, events)
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func raw(aggregateID, id string) es.RawEvent {
	return es.RawEvent{
		ID:          id,
		AggregateID: aggregateID,
		Type:        "Test",
		Data:        []byte{0},
		Timestamp:   time.Now(),
	}
}

func TestWriterBatchesConcurrentAppends(t *testing.T) {
	f := newFakeBatcher()
	w := newWithBatcher(f.mem, f,
		Config{MaxBatch: 32, MaxWait: 5 * time.Millisecond, QueueDepth: 64},
		quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = w.Append(ctx, "orderbook:AAPL"+string(rune('A'+i)), 0, []es.RawEvent{raw("a", "e"+string(rune('A'+i)))})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "append %d", i)
	}

	// All N appends should have been packed into a small number of batches.
	require.LessOrEqual(t, f.batches.Load(), int64(2), "expected coalescing; got %d batches", f.batches.Load())

	cancel()
	<-w.Stopped()
}

func TestWriterFallsBackOnBatchFailure(t *testing.T) {
	f := newFakeBatcher()
	f.failNext.Store(1) // first AppendMulti fails; per-request Append should still succeed

	w := newWithBatcher(f.mem, f,
		Config{MaxBatch: 8, MaxWait: 2 * time.Millisecond, QueueDepth: 32},
		quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	const n = 4
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = w.Append(ctx, "orderbook:X"+string(rune('A'+i)), 0, []es.RawEvent{raw("x", "e"+string(rune('A'+i)))})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "fallback append %d", i)
	}

	cancel()
	<-w.Stopped()
}

func TestWriterCtxCancelUnblocksAppend(t *testing.T) {
	f := newFakeBatcher()
	// No goroutine ever calls Run, so requests will queue forever — until ctx cancels.
	w := newWithBatcher(f.mem, f,
		Config{MaxBatch: 1, MaxWait: time.Second, QueueDepth: 1},
		quietLog())

	ctx, cancel := context.WithCancel(context.Background())
	// Fill the queue depth of 1 first so the next Append blocks on enqueue.
	op := &appendOp{
		req:  pgstore.AppendRequest{AggregateID: "orderbook:Y", Events: []es.RawEvent{raw("y", "e1")}},
		done: make(chan error, 1),
	}
	w.inbox <- op

	done := make(chan error, 1)
	go func() {
		done <- w.Append(ctx, "orderbook:Z", 0, []es.RawEvent{raw("z", "e2")})
	}()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Append did not unblock on ctx cancel")
	}
}
