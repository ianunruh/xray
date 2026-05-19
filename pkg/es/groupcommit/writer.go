// Package groupcommit wraps an event store so concurrent Append calls
// are coalesced into a single transaction per fsync. Reads pass straight
// through; only the write path is intercepted.
//
// Trade-off: each Append waits up to MaxWait for siblings to join the
// batch. Under low load that wait is pure added latency, so the writer
// is only worth enabling when sustained throughput is the goal. At high
// concurrency the shared fsync usually dominates the wait.
package groupcommit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/ianunruh/xray/internal/metrics"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/pgstore"
)

// BatchAppender is the subset of pgstore.Store the writer needs. Defined
// as an interface so tests can supply a fake.
type BatchAppender interface {
	AppendMulti(ctx context.Context, reqs []pgstore.AppendRequest) error
	Append(ctx context.Context, aggregateID string, expectedVersion int, events []es.RawEvent) error
}

// Config controls the drain loop.
type Config struct {
	// MaxBatch flushes as soon as this many requests are queued.
	MaxBatch int
	// MaxWait flushes once the oldest queued request has waited this long.
	MaxWait time.Duration
	// QueueDepth is the buffer between callers and the drain goroutine.
	// When full, Append blocks (i.e. natural backpressure).
	QueueDepth int
}

// Default returns a Config tuned for "modest throughput, sub-ms latency."
// Adjust based on load-test data.
func Default() Config {
	return Config{MaxBatch: 64, MaxWait: 500 * time.Microsecond, QueueDepth: 1024}
}

type appendOp struct {
	req  pgstore.AppendRequest
	done chan error
}

// Writer satisfies es.EventStore via the embedded store, intercepting
// Append to route through the group-commit drain loop.
type Writer struct {
	es.EventStore // delegate for reads + metadata

	store   BatchAppender
	cfg     Config
	log     *slog.Logger
	inbox   chan *appendOp
	stopped chan struct{}
	once    sync.Once
}

// New constructs a Writer wrapping store. Call Run in a goroutine to start
// draining; Append blocks until then (callers will queue, drain consumes).
func New(store *pgstore.Store, cfg Config, log *slog.Logger) *Writer {
	return &Writer{
		EventStore: store,
		store:      store,
		cfg:        cfg,
		log:        log,
		inbox:      make(chan *appendOp, cfg.QueueDepth),
		stopped:    make(chan struct{}),
	}
}

// newWithBatcher is a test seam: build a Writer around any BatchAppender
// and EventStore pair without requiring a real pgstore.
func newWithBatcher(reads es.EventStore, batcher BatchAppender, cfg Config, log *slog.Logger) *Writer {
	return &Writer{
		EventStore: reads,
		store:      batcher,
		cfg:        cfg,
		log:        log,
		inbox:      make(chan *appendOp, cfg.QueueDepth),
		stopped:    make(chan struct{}),
	}
}

// Append enqueues the request onto the drain loop and waits for the
// outcome. Honors ctx cancellation in both directions.
func (w *Writer) Append(ctx context.Context, aggregateID string, expectedVersion int, events []es.RawEvent) error {
	op := &appendOp{
		req:  pgstore.AppendRequest{AggregateID: aggregateID, ExpectedVersion: expectedVersion, Events: events},
		done: make(chan error, 1),
	}
	select {
	case w.inbox <- op:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-op.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run drains the inbox until ctx is cancelled. Returns when the drain
// loop has shut down and all in-flight requests have been answered.
func (w *Writer) Run(ctx context.Context) {
	defer close(w.stopped)
	for {
		op, ok := w.waitFirst(ctx)
		if !ok {
			w.drainRemaining()
			return
		}
		ops := w.collect(ctx, op)
		w.flush(ctx, ops)
	}
}

// waitFirst blocks until either ctx cancels or a request arrives.
func (w *Writer) waitFirst(ctx context.Context) (*appendOp, bool) {
	select {
	case op := <-w.inbox:
		return op, true
	case <-ctx.Done():
		return nil, false
	}
}

// collect greedily grabs up to MaxBatch-1 additional requests (op is the
// first) or stops on MaxWait expiry, whichever comes first.
func (w *Writer) collect(ctx context.Context, first *appendOp) []*appendOp {
	ops := make([]*appendOp, 1, w.cfg.MaxBatch)
	ops[0] = first

	if w.cfg.MaxBatch <= 1 {
		return ops
	}

	timer := time.NewTimer(w.cfg.MaxWait)
	defer timer.Stop()

	for len(ops) < w.cfg.MaxBatch {
		select {
		case op := <-w.inbox:
			ops = append(ops, op)
		case <-timer.C:
			return ops
		case <-ctx.Done():
			return ops
		}
	}
	return ops
}

// flush attempts a batched commit, falling back to per-request Append on
// failure so a single bad apple (e.g. optimistic conflict) doesn't kill
// the whole batch.
func (w *Writer) flush(ctx context.Context, ops []*appendOp) {
	flushStart := time.Now()
	w.recordBatchSize(ctx, len(ops))

	reqs := make([]pgstore.AppendRequest, len(ops))
	for i, op := range ops {
		reqs[i] = op.req
	}

	err := w.store.AppendMulti(ctx, reqs)
	if err == nil {
		w.recordFlushSeconds(ctx, time.Since(flushStart).Seconds(), "batched")
		for _, op := range ops {
			op.done <- nil
		}
		return
	}

	// Batched commit failed. We can't blame any single request without
	// savepoints, so retry each individually. The slow path also gives
	// us back per-request error fidelity.
	if !errors.Is(err, es.ErrOptimisticConcurrency) {
		w.log.Warn("group commit failed, falling back to per-request appends",
			"error", err, "batch_size", len(ops))
	}
	w.recordFallbacks(ctx, len(ops))

	for _, op := range ops {
		op.done <- w.store.Append(ctx, op.req.AggregateID, op.req.ExpectedVersion, op.req.Events)
	}
	w.recordFlushSeconds(ctx, time.Since(flushStart).Seconds(), "fallback")
}

// drainRemaining answers any in-flight ops at shutdown so callers don't
// hang forever after ctx is cancelled.
func (w *Writer) drainRemaining() {
	for {
		select {
		case op := <-w.inbox:
			op.done <- fmt.Errorf("group-commit writer shutting down: %w", context.Canceled)
		default:
			return
		}
	}
}

// Stopped returns a channel closed once Run has exited.
func (w *Writer) Stopped() <-chan struct{} { return w.stopped }

// ---- metrics helpers ----

func (w *Writer) recordBatchSize(ctx context.Context, n int) {
	if groupBatchSize == nil {
		return
	}
	groupBatchSize.Record(ctx, int64(n))
}

func (w *Writer) recordFlushSeconds(ctx context.Context, secs float64, kind string) {
	if groupFlushSeconds == nil {
		return
	}
	groupFlushSeconds.Record(ctx, secs, metric.WithAttributes(attribute.String("kind", kind)))
}

func (w *Writer) recordFallbacks(ctx context.Context, n int) {
	if groupFallbacks == nil {
		return
	}
	groupFallbacks.Add(ctx, int64(n))
}

// Instruments built lazily on first Writer construction; package-level so
// they're cheap to access. Init is idempotent.
var (
	groupBatchSize    metric.Int64Histogram
	groupFlushSeconds metric.Float64Histogram
	groupFallbacks    metric.Int64Counter
	instrumentOnce    sync.Once
)

func init() {
	instrumentOnce.Do(func() {
		// Built against metrics.Meter, which is no-op until metrics.Init
		// runs. Safe to construct here either way.
		var err error
		groupBatchSize, err = metrics.Meter.Int64Histogram(
			"xray.groupcommit.batch_size",
			metric.WithDescription("Number of append requests packed into one group-commit flush."),
		)
		if err != nil {
			groupBatchSize = nil
		}
		groupFlushSeconds, err = metrics.Meter.Float64Histogram(
			"xray.groupcommit.flush_seconds",
			metric.WithDescription("End-to-end group-commit flush latency, labeled batched|fallback."),
			metric.WithUnit("s"),
		)
		if err != nil {
			groupFlushSeconds = nil
		}
		groupFallbacks, err = metrics.Meter.Int64Counter(
			"xray.groupcommit.fallbacks_total",
			metric.WithDescription("Requests that fell back to individual append after a batched commit failed."),
		)
		if err != nil {
			groupFallbacks = nil
		}
	})
}
