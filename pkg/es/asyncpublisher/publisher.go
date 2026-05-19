// Package asyncpublisher wraps an es.EventPublisher so Publish enqueues
// onto a buffered channel and returns immediately. A single drain
// goroutine forwards batches to the inner publisher in FIFO order, which
// preserves per-aggregate event ordering (the inner publisher already
// publishes events within a batch in order).
//
// This moves NATS publish off the synchronous response path of
// Handler.Handle. The trade-off is durability of the projection-side
// view: if the process crashes between Append and the drain catching
// up, queued events are lost. They remain durable in Postgres, so
// natsstore.Backfill recovers them on the next start.
package asyncpublisher

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel/metric"

	"github.com/ianunruh/xray/internal/metrics"
	"github.com/ianunruh/xray/pkg/es"
)

// Config controls the queue + drain behaviour.
type Config struct {
	// QueueDepth is the buffer between callers and the drain goroutine.
	// When full, Publish blocks (backpressure).
	QueueDepth int
}

// Default returns a queue depth suited to bursty trade flow without
// dominating memory.
func Default() Config {
	return Config{QueueDepth: 4096}
}

// Publisher is an es.EventPublisher that decouples Publish from the
// caller's goroutine.
type Publisher struct {
	inner   es.EventPublisher
	queue   chan []es.Event
	log     *slog.Logger
	stopped chan struct{}
	once    sync.Once
}

// New wraps inner with a queue of cfg.QueueDepth slots. Run must be
// called in a goroutine before Publish will make progress.
func New(inner es.EventPublisher, cfg Config, log *slog.Logger) *Publisher {
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = Default().QueueDepth
	}
	return &Publisher{
		inner:   inner,
		queue:   make(chan []es.Event, cfg.QueueDepth),
		log:     log,
		stopped: make(chan struct{}),
	}
}

// Publish enqueues the batch and returns. Blocks only if the queue is
// full (i.e. the drain goroutine has fallen behind), which is the
// natural backpressure signal. Honors ctx cancellation while blocked.
func (p *Publisher) Publish(ctx context.Context, events []es.Event) error {
	if len(events) == 0 {
		return nil
	}
	select {
	case p.queue <- events:
		p.recordQueueDelta(ctx, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run drains the queue until ctx cancels, then flushes any remaining
// in-flight batches so the queued events still reach NATS during a
// graceful shutdown. Close(stopped) signals completion.
func (p *Publisher) Run(ctx context.Context) {
	defer close(p.stopped)
	for {
		select {
		case events := <-p.queue:
			p.recordQueueDelta(ctx, -1)
			p.deliver(events)
		case <-ctx.Done():
			p.drain()
			return
		}
	}
}

// Stopped returns a channel closed when Run has exited.
func (p *Publisher) Stopped() <-chan struct{} { return p.stopped }

// deliver forwards one batch with a fresh context. The caller's ctx is
// long gone by the time we publish, so using it would race on cancel
// and drop events that are already past the synchronous response.
func (p *Publisher) deliver(events []es.Event) {
	if err := p.inner.Publish(context.Background(), events); err != nil {
		p.recordPublishError(context.Background(), len(events))
		// Inner publisher already logs; PG remains the source of
		// truth so natsstore.Backfill recovers on next start.
		if !errors.Is(err, context.Canceled) {
			p.log.Warn("async publish failed; relying on PG backfill",
				"event_count", len(events), "error", err)
		}
	}
}

// drain pulls any batches still in the queue at shutdown and forwards
// them synchronously so they aren't dropped on the floor.
func (p *Publisher) drain() {
	for {
		select {
		case events := <-p.queue:
			p.recordQueueDelta(context.Background(), -1)
			p.deliver(events)
		default:
			return
		}
	}
}

// ---- metrics helpers ----

func (p *Publisher) recordQueueDelta(ctx context.Context, delta int64) {
	if asyncQueueDepth == nil {
		return
	}
	asyncQueueDepth.Add(ctx, delta)
}

func (p *Publisher) recordPublishError(ctx context.Context, n int) {
	if asyncPublishErrors == nil {
		return
	}
	asyncPublishErrors.Add(ctx, int64(n))
}

// Instruments are built once against metrics.Meter (no-op until
// metrics.Init runs).
var (
	asyncQueueDepth    metric.Int64UpDownCounter
	asyncPublishErrors metric.Int64Counter
)

func init() {
	var err error
	asyncQueueDepth, err = metrics.Meter.Int64UpDownCounter(
		"xray.asyncpublisher.queue_depth",
		metric.WithDescription("Number of event batches queued for async publish."),
	)
	if err != nil {
		asyncQueueDepth = nil
	}
	asyncPublishErrors, err = metrics.Meter.Int64Counter(
		"xray.asyncpublisher.publish_errors_total",
		metric.WithDescription("Events whose async publish failed (durable in PG, recovered via Backfill)."),
	)
	if err != nil {
		asyncPublishErrors = nil
	}
}
