// Package metrics holds the OpenTelemetry instruments xray uses on its
// hot paths. All instruments are created once at process start (via Init)
// and accessed through package-level vars so call sites stay allocation-
// free. The /metrics endpoint is served by the Prometheus exporter wired
// up alongside the meter provider.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Meter is the OTel meter used by every instrument in this package.
// Defaults to a no-op so packages that import metrics without calling
// Init (tests, CLIs) don't panic.
var Meter metric.Meter = noop.NewMeterProvider().Meter("xray")

// Instruments. Package-level so call sites don't need a reference; nil
// until Init is called, in which case Record* helpers no-op.
var (
	CommandLockWaitSeconds   metric.Float64Histogram
	CommandHandleSeconds     metric.Float64Histogram
	CommandRetriesTotal      metric.Int64Counter
	AggregateCacheHitsTotal  metric.Int64Counter
	AggregateCacheMissTotal  metric.Int64Counter

	StoreAppendSeconds       metric.Float64Histogram
	StoreAppendEvents        metric.Int64Histogram
	StoreAppendConflictTotal metric.Int64Counter
	StoreAppendErrorsTotal   metric.Int64Counter

	PublisherPublishSeconds metric.Float64Histogram
	PublisherErrorsTotal    metric.Int64Counter

	AsyncPublisherQueueDepth   metric.Int64UpDownCounter
	AsyncPublisherErrorsTotal  metric.Int64Counter

	GroupCommitBatchSize       metric.Int64Histogram
	GroupCommitFlushSeconds    metric.Float64Histogram
	GroupCommitFallbacksTotal  metric.Int64Counter

	RPCDurationSeconds metric.Float64Histogram
	RPCErrorsTotal     metric.Int64Counter
)

// Init wires up the Prometheus exporter, builds the meter provider, and
// constructs every instrument. Returns an http.Handler for /metrics.
// Safe to call exactly once; subsequent calls re-initialize.
func Init() (http.Handler, error) {
	reg := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("create otel prometheus exporter: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	Meter = provider.Meter("github.com/ianunruh/xray")

	if err := buildInstruments(); err != nil {
		return nil, err
	}

	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}), nil
}

func buildInstruments() error {
	var err error

	if CommandLockWaitSeconds, err = Meter.Float64Histogram(
		"xray.command.lock_wait_seconds",
		metric.WithDescription("Time waiting on per-aggregate lock before Handle proceeds."),
		metric.WithUnit("s"),
	); err != nil {
		return err
	}
	if CommandHandleSeconds, err = Meter.Float64Histogram(
		"xray.command.handle_seconds",
		metric.WithDescription("End-to-end Handler.Handle latency (load + execute + append + publish)."),
		metric.WithUnit("s"),
	); err != nil {
		return err
	}
	if CommandRetriesTotal, err = Meter.Int64Counter(
		"xray.command.retries_total",
		metric.WithDescription("Optimistic-concurrency retries inside Handler.Handle."),
	); err != nil {
		return err
	}
	if AggregateCacheHitsTotal, err = Meter.Int64Counter(
		"xray.aggregate.cache_hits_total",
		metric.WithDescription("Per-aggregate cache hits, avoiding a reload from the event store."),
	); err != nil {
		return err
	}
	if AggregateCacheMissTotal, err = Meter.Int64Counter(
		"xray.aggregate.cache_misses_total",
		metric.WithDescription("Per-aggregate cache misses, triggering a reload from the event store."),
	); err != nil {
		return err
	}

	if StoreAppendSeconds, err = Meter.Float64Histogram(
		"xray.store.append_seconds",
		metric.WithDescription("Postgres event-store batch Append latency."),
		metric.WithUnit("s"),
	); err != nil {
		return err
	}
	if StoreAppendEvents, err = Meter.Int64Histogram(
		"xray.store.append_events",
		metric.WithDescription("Number of events per Append call."),
	); err != nil {
		return err
	}
	if StoreAppendConflictTotal, err = Meter.Int64Counter(
		"xray.store.append_conflicts_total",
		metric.WithDescription("Optimistic-concurrency conflicts seen by the store."),
	); err != nil {
		return err
	}
	if StoreAppendErrorsTotal, err = Meter.Int64Counter(
		"xray.store.append_errors_total",
		metric.WithDescription("Non-conflict Append errors."),
	); err != nil {
		return err
	}

	if PublisherPublishSeconds, err = Meter.Float64Histogram(
		"xray.publisher.publish_seconds",
		metric.WithDescription("Time to publish a batch of events to NATS JetStream."),
		metric.WithUnit("s"),
	); err != nil {
		return err
	}
	if PublisherErrorsTotal, err = Meter.Int64Counter(
		"xray.publisher.errors_total",
		metric.WithDescription("Failed NATS publish attempts."),
	); err != nil {
		return err
	}

	if AsyncPublisherQueueDepth, err = Meter.Int64UpDownCounter(
		"xray.asyncpublisher.queue_depth",
		metric.WithDescription("Number of event batches queued for async publish."),
	); err != nil {
		return err
	}
	if AsyncPublisherErrorsTotal, err = Meter.Int64Counter(
		"xray.asyncpublisher.publish_errors_total",
		metric.WithDescription("Events whose async publish failed (durable in PG, recovered via Backfill)."),
	); err != nil {
		return err
	}

	if GroupCommitBatchSize, err = Meter.Int64Histogram(
		"xray.groupcommit.batch_size",
		metric.WithDescription("Number of append requests packed into one group-commit flush."),
	); err != nil {
		return err
	}
	if GroupCommitFlushSeconds, err = Meter.Float64Histogram(
		"xray.groupcommit.flush_seconds",
		metric.WithDescription("End-to-end group-commit flush latency, labeled batched|fallback."),
		metric.WithUnit("s"),
	); err != nil {
		return err
	}
	if GroupCommitFallbacksTotal, err = Meter.Int64Counter(
		"xray.groupcommit.fallbacks_total",
		metric.WithDescription("Requests that fell back to individual append after a batched commit failed."),
	); err != nil {
		return err
	}

	if RPCDurationSeconds, err = Meter.Float64Histogram(
		"xray.rpc.duration_seconds",
		metric.WithDescription("End-to-end Connect RPC handler latency."),
		metric.WithUnit("s"),
	); err != nil {
		return err
	}
	if RPCErrorsTotal, err = Meter.Int64Counter(
		"xray.rpc.errors_total",
		metric.WithDescription("RPC handlers that returned an error."),
	); err != nil {
		return err
	}

	return nil
}

// AggregateType returns the type prefix from an aggregate ID of the form
// "type:rest" (e.g. "orderbook:AAPL" -> "orderbook"). Used as a low-
// cardinality label so per-symbol IDs don't blow up time-series count.
// Falls back to "unknown" if the ID has no colon.
func AggregateType(id string) string {
	if i := strings.IndexByte(id, ':'); i > 0 {
		return id[:i]
	}
	return "unknown"
}

// Ctx is a typed alias to keep call sites tidy when recording.
type Ctx = context.Context
