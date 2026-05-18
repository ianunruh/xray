package natsstore

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ianunruh/xray/pkg/es"
)

const (
	defaultBatchSize = 256
	fetchTimeout     = 100 * time.Millisecond
)

// ProgressHook is invoked after each successful batch dispatch with the
// number of events processed and the new checkpoint sequence. Used by the
// ProjectionManager to stream rebuild progress; nil disables it.
type ProgressHook func(batchSize int, newCheckpoint uint64)

type ProjectionConsumer struct {
	js        jetstream.JetStream
	registry  *es.Registry
	log       *slog.Logger
	name      string
	batchSize int

	ephemeral  []es.Projection
	persistent []es.Projection
	checkpoint es.CheckpointStore

	// reactor is true when this consumer hosts at least one component
	// that issues commands in response to events (a saga reactor,
	// margin-call reactor, etc.). Rebuilding such a consumer would
	// double-execute those commands, so the ProjectionManager hides
	// reactor consumers from the rebuild UI.
	reactor bool

	// hookMu guards progressHook so the manager can install and remove
	// it during a rebuild without racing the dispatch goroutine.
	hookMu       sync.RWMutex
	progressHook ProgressHook

	// runMu serializes Start/Stop so callers can drive the lifecycle
	// without coordinating themselves.
	runMu  sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewProjectionConsumer creates a JetStream consumer named `name`. The same
// name is used for the durable consumer on NATS and for the checkpoint row
// in the checkpoint store, so two instances with different names hold
// independent cursors and can be advanced independently.
func NewProjectionConsumer(js jetstream.JetStream, registry *es.Registry, log *slog.Logger, name string) *ProjectionConsumer {
	return &ProjectionConsumer{
		js:        js,
		registry:  registry,
		log:       log,
		name:      name,
		batchSize: defaultBatchSize,
	}
}

func (c *ProjectionConsumer) WithEphemeral(projections ...es.Projection) *ProjectionConsumer {
	c.ephemeral = append(c.ephemeral, projections...)
	return c
}

func (c *ProjectionConsumer) WithPersistent(checkpoint es.CheckpointStore, projections ...es.Projection) *ProjectionConsumer {
	c.checkpoint = checkpoint
	c.persistent = append(c.persistent, projections...)
	return c
}

// WithReactor marks this consumer as hosting a reactor (or any component
// that issues commands). Reactor consumers cannot be rebuilt — replaying
// past events through them would re-execute their command side effects.
func (c *ProjectionConsumer) WithReactor() *ProjectionConsumer {
	c.reactor = true
	return c
}

// Name returns the consumer's durable / checkpoint name.
func (c *ProjectionConsumer) Name() string { return c.name }

// IsReactor reports whether the consumer was marked as hosting a reactor.
func (c *ProjectionConsumer) IsReactor() bool { return c.reactor }

// Projections returns the consumer's full projection set (persistent + ephemeral)
// in the order they receive events. The manager uses this to call Reset on
// resettable projections during a rebuild.
func (c *ProjectionConsumer) Projections() []es.Projection {
	out := make([]es.Projection, 0, len(c.persistent)+len(c.ephemeral))
	out = append(out, c.persistent...)
	out = append(out, c.ephemeral...)
	return out
}

// CheckpointStore returns the checkpoint store, or nil for ephemeral
// consumers.
func (c *ProjectionConsumer) CheckpointStore() es.CheckpointStore { return c.checkpoint }

// SetProgressHook installs (or clears) a callback fired after each successful
// batch dispatch. The hook receives the batch size and the new max sequence
// processed by this consumer. Safe to call while the consumer is running.
func (c *ProjectionConsumer) SetProgressHook(h ProgressHook) {
	c.hookMu.Lock()
	defer c.hookMu.Unlock()
	c.progressHook = h
}

func (c *ProjectionConsumer) fireProgress(batchSize int, newCheckpoint uint64) {
	c.hookMu.RLock()
	hook := c.progressHook
	c.hookMu.RUnlock()
	if hook != nil {
		hook(batchSize, newCheckpoint)
	}
}

func (c *ProjectionConsumer) Start(ctx context.Context) error {
	c.runMu.Lock()
	defer c.runMu.Unlock()

	if c.cancel != nil {
		return fmt.Errorf("consumer %s already started", c.name)
	}

	consumer, err := c.ensureConsumer(ctx)
	if err != nil {
		return fmt.Errorf("ensure consumer: %w", err)
	}

	var checkpointSeq uint64
	if c.checkpoint != nil {
		checkpointSeq, err = c.checkpoint.LoadCheckpoint(ctx, c.name)
		if err != nil {
			return fmt.Errorf("load checkpoint: %w", err)
		}
	}

	checkpointSeq, err = c.catchUp(ctx, consumer, checkpointSeq)
	if err != nil {
		return fmt.Errorf("projection catch-up: %w", err)
	}

	// Derive the run context from the caller's so the consumer still
	// stops when the parent ctx is cancelled, but Stop() can cancel it
	// independently for in-place rebuilds.
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.cancel = cancel
	c.done = done

	go func() {
		defer close(done)
		c.run(runCtx, consumer, checkpointSeq)
	}()
	return nil
}

// Stop cancels the run goroutine and waits for it to exit. Safe to call
// multiple times. After Stop returns, the consumer may be Start-ed again
// (the manager does this during a rebuild).
func (c *ProjectionConsumer) Stop() {
	c.runMu.Lock()
	cancel := c.cancel
	done := c.done
	c.cancel = nil
	c.done = nil
	c.runMu.Unlock()

	if cancel == nil {
		return
	}
	cancel()
	<-done
}

func (c *ProjectionConsumer) ensureConsumer(ctx context.Context) (jetstream.Consumer, error) {
	// Persistent consumers (with a checkpoint store) keep their JetStream
	// cursor across boots so they can resume rather than replay.
	// Consumers without a checkpoint store rebuild in-memory state on
	// every boot, so we reset the cursor to deliver from the beginning.
	if c.checkpoint == nil {
		_ = c.js.DeleteConsumer(ctx, StreamName, c.name)
	}
	return c.js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable:       c.name,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "events.>",
	})
}

func (c *ProjectionConsumer) catchUp(ctx context.Context, consumer jetstream.Consumer, checkpointSeq uint64) (uint64, error) {
	total := 0
	for {
		n, newSeq, err := c.fetchAndDispatch(ctx, consumer, checkpointSeq)
		if err != nil {
			return checkpointSeq, err
		}
		if newSeq > checkpointSeq {
			checkpointSeq = newSeq
		}
		total += n
		if n < c.batchSize {
			break
		}
	}
	c.log.Info("projections caught up from NATS", "name", c.name, "event_count", total, "checkpoint", checkpointSeq)
	return checkpointSeq, nil
}

func (c *ProjectionConsumer) run(ctx context.Context, consumer jetstream.Consumer, checkpointSeq uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, newSeq, err := c.fetchAndDispatch(ctx, consumer, checkpointSeq)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Error("projection fetch failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		if newSeq > checkpointSeq {
			checkpointSeq = newSeq
		}
	}
}

func (c *ProjectionConsumer) fetchAndDispatch(ctx context.Context, consumer jetstream.Consumer, checkpointSeq uint64) (int, uint64, error) {
	msgs, err := consumer.Fetch(c.batchSize, jetstream.FetchMaxWait(fetchTimeout))
	if err != nil {
		return 0, 0, fmt.Errorf("fetch messages: %w", err)
	}

	var events []es.Event
	var fetched []jetstream.Msg
	var maxSeq uint64

	for msg := range msgs.Messages() {
		evt, seq, err := c.deserialize(msg)
		if err != nil {
			c.log.Error("failed to deserialize NATS message", "subject", msg.Subject(), "error", err)
			msg.Nak()
			continue
		}
		events = append(events, evt)
		fetched = append(fetched, msg)
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	if err := msgs.Error(); err != nil {
		return 0, 0, fmt.Errorf("fetch error: %w", err)
	}

	if len(events) == 0 {
		return 0, 0, nil
	}

	// Persistent projections run first so that subscribers woken up by
	// ephemeral brokers/reactors see fully-updated state when they re-query.
	// Persistent projections only receive events past the checkpoint.
	if len(c.persistent) > 0 {
		var newEvents []es.Event
		for _, evt := range events {
			if uint64(evt.Position) > checkpointSeq {
				newEvents = append(newEvents, evt)
			}
		}

		if len(newEvents) > 0 {
			for _, proj := range c.persistent {
				if err := proj.HandleEvents(ctx, newEvents); err != nil {
					c.log.Error("projection failed to handle events", "error", err)
				}
			}
		}
	}

	// Ephemeral projections always receive all events.
	for _, proj := range c.ephemeral {
		if err := proj.HandleEvents(ctx, events); err != nil {
			c.log.Error("projection failed to handle events", "error", err)
		}
	}

	// Save checkpoint after successful dispatch.
	if c.checkpoint != nil && maxSeq > checkpointSeq {
		if err := c.checkpoint.SaveCheckpoint(ctx, c.name, maxSeq); err != nil {
			c.log.Error("failed to save checkpoint", "error", err)
		}
	}

	for _, msg := range fetched {
		msg.Ack()
	}

	c.fireProgress(len(events), maxSeq)

	return len(events), maxSeq, nil
}

func (c *ProjectionConsumer) deserialize(msg jetstream.Msg) (es.Event, uint64, error) {
	headers := msg.Headers()
	eventType := headers.Get("Xray-Event-Type")
	aggregateID := headers.Get("Xray-Aggregate-Id")
	eventID := headers.Get("Xray-Event-Id")
	causationID := headers.Get("Xray-Causation-Id")
	correlationID := headers.Get("Xray-Correlation-Id")
	versionStr := headers.Get("Xray-Version")
	timestampStr := headers.Get("Xray-Timestamp")

	version, _ := strconv.Atoi(versionStr)
	ts, _ := time.Parse(time.RFC3339Nano, timestampStr)

	meta, err := msg.Metadata()
	if err != nil {
		return es.Event{}, 0, fmt.Errorf("message metadata: %w", err)
	}

	raw := es.RawEvent{
		ID:            eventID,
		CausationID:   causationID,
		CorrelationID: correlationID,
		AggregateID:   aggregateID,
		Type:          eventType,
		Version:       version,
		Position:      int64(meta.Sequence.Stream),
		Timestamp:     ts,
		Data:          msg.Data(),
	}

	evt, err := c.registry.Deserialize(raw)
	return evt, meta.Sequence.Stream, err
}
