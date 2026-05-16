package natsstore

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ianunruh/xray/pkg/es"
)

const (
	defaultBatchSize      = 256
	defaultCheckpointName = "nats-consumer"
	fetchTimeout          = 100 * time.Millisecond
)

type ProjectionConsumer struct {
	js        jetstream.JetStream
	registry  *es.Registry
	log       *slog.Logger
	batchSize int

	ephemeral  []es.Projection
	persistent []es.Projection
	checkpoint es.CheckpointStore
}

func NewProjectionConsumer(js jetstream.JetStream, registry *es.Registry, log *slog.Logger) *ProjectionConsumer {
	return &ProjectionConsumer{
		js:        js,
		registry:  registry,
		log:       log,
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

func (c *ProjectionConsumer) Start(ctx context.Context) error {
	consumer, err := c.ensureConsumer(ctx)
	if err != nil {
		return fmt.Errorf("ensure consumer: %w", err)
	}

	var checkpointSeq uint64
	if c.checkpoint != nil {
		checkpointSeq, err = c.checkpoint.LoadCheckpoint(ctx, defaultCheckpointName)
		if err != nil {
			return fmt.Errorf("load checkpoint: %w", err)
		}
	}

	checkpointSeq, err = c.catchUp(ctx, consumer, checkpointSeq)
	if err != nil {
		return fmt.Errorf("projection catch-up: %w", err)
	}

	go c.run(ctx, consumer, checkpointSeq)
	return nil
}

func (c *ProjectionConsumer) ensureConsumer(ctx context.Context) (jetstream.Consumer, error) {
	_ = c.js.DeleteConsumer(ctx, StreamName, ConsumerName)

	return c.js.CreateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable:       ConsumerName,
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
	c.log.Info("projections caught up from NATS", "event_count", total, "checkpoint", checkpointSeq)
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
		if err := c.checkpoint.SaveCheckpoint(ctx, defaultCheckpointName, maxSeq); err != nil {
			c.log.Error("failed to save checkpoint", "error", err)
		}
	}

	for _, msg := range fetched {
		msg.Ack()
	}

	return len(events), maxSeq, nil
}

func (c *ProjectionConsumer) deserialize(msg jetstream.Msg) (es.Event, uint64, error) {
	headers := msg.Headers()
	eventType := headers.Get("Xray-Event-Type")
	aggregateID := headers.Get("Xray-Aggregate-Id")
	versionStr := headers.Get("Xray-Version")
	timestampStr := headers.Get("Xray-Timestamp")

	version, _ := strconv.Atoi(versionStr)
	ts, _ := time.Parse(time.RFC3339Nano, timestampStr)

	meta, err := msg.Metadata()
	if err != nil {
		return es.Event{}, 0, fmt.Errorf("message metadata: %w", err)
	}

	raw := es.RawEvent{
		AggregateID: aggregateID,
		Type:        eventType,
		Version:     version,
		Position:    int64(meta.Sequence.Stream),
		Timestamp:   ts,
		Data:        msg.Data(),
	}

	evt, err := c.registry.Deserialize(raw)
	return evt, meta.Sequence.Stream, err
}
