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
	defaultBatchSize = 256
	fetchTimeout     = 100 * time.Millisecond
)

type ProjectionConsumer struct {
	js          jetstream.JetStream
	registry    *es.Registry
	projections []es.Projection
	log         *slog.Logger
	batchSize   int
}

func NewProjectionConsumer(js jetstream.JetStream, registry *es.Registry, log *slog.Logger, projections ...es.Projection) *ProjectionConsumer {
	return &ProjectionConsumer{
		js:          js,
		registry:    registry,
		projections: projections,
		log:         log,
		batchSize:   defaultBatchSize,
	}
}

func (c *ProjectionConsumer) Start(ctx context.Context) error {
	consumer, err := c.ensureConsumer(ctx)
	if err != nil {
		return fmt.Errorf("ensure consumer: %w", err)
	}

	if err := c.catchUp(ctx, consumer); err != nil {
		return fmt.Errorf("projection catch-up: %w", err)
	}

	go c.run(ctx, consumer)
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

func (c *ProjectionConsumer) catchUp(ctx context.Context, consumer jetstream.Consumer) error {
	total := 0
	for {
		n, err := c.fetchAndDispatch(ctx, consumer)
		if err != nil {
			return err
		}
		total += n
		if n < c.batchSize {
			break
		}
	}
	c.log.Info("projections caught up from NATS", "event_count", total)
	return nil
}

func (c *ProjectionConsumer) run(ctx context.Context, consumer jetstream.Consumer) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if _, err := c.fetchAndDispatch(ctx, consumer); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Error("projection fetch failed", "error", err)
			time.Sleep(time.Second)
		}
	}
}

func (c *ProjectionConsumer) fetchAndDispatch(ctx context.Context, consumer jetstream.Consumer) (int, error) {
	msgs, err := consumer.Fetch(c.batchSize, jetstream.FetchMaxWait(fetchTimeout))
	if err != nil {
		return 0, fmt.Errorf("fetch messages: %w", err)
	}

	var events []es.Event
	var fetched []jetstream.Msg

	for msg := range msgs.Messages() {
		evt, err := c.deserialize(msg)
		if err != nil {
			c.log.Error("failed to deserialize NATS message", "subject", msg.Subject(), "error", err)
			msg.Nak()
			continue
		}
		events = append(events, evt)
		fetched = append(fetched, msg)
	}

	if err := msgs.Error(); err != nil {
		return 0, fmt.Errorf("fetch error: %w", err)
	}

	if len(events) == 0 {
		return 0, nil
	}

	for _, proj := range c.projections {
		if err := proj.HandleEvents(ctx, events); err != nil {
			c.log.Error("projection failed to handle events", "error", err)
		}
	}

	for _, msg := range fetched {
		msg.Ack()
	}

	return len(events), nil
}

func (c *ProjectionConsumer) deserialize(msg jetstream.Msg) (es.Event, error) {
	headers := msg.Headers()
	eventType := headers.Get("Xray-Event-Type")
	aggregateID := headers.Get("Xray-Aggregate-Id")
	versionStr := headers.Get("Xray-Version")
	timestampStr := headers.Get("Xray-Timestamp")

	version, _ := strconv.Atoi(versionStr)

	ts, _ := time.Parse(time.RFC3339Nano, timestampStr)

	raw := es.RawEvent{
		AggregateID: aggregateID,
		Type:        eventType,
		Version:     version,
		Timestamp:   ts,
		Data:        msg.Data(),
	}

	return c.registry.Deserialize(raw)
}
