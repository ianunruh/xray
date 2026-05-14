package natsstore

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ianunruh/xray/pkg/es"
)

type Publisher struct {
	js  jetstream.JetStream
	log *slog.Logger
}

func NewPublisher(js jetstream.JetStream, log *slog.Logger) *Publisher {
	return &Publisher{js: js, log: log}
}

func (p *Publisher) Publish(ctx context.Context, events []es.Event) error {
	for _, evt := range events {
		data, err := proto.Marshal(evt.Data)
		if err != nil {
			return fmt.Errorf("marshal event %s: %w", evt.Type, err)
		}

		msg := &nats.Msg{
			Subject: Subject(evt.AggregateID, evt.Type),
			Data:    data,
			Header: nats.Header{
				"Nats-Msg-Id":      {evt.AggregateID + ":" + strconv.Itoa(evt.Version)},
				"Xray-Aggregate-Id": {evt.AggregateID},
				"Xray-Event-Type":   {evt.Type},
				"Xray-Version":      {strconv.Itoa(evt.Version)},
				"Xray-Timestamp":    {evt.Timestamp.Format(time.RFC3339Nano)},
			},
		}

		if _, err := p.js.PublishMsg(ctx, msg); err != nil {
			p.log.Error("failed to publish event to NATS", "type", evt.Type, "aggregate_id", evt.AggregateID, "error", err)
			return fmt.Errorf("publish event to NATS: %w", err)
		}
	}
	return nil
}
