package natsstore

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ianunruh/xray/pkg/es"
)

const backfillBatch = 256

func Backfill(ctx context.Context, poller es.GlobalEventPoller, js jetstream.JetStream, log *slog.Logger) error {
	stream, err := js.Stream(ctx, StreamName)
	if err != nil {
		return fmt.Errorf("get stream info: %w", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("get stream info: %w", err)
	}

	if info.State.Msgs > 0 {
		log.Info("NATS stream already has messages, skipping backfill", "messages", info.State.Msgs)
		return nil
	}

	log.Info("backfilling NATS stream from Postgres")

	var position int64
	var total int

	for {
		rawEvents, err := poller.LoadAfter(ctx, position, backfillBatch)
		if err != nil {
			return fmt.Errorf("load events after position %d: %w", position, err)
		}

		if len(rawEvents) == 0 {
			break
		}

		for _, raw := range rawEvents {
			msg := &nats.Msg{
				Subject: Subject(raw.AggregateID, raw.Type),
				Data:    raw.Data,
				Header: nats.Header{
					"Nats-Msg-Id":       {raw.AggregateID + ":" + strconv.Itoa(raw.Version)},
					"Xray-Aggregate-Id": {raw.AggregateID},
					"Xray-Event-Type":   {raw.Type},
					"Xray-Version":      {strconv.Itoa(raw.Version)},
					"Xray-Timestamp":    {raw.Timestamp.Format(time.RFC3339Nano)},
				},
			}

			if _, err := js.PublishMsg(ctx, msg); err != nil {
				return fmt.Errorf("publish backfill event: %w", err)
			}
		}

		position = rawEvents[len(rawEvents)-1].Position
		total += len(rawEvents)

		if len(rawEvents) < backfillBatch {
			break
		}
	}

	log.Info("backfill complete", "events", total)
	return nil
}
