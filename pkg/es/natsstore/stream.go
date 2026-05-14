package natsstore

import (
	"context"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamName   = "EVENTS"
	ConsumerName = "projections"
)

func EnsureStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{"events.>"},
		Retention:  jetstream.LimitsPolicy,
		Storage:    jetstream.FileStorage,
		Duplicates: 2 * time.Minute,
	})
}

func Subject(aggregateID, eventType string) string {
	return "events." + strings.ReplaceAll(aggregateID, ":", ".") + "." + eventType
}
