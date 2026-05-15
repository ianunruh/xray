package bracket

import (
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const (
	EventSagaStarted      = "SagaStarted"
	EventEntryFilled      = "EntryFilled"
	EventExitFilled       = "ExitFilled"
	EventSagaCompleted    = "SagaCompleted"
	EventSagaFailed       = "SagaFailed"
	EventSagaActionFailed = "SagaActionFailed"
)

func RegisterEvents(r *es.Registry) {
	r.Register(EventSagaStarted, func() proto.Message { return new(orderbookv1.SagaStarted) })
	r.Register(EventEntryFilled, func() proto.Message { return new(orderbookv1.EntryFilled) })
	r.Register(EventExitFilled, func() proto.Message { return new(orderbookv1.ExitFilled) })
	r.Register(EventSagaCompleted, func() proto.Message { return new(orderbookv1.SagaCompleted) })
	r.Register(EventSagaFailed, func() proto.Message { return new(orderbookv1.SagaFailed) })
	r.Register(EventSagaActionFailed, func() proto.Message { return new(orderbookv1.SagaActionFailed) })
}
