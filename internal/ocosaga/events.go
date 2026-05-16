package ocosaga

import (
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const (
	EventOCOSagaStarted      = "OCOSagaStarted"
	EventOCOSagaSharesHeld   = "OCOSagaSharesHeld"
	EventOCOSagaExitPlaced   = "OCOSagaExitPlaced"
	EventOCOSagaFillRecorded = "OCOSagaFillRecorded"
	EventOCOSagaCompleted    = "OCOSagaCompleted"
	EventOCOSagaFailed       = "OCOSagaFailed"
	EventOCOSagaActionFailed = "OCOSagaActionFailed"
)

func RegisterEvents(r *es.Registry) {
	r.Register(EventOCOSagaStarted, func() proto.Message { return new(orderbookv1.OCOSagaStarted) })
	r.Register(EventOCOSagaSharesHeld, func() proto.Message { return new(orderbookv1.OCOSagaSharesHeld) })
	r.Register(EventOCOSagaExitPlaced, func() proto.Message { return new(orderbookv1.OCOSagaExitPlaced) })
	r.Register(EventOCOSagaFillRecorded, func() proto.Message { return new(orderbookv1.OCOSagaFillRecorded) })
	r.Register(EventOCOSagaCompleted, func() proto.Message { return new(orderbookv1.OCOSagaCompleted) })
	r.Register(EventOCOSagaFailed, func() proto.Message { return new(orderbookv1.OCOSagaFailed) })
	r.Register(EventOCOSagaActionFailed, func() proto.Message { return new(orderbookv1.OCOSagaActionFailed) })
}
