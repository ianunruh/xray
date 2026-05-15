package ordersaga

import (
	"google.golang.org/protobuf/proto"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const (
	EventOrderSagaStarted      = "OrderSagaStarted"
	EventOrderSagaCashHeld     = "OrderSagaCashHeld"
	EventOrderSagaOrderPlaced  = "OrderSagaOrderPlaced"
	EventOrderSagaFillRecorded = "OrderSagaFillRecorded"
	EventOrderSagaCompleted    = "OrderSagaCompleted"
	EventOrderSagaFailed       = "OrderSagaFailed"
	EventOrderSagaActionFailed = "OrderSagaActionFailed"
)

func RegisterEvents(r *es.Registry) {
	r.Register(EventOrderSagaStarted, func() proto.Message { return new(portfoliov1.OrderSagaStarted) })
	r.Register(EventOrderSagaCashHeld, func() proto.Message { return new(portfoliov1.OrderSagaCashHeld) })
	r.Register(EventOrderSagaOrderPlaced, func() proto.Message { return new(portfoliov1.OrderSagaOrderPlaced) })
	r.Register(EventOrderSagaFillRecorded, func() proto.Message { return new(portfoliov1.OrderSagaFillRecorded) })
	r.Register(EventOrderSagaCompleted, func() proto.Message { return new(portfoliov1.OrderSagaCompleted) })
	r.Register(EventOrderSagaFailed, func() proto.Message { return new(portfoliov1.OrderSagaFailed) })
	r.Register(EventOrderSagaActionFailed, func() proto.Message { return new(portfoliov1.OrderSagaActionFailed) })
}
