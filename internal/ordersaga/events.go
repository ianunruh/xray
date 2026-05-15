package ordersaga

import (
	"google.golang.org/protobuf/proto"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

func RegisterEvents(r *es.Registry) {
	r.Register("OrderSagaStarted", func() proto.Message { return new(portfoliov1.OrderSagaStarted) })
	r.Register("OrderSagaCashHeld", func() proto.Message { return new(portfoliov1.OrderSagaCashHeld) })
	r.Register("OrderSagaOrderPlaced", func() proto.Message { return new(portfoliov1.OrderSagaOrderPlaced) })
	r.Register("OrderSagaFillRecorded", func() proto.Message { return new(portfoliov1.OrderSagaFillRecorded) })
	r.Register("OrderSagaCompleted", func() proto.Message { return new(portfoliov1.OrderSagaCompleted) })
	r.Register("OrderSagaFailed", func() proto.Message { return new(portfoliov1.OrderSagaFailed) })
	r.Register("OrderSagaActionFailed", func() proto.Message { return new(portfoliov1.OrderSagaActionFailed) })
}
