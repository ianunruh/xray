package bracket

import (
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

func RegisterEvents(r *es.Registry) {
	r.Register("SagaStarted", func() proto.Message { return new(orderbookv1.SagaStarted) })
	r.Register("EntryFilled", func() proto.Message { return new(orderbookv1.EntryFilled) })
	r.Register("ExitFilled", func() proto.Message { return new(orderbookv1.ExitFilled) })
	r.Register("SagaCompleted", func() proto.Message { return new(orderbookv1.SagaCompleted) })
	r.Register("SagaFailed", func() proto.Message { return new(orderbookv1.SagaFailed) })
	r.Register("SagaActionFailed", func() proto.Message { return new(orderbookv1.SagaActionFailed) })
}
