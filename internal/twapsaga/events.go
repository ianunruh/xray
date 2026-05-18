package twapsaga

import (
	"google.golang.org/protobuf/proto"

	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const (
	EventTWAPSagaStarted      = "TWAPSagaStarted"
	EventTWAPSliceLaunched    = "TWAPSliceLaunched"
	EventTWAPSliceCompleted   = "TWAPSliceCompleted"
	EventTWAPSagaCompleted    = "TWAPSagaCompleted"
	EventTWAPSagaFailed       = "TWAPSagaFailed"
	EventTWAPSagaActionFailed = "TWAPSagaActionFailed"
)

func RegisterEvents(r *es.Registry) {
	r.Register(EventTWAPSagaStarted, func() proto.Message { return new(sagav1.TWAPSagaStarted) })
	r.Register(EventTWAPSliceLaunched, func() proto.Message { return new(sagav1.TWAPSliceLaunched) })
	r.Register(EventTWAPSliceCompleted, func() proto.Message { return new(sagav1.TWAPSliceCompleted) })
	r.Register(EventTWAPSagaCompleted, func() proto.Message { return new(sagav1.TWAPSagaCompleted) })
	r.Register(EventTWAPSagaFailed, func() proto.Message { return new(sagav1.TWAPSagaFailed) })
	r.Register(EventTWAPSagaActionFailed, func() proto.Message { return new(sagav1.TWAPSagaActionFailed) })
}
