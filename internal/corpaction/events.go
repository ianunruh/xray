package corpaction

import (
	"google.golang.org/protobuf/proto"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const (
	EventCorporateActionDeclared    = "CorporateActionDeclared"
	EventCorporateActionApplied     = "CorporateActionApplied"
	EventCorporateActionFailed      = "CorporateActionFailed"
	EventDividendRecordSnapshotted  = "DividendRecordSnapshotted"
)

func RegisterEvents(r *es.Registry) {
	r.Register(EventCorporateActionDeclared, func() proto.Message { return new(corpactionv1.CorporateActionDeclared) })
	r.Register(EventCorporateActionApplied, func() proto.Message { return new(corpactionv1.CorporateActionApplied) })
	r.Register(EventCorporateActionFailed, func() proto.Message { return new(corpactionv1.CorporateActionFailed) })
	r.Register(EventDividendRecordSnapshotted, func() proto.Message { return new(corpactionv1.DividendRecordSnapshotted) })
}
