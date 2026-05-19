// Package corpaction owns the CorporateAction aggregate: a small
// state machine (Declared → Applied | Failed) plus the events that
// drive it. Actual position / order / saga adjustments happen on
// their own aggregates via their own events; this package only
// records that an action was declared, when it was applied, and
// roughly what it touched (counts on CorporateActionApplied).
package corpaction

import (
	"fmt"
	"time"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const AggregateType = "corp-action"

func AggregateID(actionID string) string {
	return AggregateType + ":" + actionID
}

type Status int

const (
	Uninitialized Status = iota
	Declared
	Applied
	Failed
)

func (s Status) Proto() corpactionv1.ActionStatus {
	switch s {
	case Declared:
		return corpactionv1.ActionStatus_ACTION_STATUS_DECLARED
	case Applied:
		return corpactionv1.ActionStatus_ACTION_STATUS_APPLIED
	case Failed:
		return corpactionv1.ActionStatus_ACTION_STATUS_FAILED
	default:
		return corpactionv1.ActionStatus_ACTION_STATUS_UNSPECIFIED
	}
}

// CorporateAction is the aggregate. Type-specific fields are
// non-zero only when Type matches; payload is checked at declare
// time so the aggregate trusts the values it loads from events.
type CorporateAction struct {
	es.AggregateBase

	ActionID         string
	Symbol           string
	Type             corpactionv1.ActionType
	Status           Status
	SplitNumerator   int32
	SplitDenominator int32
	DividendPerShare int64
	NewSymbol        string
	EffectiveDate    time.Time
	RecordDate       time.Time
	PayDate          time.Time
	DeclaredAt       time.Time
	AppliedAt        time.Time
	FailedReason     string
	// HoldersCount / OrdersCount / SagasCount are populated by the
	// applier from CorporateActionApplied — kept on the aggregate so
	// snapshot rebuilds and per-action Get RPCs don't need to walk
	// the event log.
	HoldersCount int32
	OrdersCount  int32
	SagasCount   int32
	// DividendSnapshotted tracks whether the record-date snapshot
	// has been taken yet. Reactor checks this before re-snapshotting
	// on each tick between record_date and pay_date.
	DividendSnapshotted bool
}

func NewCorporateAction(id string) *CorporateAction {
	a := &CorporateAction{}
	a.SetID(id)
	return a
}

func (a *CorporateAction) Apply(evt es.Event) error {
	switch data := evt.Data.(type) {
	case *corpactionv1.CorporateActionDeclared:
		a.applyDeclared(data)
	case *corpactionv1.CorporateActionApplied:
		a.applyApplied(data)
	case *corpactionv1.CorporateActionFailed:
		a.applyFailed(data)
	case *corpactionv1.DividendRecordSnapshotted:
		a.applyDividendSnapshotted(data)
	default:
		return fmt.Errorf("unknown event type: %T", evt.Data)
	}
	a.IncrementVersion()
	return nil
}

func (a *CorporateAction) applyDeclared(data *corpactionv1.CorporateActionDeclared) {
	a.ActionID = data.ActionId
	a.Symbol = data.Symbol
	a.Type = data.Type
	a.Status = Declared
	a.SplitNumerator = data.SplitNumerator
	a.SplitDenominator = data.SplitDenominator
	a.DividendPerShare = data.DividendPerShare
	a.NewSymbol = data.NewSymbol
	if data.EffectiveDate != nil {
		a.EffectiveDate = data.EffectiveDate.AsTime()
	}
	if data.RecordDate != nil {
		a.RecordDate = data.RecordDate.AsTime()
	}
	if data.PayDate != nil {
		a.PayDate = data.PayDate.AsTime()
	}
	if data.DeclaredAt != nil {
		a.DeclaredAt = data.DeclaredAt.AsTime()
	}
}

func (a *CorporateAction) applyApplied(data *corpactionv1.CorporateActionApplied) {
	a.Status = Applied
	if data.AppliedAt != nil {
		a.AppliedAt = data.AppliedAt.AsTime()
	}
	a.HoldersCount = data.HoldersCount
	a.OrdersCount = data.OrdersCount
	a.SagasCount = data.SagasCount
}

func (a *CorporateAction) applyFailed(data *corpactionv1.CorporateActionFailed) {
	a.Status = Failed
	a.FailedReason = data.Reason
}

func (a *CorporateAction) applyDividendSnapshotted(_ *corpactionv1.DividendRecordSnapshotted) {
	a.DividendSnapshotted = true
}
