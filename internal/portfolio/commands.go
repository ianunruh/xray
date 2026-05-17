package portfolio

import (
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

var (
	ErrInvalidAmount        = errors.New("amount must be positive")
	ErrInsufficientFunds    = errors.New("insufficient funds")
	ErrInsufficientShares   = errors.New("insufficient shares")
	ErrInsufficientShortQty = errors.New("cover quantity exceeds open short")
	ErrShortHoldsLong       = errors.New("cannot open short while holding long in symbol")
	ErrLongHoldsShort       = errors.New("cannot buy long while holding short in symbol")
)

type DepositCash struct {
	AccountID string
	Amount    int64
}

func (c DepositCash) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteDepositCash(p *Portfolio, cmd DepositCash) ([]es.Event, error) {
	if cmd.Amount <= 0 {
		return nil, ErrInvalidAmount
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventCashDeposited,
		Timestamp:   now,
		Data: &portfoliov1.CashDeposited{
			AccountId:   cmd.AccountID,
			Amount:      cmd.Amount,
			DepositedAt: timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type WithdrawCash struct {
	AccountID string
	Amount    int64
}

func (c WithdrawCash) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteWithdrawCash(p *Portfolio, cmd WithdrawCash) ([]es.Event, error) {
	if cmd.Amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if p.CashBalance < cmd.Amount {
		return nil, ErrInsufficientFunds
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventCashWithdrawn,
		Timestamp:   now,
		Data: &portfoliov1.CashWithdrawn{
			AccountId:   cmd.AccountID,
			Amount:      cmd.Amount,
			WithdrawnAt: timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type HoldCash struct {
	AccountID   string
	OrderSagaID string
	Symbol      string
	Amount      int64
}

func (c HoldCash) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteHoldCash(p *Portfolio, cmd HoldCash) ([]es.Event, error) {
	if cmd.Amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if p.HoldsBySaga[cmd.OrderSagaID] != 0 {
		return nil, nil
	}
	// No flip in one saga: refuse to open or add to a long position
	// while a short is open in the same symbol.
	if s, ok := p.ShortPositions[cmd.Symbol]; ok && s.Quantity > 0 {
		return nil, ErrLongHoldsShort
	}
	if p.CashBalance < cmd.Amount {
		return nil, ErrInsufficientFunds
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventCashHeld,
		Timestamp:   now,
		Data: &portfoliov1.CashHeld{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Symbol:      cmd.Symbol,
			Amount:      cmd.Amount,
			HeldAt:      timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type ReleaseCash struct {
	AccountID   string
	OrderSagaID string
	Amount      int64
}

func (c ReleaseCash) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteReleaseCash(p *Portfolio, cmd ReleaseCash) ([]es.Event, error) {
	if cmd.Amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if p.HoldsBySaga[cmd.OrderSagaID] == 0 {
		return nil, nil
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventCashReleased,
		Timestamp:   now,
		Data: &portfoliov1.CashReleased{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Amount:      cmd.Amount,
			ReleasedAt:  timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type SettleTrade struct {
	AccountID    string
	OrderSagaID  string
	TradeID      string
	Amount       int64
	Symbol       string
	Quantity     int64
	CostPerShare int64
}

func (c SettleTrade) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteSettleTrade(p *Portfolio, cmd SettleTrade) ([]es.Event, error) {
	if p.HasSettled(cmd.OrderSagaID, cmd.TradeID) {
		return nil, nil
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventCashSettled,
		Timestamp:   now,
		Data: &portfoliov1.CashSettled{
			AccountId:    cmd.AccountID,
			OrderSagaId:  cmd.OrderSagaID,
			TradeId:      cmd.TradeID,
			Amount:       cmd.Amount,
			Symbol:       cmd.Symbol,
			Quantity:     cmd.Quantity,
			CostPerShare: cmd.CostPerShare,
			SettledAt:    timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type CreditShares struct {
	AccountID    string
	Symbol       string
	Quantity     int64
	CostPerShare int64
}

func (c CreditShares) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteCreditShares(p *Portfolio, cmd CreditShares) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSharesCredited,
		Timestamp:   now,
		Data: &portfoliov1.SharesCredited{
			AccountId:    cmd.AccountID,
			Symbol:       cmd.Symbol,
			Quantity:     cmd.Quantity,
			CostPerShare: cmd.CostPerShare,
			CreditedAt:   timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type HoldShares struct {
	AccountID   string
	OrderSagaID string
	Symbol      string
	Quantity    int64
}

func (c HoldShares) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteHoldShares(p *Portfolio, cmd HoldShares) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if p.ShareHoldsBySaga[cmd.OrderSagaID] != nil {
		return nil, nil
	}
	h := p.Holdings[cmd.Symbol]
	available := int64(0)
	if h != nil {
		available = h.Quantity - p.SharesHeld[cmd.Symbol]
	}
	if available < cmd.Quantity {
		return nil, ErrInsufficientShares
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSharesHeld,
		Timestamp:   now,
		Data: &portfoliov1.SharesHeld{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Symbol:      cmd.Symbol,
			Quantity:    cmd.Quantity,
			HeldAt:      timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type ReleaseShares struct {
	AccountID   string
	OrderSagaID string
	Symbol      string
	Quantity    int64
}

func (c ReleaseShares) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteReleaseShares(p *Portfolio, cmd ReleaseShares) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if p.ShareHoldsBySaga[cmd.OrderSagaID] == nil {
		return nil, nil
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSharesReleased,
		Timestamp:   now,
		Data: &portfoliov1.SharesReleased{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Symbol:      cmd.Symbol,
			Quantity:    cmd.Quantity,
			ReleasedAt:  timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type SettleSale struct {
	AccountID     string
	OrderSagaID   string
	TradeID       string
	Symbol        string
	Quantity      int64
	PricePerShare int64
	Proceeds      int64
}

func (c SettleSale) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteSettleSale(p *Portfolio, cmd SettleSale) ([]es.Event, error) {
	if p.HasSettled(cmd.OrderSagaID, cmd.TradeID) {
		return nil, nil
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSharesSettled,
		Timestamp:   now,
		Data: &portfoliov1.SharesSettled{
			AccountId:     cmd.AccountID,
			OrderSagaId:   cmd.OrderSagaID,
			TradeId:       cmd.TradeID,
			Symbol:        cmd.Symbol,
			Quantity:      cmd.Quantity,
			PricePerShare: cmd.PricePerShare,
			Proceeds:      cmd.Proceeds,
			SettledAt:     timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

// --- Short-selling commands ---

type HoldCollateral struct {
	AccountID   string
	OrderSagaID string
	Symbol      string
	Quantity    int64
	// Amount is pre-computed from policy (e.g. collateral_rate * notional).
	// Aggregate stays policy-agnostic.
	Amount int64
}

func (c HoldCollateral) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteHoldCollateral(p *Portfolio, cmd HoldCollateral) ([]es.Event, error) {
	if cmd.Amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if _, exists := p.CollateralHeldBySaga[cmd.OrderSagaID]; exists {
		return nil, nil
	}
	if h, ok := p.Holdings[cmd.Symbol]; ok && h.Quantity > 0 {
		return nil, ErrShortHoldsLong
	}
	if p.CashBalance < cmd.Amount {
		return nil, ErrInsufficientFunds
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventCollateralHeld,
		Timestamp:   now,
		Data: &portfoliov1.CollateralHeld{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Symbol:      cmd.Symbol,
			Quantity:    cmd.Quantity,
			Amount:      cmd.Amount,
			HeldAt:      timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type ReleaseCollateral struct {
	AccountID   string
	OrderSagaID string
}

func (c ReleaseCollateral) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteReleaseCollateral(p *Portfolio, cmd ReleaseCollateral) ([]es.Event, error) {
	hold, ok := p.CollateralHeldBySaga[cmd.OrderSagaID]
	if !ok {
		return nil, nil
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventCollateralReleased,
		Timestamp:   now,
		Data: &portfoliov1.CollateralReleased{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Amount:      hold.Amount,
			ReleasedAt:  timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type OpenShort struct {
	AccountID     string
	OrderSagaID   string
	TradeID       string
	Symbol        string
	Quantity      int64
	PricePerShare int64
}

func (c OpenShort) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteOpenShort(p *Portfolio, cmd OpenShort) ([]es.Event, error) {
	if p.HasSettled(cmd.OrderSagaID, cmd.TradeID) {
		return nil, nil
	}
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if h, ok := p.Holdings[cmd.Symbol]; ok && h.Quantity > 0 {
		return nil, ErrShortHoldsLong
	}

	proceeds := cmd.PricePerShare * cmd.Quantity

	// Collateral comes from the pre-fill hold if any; the saga is
	// responsible for posting it via HoldCollateral first.
	collateralHeld := int64(0)
	if hold := p.CollateralHeldBySaga[cmd.OrderSagaID]; hold != nil {
		collateralHeld = hold.Amount
	}

	// Weighted average open price across all opens of this short.
	short := p.ShortPositions[cmd.Symbol]
	newAvg := cmd.PricePerShare
	if short != nil && short.Quantity > 0 {
		newQty := short.Quantity + cmd.Quantity
		newAvg = (short.AvgOpenPrice*short.Quantity + cmd.PricePerShare*cmd.Quantity) / newQty
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventShortOpened,
		Timestamp:   now,
		Data: &portfoliov1.ShortOpened{
			AccountId:       cmd.AccountID,
			OrderSagaId:     cmd.OrderSagaID,
			TradeId:         cmd.TradeID,
			Symbol:          cmd.Symbol,
			Quantity:        cmd.Quantity,
			PricePerShare:   cmd.PricePerShare,
			ProceedsHeld:    proceeds,
			CollateralHeld:  collateralHeld,
			NewAvgOpenPrice: newAvg,
			OpenedAt:        timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type HoldShortCover struct {
	AccountID   string
	OrderSagaID string
	Symbol      string
	Quantity    int64
}

func (c HoldShortCover) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteHoldShortCover(p *Portfolio, cmd HoldShortCover) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if p.ShortCoverHoldsBySaga[cmd.OrderSagaID] != nil {
		return nil, nil
	}
	short := p.ShortPositions[cmd.Symbol]
	available := int64(0)
	if short != nil {
		available = short.Quantity - p.ShortCoversHeld[cmd.Symbol]
	}
	if available < cmd.Quantity {
		return nil, ErrInsufficientShortQty
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventShortCoverHeld,
		Timestamp:   now,
		Data: &portfoliov1.ShortCoverHeld{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Symbol:      cmd.Symbol,
			Quantity:    cmd.Quantity,
			HeldAt:      timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type ReleaseShortCover struct {
	AccountID   string
	OrderSagaID string
	Symbol      string
	Quantity    int64
}

func (c ReleaseShortCover) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteReleaseShortCover(p *Portfolio, cmd ReleaseShortCover) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if p.ShortCoverHoldsBySaga[cmd.OrderSagaID] == nil {
		return nil, nil
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventShortCoverReleased,
		Timestamp:   now,
		Data: &portfoliov1.ShortCoverReleased{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Symbol:      cmd.Symbol,
			Quantity:    cmd.Quantity,
			ReleasedAt:  timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type CoverShort struct {
	AccountID    string
	OrderSagaID  string
	TradeID      string
	Symbol       string
	Quantity     int64
	CostPerShare int64
}

func (c CoverShort) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteCoverShort(p *Portfolio, cmd CoverShort) ([]es.Event, error) {
	if p.HasSettled(cmd.OrderSagaID, cmd.TradeID) {
		return nil, nil
	}
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	short, ok := p.ShortPositions[cmd.Symbol]
	if !ok || short.Quantity < cmd.Quantity {
		// Cover cannot go through zero into a long.
		return nil, ErrInsufficientShortQty
	}

	// Proportional pool release; rounding tail drains on full close.
	proceedsReleased := short.ProceedsHeld * cmd.Quantity / short.Quantity
	collateralReleased := short.CollateralHeld * cmd.Quantity / short.Quantity
	cost := cmd.CostPerShare * cmd.Quantity
	realizedPnL := (short.AvgOpenPrice - cmd.CostPerShare) * cmd.Quantity

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventShortCovered,
		Timestamp:   now,
		Data: &portfoliov1.ShortCovered{
			AccountId:          cmd.AccountID,
			OrderSagaId:        cmd.OrderSagaID,
			TradeId:            cmd.TradeID,
			Symbol:             cmd.Symbol,
			Quantity:           cmd.Quantity,
			CostPerShare:       cmd.CostPerShare,
			Cost:               cost,
			ProceedsReleased:   proceedsReleased,
			CollateralReleased: collateralReleased,
			RealizedPnl:        realizedPnL,
			CoveredAt:          timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

// --- Margin call commands ---

type IssueMarginCall struct {
	AccountID                     string
	CallID                        string
	TriggerTradeID                string
	TriggerSymbol                 string
	MarkPrice                     int64
	EquityAtIssue                 int64
	MaintenanceRequirementAtIssue int64
}

func (c IssueMarginCall) AggregateID() string {
	return AggregateID(c.AccountID)
}

// ExecuteIssueMarginCall is idempotent against an already-open call:
// if ActiveMarginCall has the same call_id, returns no-op. Different
// call_id while one is active returns no-op too — the reactor uses
// MarginCallCovered to clear before issuing a new one.
func ExecuteIssueMarginCall(p *Portfolio, cmd IssueMarginCall) ([]es.Event, error) {
	if p.ActiveMarginCall != nil {
		return nil, nil
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventMarginCallIssued,
		Timestamp:   now,
		Data: &portfoliov1.MarginCallIssued{
			AccountId:                     cmd.AccountID,
			CallId:                        cmd.CallID,
			TriggerTradeId:                cmd.TriggerTradeID,
			TriggerSymbol:                 cmd.TriggerSymbol,
			MarkPrice:                     cmd.MarkPrice,
			EquityAtIssue:                 cmd.EquityAtIssue,
			MaintenanceRequirementAtIssue: cmd.MaintenanceRequirementAtIssue,
			IssuedAt:                      timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type CoverMarginCall struct {
	AccountID                      string
	EquityAtCover                  int64
	MaintenanceRequirementAtCover  int64
}

func (c CoverMarginCall) AggregateID() string {
	return AggregateID(c.AccountID)
}

// ExecuteCoverMarginCall clears the active call. No-op if none open.
// CallID is derived from aggregate state so callers don't need to
// remember which call is currently active.
func ExecuteCoverMarginCall(p *Portfolio, cmd CoverMarginCall) ([]es.Event, error) {
	if p.ActiveMarginCall == nil {
		return nil, nil
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventMarginCallCovered,
		Timestamp:   now,
		Data: &portfoliov1.MarginCallCovered{
			AccountId:                     cmd.AccountID,
			CallId:                        p.ActiveMarginCall.CallID,
			EquityAtCover:                 cmd.EquityAtCover,
			MaintenanceRequirementAtCover: cmd.MaintenanceRequirementAtCover,
			CoveredAt:                     timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}
