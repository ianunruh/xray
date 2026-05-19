package portfolio

import (
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/margin"
	"github.com/ianunruh/xray/pkg/es"
)

// applyTxnFee computes the per-side transaction fee on notional,
// builds and applies the matching TransactionFeeCharged event, and
// returns it for the caller to include in its event list. Returns
// (zero event, false) when the fee rounds to zero — settlement
// callers should omit the event entirely in that case.
func applyTxnFee(p *Portfolio, accountID, sagaID, tradeID, symbol string, notional int64, positionSide orderbookv1.PositionSide, now time.Time) (es.Event, bool, error) {
	amount := margin.TxnFeeAmount(notional)
	if amount <= 0 {
		return es.Event{}, false, nil
	}
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventTransactionFeeCharged,
		Timestamp:   now,
		Data: &portfoliov1.TransactionFeeCharged{
			AccountId:    accountID,
			OrderSagaId:  sagaID,
			TradeId:      tradeID,
			Symbol:       symbol,
			Notional:     notional,
			RateBps:      margin.TxnFeeBps,
			Amount:       amount,
			ChargedAt:    timestamppb.New(now),
			PositionSide: positionSide,
		},
	}
	if err := p.Apply(evt); err != nil {
		return es.Event{}, false, err
	}
	return evt, true, nil
}

var (
	ErrInvalidAmount        = errors.New("amount must be positive")
	ErrInsufficientFunds    = errors.New("insufficient funds")
	ErrInsufficientShares   = errors.New("insufficient shares")
	ErrInsufficientShortQty = errors.New("cover quantity exceeds open short")
	ErrShortHoldsLong       = errors.New("cannot open short while holding long in symbol")
	ErrLongHoldsShort       = errors.New("cannot buy long while holding short in symbol")
	ErrUnsettledFunds       = errors.New("withdrawal exceeds settled cash")
	ErrSettlementNotDue     = errors.New("pending settlement leg is not yet due")
)

// SettlementPolicy controls whether trade-settlement events defer
// their cash leg to a later SettlementCleared event (T+1 settlement)
// or apply it immediately (today's instant behavior). Caller-side
// concern: the aggregate consumes only the stamped SettlesAt on each
// event — see isInstantSettlement in aggregate.go.
type SettlementPolicy struct {
	// Enabled toggles deferred settlement. When false, SettlesAt is
	// always zero and the aggregate takes the instant path.
	Enabled bool
	// Window is the gap from trade date to settlement date when
	// Enabled. Typical real-world value: 24h (T+1).
	Window time.Duration
}

// SettlesAt computes the settlement date for a trade executed at
// tradeDate. Returns the zero time when the policy is disabled or the
// window is non-positive — both encode "instant" on the wire. Callers
// stamp the result onto the settlement event's settles_at field.
func (p SettlementPolicy) SettlesAt(tradeDate time.Time) time.Time {
	if !p.Enabled || p.Window <= 0 {
		return time.Time{}
	}
	return tradeDate.Add(p.Window)
}

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
	// Withdrawal must come from cleared funds: unsettled credits
	// (e.g. proceeds from a sell that hasn't settled yet) sit in
	// PendingLegs and can be traded against but not withdrawn.
	if p.SettledCash < cmd.Amount {
		return nil, ErrUnsettledFunds
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
	// Buying on margin is allowed: CashBalance can go negative, with
	// the deficit representing a broker loan. The saga reactor +
	// PreviewOrderImpact + margin reactor are the gatekeepers — they
	// know the marks and can compute equity / maintenance. The
	// aggregate trusts the caller; over-leverage just triggers a
	// margin call on the next mark update.

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
	// SettlesAt, when non-zero and after the trade date, marks this
	// settlement as T+N and routes it through PendingLegs. Zero =
	// instant settlement (today's behavior).
	SettlesAt time.Time
}

func (c SettleTrade) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteSettleTrade(p *Portfolio, cmd SettleTrade) ([]es.Event, error) {
	if p.HasSettled(cmd.OrderSagaID, cmd.TradeID) {
		return nil, nil
	}
	now := time.Now()
	data := &portfoliov1.CashSettled{
		AccountId:    cmd.AccountID,
		OrderSagaId:  cmd.OrderSagaID,
		TradeId:      cmd.TradeID,
		Amount:       cmd.Amount,
		Symbol:       cmd.Symbol,
		Quantity:     cmd.Quantity,
		CostPerShare: cmd.CostPerShare,
		SettledAt:    timestamppb.New(now),
	}
	if !cmd.SettlesAt.IsZero() {
		data.SettlesAt = timestamppb.New(cmd.SettlesAt)
	}
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventCashSettled,
		Timestamp:   now,
		Data:        data,
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	out := []es.Event{evt}
	if feeEvt, ok, err := applyTxnFee(p, cmd.AccountID, cmd.OrderSagaID, cmd.TradeID, cmd.Symbol, cmd.Amount, orderbookv1.PositionSide_POSITION_SIDE_LONG, now); err != nil {
		return nil, err
	} else if ok {
		out = append(out, feeEvt)
	}
	return out, nil
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
	// SettlesAt — see SettleTrade.SettlesAt.
	SettlesAt time.Time
}

func (c SettleSale) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteSettleSale(p *Portfolio, cmd SettleSale) ([]es.Event, error) {
	if p.HasSettled(cmd.OrderSagaID, cmd.TradeID) {
		return nil, nil
	}
	now := time.Now()
	data := &portfoliov1.SharesSettled{
		AccountId:     cmd.AccountID,
		OrderSagaId:   cmd.OrderSagaID,
		TradeId:       cmd.TradeID,
		Symbol:        cmd.Symbol,
		Quantity:      cmd.Quantity,
		PricePerShare: cmd.PricePerShare,
		Proceeds:      cmd.Proceeds,
		SettledAt:     timestamppb.New(now),
	}
	if !cmd.SettlesAt.IsZero() {
		data.SettlesAt = timestamppb.New(cmd.SettlesAt)
	}
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSharesSettled,
		Timestamp:   now,
		Data:        data,
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	out := []es.Event{evt}
	if feeEvt, ok, err := applyTxnFee(p, cmd.AccountID, cmd.OrderSagaID, cmd.TradeID, cmd.Symbol, cmd.Proceeds, orderbookv1.PositionSide_POSITION_SIDE_LONG, now); err != nil {
		return nil, err
	} else if ok {
		out = append(out, feeEvt)
	}
	return out, nil
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
	// SettlesAt — see SettleTrade.SettlesAt.
	SettlesAt time.Time
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
	data := &portfoliov1.ShortOpened{
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
	}
	if !cmd.SettlesAt.IsZero() {
		data.SettlesAt = timestamppb.New(cmd.SettlesAt)
	}
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventShortOpened,
		Timestamp:   now,
		Data:        data,
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	out := []es.Event{evt}
	if feeEvt, ok, err := applyTxnFee(p, cmd.AccountID, cmd.OrderSagaID, cmd.TradeID, cmd.Symbol, proceeds, orderbookv1.PositionSide_POSITION_SIDE_SHORT, now); err != nil {
		return nil, err
	} else if ok {
		out = append(out, feeEvt)
	}
	return out, nil
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
	// SettlesAt — see SettleTrade.SettlesAt.
	SettlesAt time.Time
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
	data := &portfoliov1.ShortCovered{
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
	}
	if !cmd.SettlesAt.IsZero() {
		data.SettlesAt = timestamppb.New(cmd.SettlesAt)
	}
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventShortCovered,
		Timestamp:   now,
		Data:        data,
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	out := []es.Event{evt}
	if feeEvt, ok, err := applyTxnFee(p, cmd.AccountID, cmd.OrderSagaID, cmd.TradeID, cmd.Symbol, cost, orderbookv1.PositionSide_POSITION_SIDE_SHORT, now); err != nil {
		return nil, err
	} else if ok {
		out = append(out, feeEvt)
	}
	return out, nil
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
	// GraceExpiresAt freezes the auto-liquidation deadline at the
	// reactor's grace-window at issue time. Zero means "no grace" —
	// reactor wires the actual value.
	GraceExpiresAt time.Time
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
	data := &portfoliov1.MarginCallIssued{
		AccountId:                     cmd.AccountID,
		CallId:                        cmd.CallID,
		TriggerTradeId:                cmd.TriggerTradeID,
		TriggerSymbol:                 cmd.TriggerSymbol,
		MarkPrice:                     cmd.MarkPrice,
		EquityAtIssue:                 cmd.EquityAtIssue,
		MaintenanceRequirementAtIssue: cmd.MaintenanceRequirementAtIssue,
		IssuedAt:                      timestamppb.New(now),
	}
	if !cmd.GraceExpiresAt.IsZero() {
		data.GraceExpiresAt = timestamppb.New(cmd.GraceExpiresAt)
	}
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventMarginCallIssued,
		Timestamp:   now,
		Data:        data,
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

// --- Fees accrual ---

// BorrowFeeEntry is one open short's borrow fee for a single accrual
// cycle. Computed by the accruer using current mark and elapsed time
// since the account's LastAccruedAt.
type BorrowFeeEntry struct {
	Symbol    string
	MarkPrice int64
	Qty       int64
	RateBps   int64
	Amount    int64
}

// AccrueFees bundles one cycle of margin interest plus per-symbol
// short borrow fees. The accruer computes amounts (it has marks); the
// aggregate just records them and advances LastAccruedAt.
//
// MarginInterestAccrued always fires (even with Amount=0), so the
// accrual clock advances each cycle regardless of whether there's a
// loan to charge. ShortBorrowFeeAccrued fires once per entry with
// Amount > 0.
type AccrueFees struct {
	AccountID      string
	PeriodStart    time.Time
	PeriodEnd      time.Time
	LoanPrincipal  int64
	LoanRateBps    int64
	InterestAmount int64
	BorrowFees     []BorrowFeeEntry
}

func (c AccrueFees) AggregateID() string { return AggregateID(c.AccountID) }

func ExecuteAccrueFees(p *Portfolio, cmd AccrueFees) ([]es.Event, error) {
	periodStart := timestamppb.New(cmd.PeriodStart)
	periodEnd := timestamppb.New(cmd.PeriodEnd)

	events := make([]es.Event, 0, 1+len(cmd.BorrowFees))

	interestEvt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventMarginInterestAccrued,
		Timestamp:   cmd.PeriodEnd,
		Data: &portfoliov1.MarginInterestAccrued{
			AccountId:   cmd.AccountID,
			PeriodStart: periodStart,
			PeriodEnd:   periodEnd,
			Principal:   cmd.LoanPrincipal,
			RateBps:     cmd.LoanRateBps,
			Amount:      cmd.InterestAmount,
		},
	}
	if err := p.Apply(interestEvt); err != nil {
		return nil, err
	}
	events = append(events, interestEvt)

	for _, bf := range cmd.BorrowFees {
		if bf.Amount <= 0 {
			continue
		}
		feeEvt := es.Event{
			AggregateID: p.AggregateID(),
			Type:        EventShortBorrowFeeAccrued,
			Timestamp:   cmd.PeriodEnd,
			Data: &portfoliov1.ShortBorrowFeeAccrued{
				AccountId:   cmd.AccountID,
				PeriodStart: periodStart,
				PeriodEnd:   periodEnd,
				Symbol:      bf.Symbol,
				MarkPrice:   bf.MarkPrice,
				Qty:         bf.Qty,
				RateBps:     bf.RateBps,
				Amount:      bf.Amount,
			},
		}
		if err := p.Apply(feeEvt); err != nil {
			return nil, err
		}
		events = append(events, feeEvt)
	}
	return events, nil
}

// ClearSettlement clears one pending settlement leg, emitting
// SettlementCleared. Idempotent: a leg that's already cleared (or
// never existed) returns no events without error. The settlement
// reactor calls this once per due leg per tick.
type ClearSettlement struct {
	AccountID string
	TradeID   string
	Kind      portfoliov1.SettlementLegKind
}

func (c ClearSettlement) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteClearSettlement(p *Portfolio, cmd ClearSettlement) ([]es.Event, error) {
	leg, ok := p.PendingLegs[PendingLegKey{TradeID: cmd.TradeID, Kind: cmd.Kind}]
	if !ok {
		return nil, nil
	}
	now := time.Now()
	if leg.SettlesAt.After(now) {
		return nil, ErrSettlementNotDue
	}
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSettlementCleared,
		Timestamp:   now,
		Data: &portfoliov1.SettlementCleared{
			AccountId:   cmd.AccountID,
			OrderSagaId: leg.OrderSagaID,
			TradeId:     cmd.TradeID,
			Kind:        cmd.Kind,
			CashAmount:  leg.CashAmount,
			ClearedAt:   timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

// AdjustHolding scales a portfolio's per-symbol holding by
// numerator/denominator (a split ratio). Pre-computes new_quantity
// and the truncation residue. Idempotent on the action_id —
// re-issuing the same command is a no-op so the corpaction reactor
// can safely retry.
type AdjustHolding struct {
	AccountID   string
	ActionID    string
	Symbol      string
	Numerator   int32
	Denominator int32
}

func (c AdjustHolding) AggregateID() string { return AggregateID(c.AccountID) }

func ExecuteAdjustHolding(p *Portfolio, cmd AdjustHolding) ([]es.Event, error) {
	if cmd.ActionID == "" {
		return nil, errors.New("action_id required")
	}
	if cmd.Numerator <= 0 || cmd.Denominator <= 0 {
		return nil, errors.New("ratio numerator and denominator must be positive")
	}
	if p.HasAppliedAction(cmd.ActionID) {
		return nil, nil
	}
	h, ok := p.Holdings[cmd.Symbol]
	if !ok || h.Quantity <= 0 {
		// No position to adjust — still emit the event so the
		// action is recorded as applied for this account (keeps
		// the audit trail complete).
		return emitHoldingAdjusted(p, cmd, 0, 0, 0)
	}
	// Forward split (numerator > denominator) multiplies; reverse
	// (denominator > numerator) divides. Truncation residue from
	// reverse splits is recorded separately so the UI can surface
	// the loss.
	oldQty := h.Quantity
	scaled := oldQty * int64(cmd.Numerator)
	newQty := scaled / int64(cmd.Denominator)
	residueShares := scaled - (newQty * int64(cmd.Denominator))
	// residueShares is denominated in the post-multiply space (i.e.
	// at the new share scale); convert back to "shares we lost" in
	// the pre-split denomination by dividing by numerator.
	dropped := int64(0)
	if residueShares > 0 {
		dropped = residueShares / int64(cmd.Numerator)
		if dropped == 0 {
			// Sub-share residue still represents loss in the new
			// denomination; round up to 1 share so the audit
			// reflects that something was truncated.
			dropped = 1
		}
	}
	return emitHoldingAdjusted(p, cmd, oldQty, newQty, dropped)
}

func emitHoldingAdjusted(p *Portfolio, cmd AdjustHolding, oldQty, newQty, dropped int64) ([]es.Event, error) {
	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventHoldingAdjusted,
		Timestamp:   now,
		Data: &portfoliov1.HoldingAdjusted{
			AccountId:     cmd.AccountID,
			ActionId:      cmd.ActionID,
			Symbol:        cmd.Symbol,
			Numerator:     cmd.Numerator,
			Denominator:   cmd.Denominator,
			OldQuantity:   oldQty,
			NewQuantity:   newQty,
			DroppedShares: dropped,
			AdjustedAt:    timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

// CreditDividend pays a cash dividend to an account. Idempotent on
// action_id — re-issuing the same command is a no-op so the
// corpaction reactor can safely retry. PerShare and shares pre-
// computed (caller knows them from the record-date snapshot); the
// amount is multiplied here so the event carries an independently
// verifiable value.
type CreditDividend struct {
	AccountID      string
	ActionID       string
	Symbol         string
	SharesOfRecord int64
	PerShare       int64
}

func (c CreditDividend) AggregateID() string { return AggregateID(c.AccountID) }

func ExecuteCreditDividend(p *Portfolio, cmd CreditDividend) ([]es.Event, error) {
	if cmd.ActionID == "" {
		return nil, errors.New("action_id required")
	}
	if cmd.SharesOfRecord <= 0 || cmd.PerShare <= 0 {
		return nil, errors.New("shares_of_record and per_share must be positive")
	}
	if p.HasAppliedAction(cmd.ActionID) {
		return nil, nil
	}
	amount := cmd.SharesOfRecord * cmd.PerShare
	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventDividendCredited,
		Timestamp:   now,
		Data: &portfoliov1.DividendCredited{
			AccountId:      cmd.AccountID,
			ActionId:       cmd.ActionID,
			Symbol:         cmd.Symbol,
			SharesOfRecord: cmd.SharesOfRecord,
			PerShare:       cmd.PerShare,
			Amount:         amount,
			CreditedAt:     timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

// MigrateSymbol rewrites a portfolio's per-symbol positional state
// from OldSymbol to NewSymbol on a SYMBOL_CHANGE corporate action.
// Idempotent on action_id — re-issuing the same command is a no-op.
type MigrateSymbol struct {
	AccountID string
	ActionID  string
	OldSymbol string
	NewSymbol string
}

func (c MigrateSymbol) AggregateID() string { return AggregateID(c.AccountID) }

func ExecuteMigrateSymbol(p *Portfolio, cmd MigrateSymbol) ([]es.Event, error) {
	if cmd.ActionID == "" {
		return nil, errors.New("action_id required")
	}
	if cmd.OldSymbol == "" || cmd.NewSymbol == "" {
		return nil, errors.New("old_symbol and new_symbol required")
	}
	if cmd.OldSymbol == cmd.NewSymbol {
		return nil, errors.New("new_symbol must differ from old_symbol")
	}
	if p.HasAppliedAction(cmd.ActionID) {
		return nil, nil
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSymbolMigrated,
		Timestamp:   now,
		Data: &portfoliov1.SymbolMigrated{
			AccountId:   cmd.AccountID,
			ActionId:    cmd.ActionID,
			OldSymbol:   cmd.OldSymbol,
			NewSymbol:   cmd.NewSymbol,
			MigratedAt:  timestamppb.New(now),
		},
	}
	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}
