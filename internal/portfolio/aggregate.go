package portfolio

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const AggregateType = "portfolio"

func AggregateID(accountID string) string {
	return AggregateType + ":" + accountID
}

type Holding struct {
	Quantity  int64
	TotalCost int64
}

type ShareHold struct {
	Symbol   string
	Quantity int64
}

// ShortPosition tracks one symbol's open short. Quantity is positive
// (the number of shares owed back). ProceedsHeld is the cash received
// from sell-to-opens, locked. CollateralHeld is the additional cash
// posted as margin. AvgOpenPrice is weighted across opens.
type ShortPosition struct {
	Quantity       int64
	ProceedsHeld   int64
	CollateralHeld int64
	AvgOpenPrice   int64
}

// CollateralHold is a per-saga pre-fill cash collateral hold for a
// pending short-open. Consumed by ShortOpened or returned by
// CollateralReleased.
type CollateralHold struct {
	Symbol   string
	Quantity int64
	Amount   int64
}

// PendingLeg is a settlement leg awaiting clearing. One per (TradeID,
// Kind); on SettlementCleared the entry is removed and CashAmount is
// rolled into SettledCash. CashBalance is unaffected on clear because
// it already reflected the trade on trade date.
type PendingLeg struct {
	TradeID     string
	OrderSagaID string
	Kind        portfoliov1.SettlementLegKind
	Symbol      string
	CashAmount  int64 // signed: + = credit, - = debit
	SettlesAt   time.Time
	EmittedAt   time.Time
}

// PendingLegKey is the map key for PendingLegs: one entry per
// (trade_id, kind). A single trade can spawn at most one leg of each
// kind (e.g., SHORT_OPEN never coexists with CASH_DEBIT on the same
// trade ID), so collisions are not possible in practice.
type PendingLegKey struct {
	TradeID string
	Kind    portfoliov1.SettlementLegKind
}

// MarginCall records an active margin breach. At most one is active
// per account — the reactor only emits MarginCallIssued when none is
// open, and emits MarginCallCovered to clear it.
type MarginCall struct {
	CallID             string
	TriggerTradeID     string
	TriggerSymbol      string
	MarkPrice          int64
	EquityAtIssue      int64
	RequirementAtIssue int64
	IssuedAt           time.Time
	// GraceExpiresAt is when auto-liquidation fires if the breach
	// isn't resolved beforehand. Computed at issue time by the
	// reactor as IssuedAt + grace and frozen on the event.
	GraceExpiresAt time.Time
}

type Portfolio struct {
	es.AggregateBase

	AccountID        string
	CashBalance      int64
	CashHeld         int64
	Holdings         map[string]*Holding
	HoldsBySaga      map[string]int64
	SharesHeld       map[string]int64
	ShareHoldsBySaga map[string]*ShareHold
	// SettledTrades tracks per-saga trade IDs already applied to this
	// portfolio. Used to make SettleSale/SettleTrade idempotent against
	// a single trade being delivered more than once — for example, when
	// a reactor batch crashes after settling some fills and is then
	// redelivered on restart.
	SettledTrades map[string]map[string]struct{}

	// Short-selling state.
	ShortPositions        map[string]*ShortPosition
	ProceedsPool          int64
	CollateralPool        int64
	CollateralHeldBySaga  map[string]*CollateralHold
	ShortCoversHeld       map[string]int64
	ShortCoverHoldsBySaga map[string]*ShareHold
	// ActiveMarginCall is non-nil when a margin call is open. The
	// reactor guards against duplicate issuance by checking this field
	// before emitting MarginCallIssued.
	ActiveMarginCall *MarginCall

	// LastAccruedAt is the period_end of the most recent
	// MarginInterestAccrued or ShortBorrowFeeAccrued event applied.
	// The fees accruer reads this to compute the elapsed window for
	// the next accrual cycle. Initialized lazily in Apply() to the
	// timestamp of the first event applied to the aggregate.
	LastAccruedAt time.Time

	// SettledCash is the cleared-settlement portion of CashBalance.
	// Withdraw is gated against it. Invariant:
	//   SettledCash + sum(PendingLegs.CashAmount) == CashBalance.
	// Lazy-seeded equal to CashBalance for portfolios that pre-date
	// T+1 settlement (legacy events emit SettlesAt == SettledAt and
	// land directly in SettledCash).
	SettledCash int64

	// PendingLegs are settlement legs awaiting their settles_at
	// instant. Keyed by (trade_id, kind) so clearing is an O(1)
	// lookup and idempotent.
	PendingLegs map[PendingLegKey]*PendingLeg

	// settledCashSeeded marks that Apply() has lazily set SettledCash
	// to CashBalance for a portfolio loaded from a pre-T+1 snapshot.
	// Unexported because it has no semantic meaning outside Apply.
	settledCashSeeded bool
}

func NewPortfolio(id string) *Portfolio {
	p := &Portfolio{
		Holdings:              make(map[string]*Holding),
		HoldsBySaga:           make(map[string]int64),
		SharesHeld:            make(map[string]int64),
		ShareHoldsBySaga:      make(map[string]*ShareHold),
		SettledTrades:         make(map[string]map[string]struct{}),
		ShortPositions:        make(map[string]*ShortPosition),
		CollateralHeldBySaga:  make(map[string]*CollateralHold),
		ShortCoversHeld:       make(map[string]int64),
		ShortCoverHoldsBySaga: make(map[string]*ShareHold),
		PendingLegs:           make(map[PendingLegKey]*PendingLeg),
	}
	p.SetID(id)
	return p
}

// MarginLoan returns the outstanding broker loan, derived from
// CashBalance going negative. Positive values mean the account owes
// the broker. Cash buying (long buys past cash) is what creates the
// loan; long sells and deposits pay it down naturally as CashBalance
// climbs back toward zero.
func (p *Portfolio) MarginLoan() int64 {
	if p.CashBalance >= 0 {
		return 0
	}
	return -p.CashBalance
}

// HasSettled reports whether the (saga, trade) pair has already settled
// against this portfolio. Empty tradeID returns false — legacy events
// without trade IDs bypass dedup, so callers always emit.
func (p *Portfolio) HasSettled(sagaID, tradeID string) bool {
	if tradeID == "" {
		return false
	}
	_, ok := p.SettledTrades[sagaID][tradeID]
	return ok
}

func (p *Portfolio) markSettled(sagaID, tradeID string) {
	if tradeID == "" {
		return
	}
	if p.SettledTrades[sagaID] == nil {
		p.SettledTrades[sagaID] = make(map[string]struct{})
	}
	p.SettledTrades[sagaID][tradeID] = struct{}{}
}

func (p *Portfolio) Apply(evt es.Event) error {
	// Seed the accrual clock on first event applied so the accruer
	// has a sensible "elapsed since" starting point. Cheap on every
	// apply since after the first one this is a no-op.
	if p.LastAccruedAt.IsZero() {
		p.LastAccruedAt = evt.Timestamp
	}
	// Seed SettledCash to CashBalance on first event for portfolios
	// that pre-date T+1 settlement (snapshots may have SettledCash=0
	// even though CashBalance is non-zero). After this seed, every
	// applier moves the two in lockstep (instant path) or records a
	// PendingLeg (deferred path).
	if !p.settledCashSeeded {
		p.SettledCash = p.CashBalance
		p.settledCashSeeded = true
	}
	// Detect the idle->liable transition. If we go from "no shorts
	// and no loan" to "has a liability" via this event, reset the
	// accrual clock to the event time. Otherwise the next accrual
	// tick would retroactively bill the dormant period.
	wasIdle := p.MarginLoan() == 0 && len(p.ShortPositions) == 0

	switch data := evt.Data.(type) {
	case *portfoliov1.CashDeposited:
		p.applyCashDeposited(data)
	case *portfoliov1.CashWithdrawn:
		p.applyCashWithdrawn(data)
	case *portfoliov1.CashHeld:
		p.applyCashHeld(data)
	case *portfoliov1.CashReleased:
		p.applyCashReleased(data)
	case *portfoliov1.CashSettled:
		p.applyCashSettled(data)
	case *portfoliov1.SharesCredited:
		p.applySharesCredited(data)
	case *portfoliov1.SharesDebited:
		p.applySharesDebited(data)
	case *portfoliov1.SharesHeld:
		p.applySharesHeld(data)
	case *portfoliov1.SharesReleased:
		p.applySharesReleased(data)
	case *portfoliov1.SharesSettled:
		p.applySharesSettled(data)
	case *portfoliov1.CollateralHeld:
		p.applyCollateralHeld(data)
	case *portfoliov1.CollateralReleased:
		p.applyCollateralReleased(data)
	case *portfoliov1.ShortOpened:
		p.applyShortOpened(data)
	case *portfoliov1.ShortCoverHeld:
		p.applyShortCoverHeld(data)
	case *portfoliov1.ShortCoverReleased:
		p.applyShortCoverReleased(data)
	case *portfoliov1.ShortCovered:
		p.applyShortCovered(data)
	case *portfoliov1.MarginCallIssued:
		p.applyMarginCallIssued(data)
	case *portfoliov1.MarginCallCovered:
		p.applyMarginCallCovered(data)
	case *portfoliov1.MarginInterestAccrued:
		p.applyMarginInterestAccrued(data)
	case *portfoliov1.ShortBorrowFeeAccrued:
		p.applyShortBorrowFeeAccrued(data)
	case *portfoliov1.TransactionFeeCharged:
		p.applyTransactionFeeCharged(data)
	case *portfoliov1.SettlementCleared:
		p.applySettlementCleared(data)
	default:
		return fmt.Errorf("unknown event type: %T", evt.Data)
	}
	if wasIdle && (p.MarginLoan() > 0 || len(p.ShortPositions) > 0) {
		// idle -> liable transition: reset the accrual clock so the
		// next tick doesn't retroactively bill the dormant period.
		p.LastAccruedAt = evt.Timestamp
	}
	p.IncrementVersion()
	return nil
}

func (p *Portfolio) applyCashDeposited(data *portfoliov1.CashDeposited) {
	p.AccountID = data.AccountId
	p.CashBalance += data.Amount
	p.SettledCash += data.Amount
}

func (p *Portfolio) applyCashWithdrawn(data *portfoliov1.CashWithdrawn) {
	p.CashBalance -= data.Amount
	p.SettledCash -= data.Amount
}

func (p *Portfolio) applyCashHeld(data *portfoliov1.CashHeld) {
	p.CashBalance -= data.Amount
	p.SettledCash -= data.Amount
	p.CashHeld += data.Amount
	p.HoldsBySaga[data.OrderSagaId] = data.Amount
}

func (p *Portfolio) applyCashReleased(data *portfoliov1.CashReleased) {
	p.CashHeld -= data.Amount
	p.CashBalance += data.Amount
	p.SettledCash += data.Amount
	delete(p.HoldsBySaga, data.OrderSagaId)
}

func (p *Portfolio) applyCashSettled(data *portfoliov1.CashSettled) {
	// Cap the hold deduction so CashHeld can never go negative. The hold is
	// an estimate (especially for market BUYs walking the ask book); if the
	// execution price exceeded that estimate, the overrun is debited
	// directly from CashBalance rather than silently masked.
	fromHold := min(data.Amount, p.HoldsBySaga[data.OrderSagaId])
	overflow := data.Amount - fromHold

	p.CashHeld -= fromHold
	p.CashBalance -= overflow
	p.HoldsBySaga[data.OrderSagaId] -= fromHold
	if p.HoldsBySaga[data.OrderSagaId] <= 0 {
		delete(p.HoldsBySaga, data.OrderSagaId)
	}

	h, ok := p.Holdings[data.Symbol]
	if !ok {
		h = &Holding{}
		p.Holdings[data.Symbol] = h
	}
	h.Quantity += data.Quantity
	h.TotalCost += data.CostPerShare * data.Quantity

	p.markSettled(data.OrderSagaId, data.TradeId)

	// Settled-cash bookkeeping. The full cost (fromHold + overflow)
	// is the cash debit; on the instant path it leaves SettledCash
	// now, on the deferred path it lands in a PendingLeg and clears
	// later via SettlementCleared.
	if isInstantSettlement(data.SettlesAt, data.SettledAt) {
		p.SettledCash -= data.Amount
	} else {
		p.PendingLegs[PendingLegKey{TradeID: data.TradeId, Kind: portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_DEBIT}] = &PendingLeg{
			TradeID:     data.TradeId,
			OrderSagaID: data.OrderSagaId,
			Kind:        portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_DEBIT,
			Symbol:      data.Symbol,
			CashAmount:  -data.Amount,
			SettlesAt:   data.SettlesAt.AsTime(),
			EmittedAt:   data.SettledAt.AsTime(),
		}
	}
}

func (p *Portfolio) applySharesCredited(data *portfoliov1.SharesCredited) {
	p.AccountID = data.AccountId
	h, ok := p.Holdings[data.Symbol]
	if !ok {
		h = &Holding{}
		p.Holdings[data.Symbol] = h
	}
	h.Quantity += data.Quantity
	h.TotalCost += data.CostPerShare * data.Quantity
}

func (p *Portfolio) applySharesDebited(data *portfoliov1.SharesDebited) {
	h, ok := p.Holdings[data.Symbol]
	if !ok {
		return
	}
	if h.Quantity > 0 {
		h.TotalCost = h.TotalCost * (h.Quantity - data.Quantity) / h.Quantity
	}
	h.Quantity -= data.Quantity
	if h.Quantity <= 0 {
		delete(p.Holdings, data.Symbol)
	}
}

func (p *Portfolio) applySharesHeld(data *portfoliov1.SharesHeld) {
	p.SharesHeld[data.Symbol] += data.Quantity
	p.ShareHoldsBySaga[data.OrderSagaId] = &ShareHold{
		Symbol:   data.Symbol,
		Quantity: data.Quantity,
	}
}

func (p *Portfolio) applySharesReleased(data *portfoliov1.SharesReleased) {
	p.SharesHeld[data.Symbol] -= data.Quantity
	if p.SharesHeld[data.Symbol] <= 0 {
		delete(p.SharesHeld, data.Symbol)
	}
	delete(p.ShareHoldsBySaga, data.OrderSagaId)
}

func (p *Portfolio) applySharesSettled(data *portfoliov1.SharesSettled) {
	p.SharesHeld[data.Symbol] -= data.Quantity
	if p.SharesHeld[data.Symbol] <= 0 {
		delete(p.SharesHeld, data.Symbol)
	}

	hold := p.ShareHoldsBySaga[data.OrderSagaId]
	if hold != nil {
		hold.Quantity -= data.Quantity
		if hold.Quantity <= 0 {
			delete(p.ShareHoldsBySaga, data.OrderSagaId)
		}
	}

	h, ok := p.Holdings[data.Symbol]
	if ok {
		if h.Quantity > 0 {
			h.TotalCost = h.TotalCost * (h.Quantity - data.Quantity) / h.Quantity
		}
		h.Quantity -= data.Quantity
		if h.Quantity <= 0 {
			delete(p.Holdings, data.Symbol)
		}
	}

	p.CashBalance += data.Proceeds

	p.markSettled(data.OrderSagaId, data.TradeId)

	if isInstantSettlement(data.SettlesAt, data.SettledAt) {
		p.SettledCash += data.Proceeds
	} else {
		p.PendingLegs[PendingLegKey{TradeID: data.TradeId, Kind: portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_CREDIT}] = &PendingLeg{
			TradeID:     data.TradeId,
			OrderSagaID: data.OrderSagaId,
			Kind:        portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_CREDIT,
			Symbol:      data.Symbol,
			CashAmount:  data.Proceeds,
			SettlesAt:   data.SettlesAt.AsTime(),
			EmittedAt:   data.SettledAt.AsTime(),
		}
	}
}

func (p *Portfolio) applyCollateralHeld(data *portfoliov1.CollateralHeld) {
	p.AccountID = data.AccountId
	p.CashBalance -= data.Amount
	p.SettledCash -= data.Amount
	p.CollateralHeldBySaga[data.OrderSagaId] = &CollateralHold{
		Symbol:   data.Symbol,
		Quantity: data.Quantity,
		Amount:   data.Amount,
	}
}

func (p *Portfolio) applyCollateralReleased(data *portfoliov1.CollateralReleased) {
	p.CashBalance += data.Amount
	p.SettledCash += data.Amount
	delete(p.CollateralHeldBySaga, data.OrderSagaId)
}

func (p *Portfolio) applyShortOpened(data *portfoliov1.ShortOpened) {
	p.AccountID = data.AccountId

	// Consume the pre-fill collateral hold. By construction
	// (ExecuteOpenShort sets data.CollateralHeld == hold.Amount),
	// the consumed amount equals the entire held amount, so we drop
	// the entry. If/when policy changes to require additional cash
	// at fill time (overflow), add an explicit field to ShortOpened
	// rather than rederiving it here — keeps the projection in sync.
	hold := p.CollateralHeldBySaga[data.OrderSagaId]
	if hold != nil {
		hold.Amount -= data.CollateralHeld
		if hold.Amount <= 0 {
			delete(p.CollateralHeldBySaga, data.OrderSagaId)
		}
	}

	short, ok := p.ShortPositions[data.Symbol]
	if !ok {
		short = &ShortPosition{}
		p.ShortPositions[data.Symbol] = short
	}
	short.Quantity += data.Quantity
	short.ProceedsHeld += data.ProceedsHeld
	short.CollateralHeld += data.CollateralHeld
	short.AvgOpenPrice = data.NewAvgOpenPrice

	p.ProceedsPool += data.ProceedsHeld
	p.CollateralPool += data.CollateralHeld

	p.markSettled(data.OrderSagaId, data.TradeId)

	// ShortOpened doesn't move CashBalance directly (the cash leg was
	// the prior CollateralHeld), so no SettledCash bookkeeping is
	// needed on the instant path. On the deferred path we still
	// record a zero-amount audit leg so the settlement reactor and
	// projection have a record that the trade is in-flight.
	if !isInstantSettlement(data.SettlesAt, data.OpenedAt) {
		p.PendingLegs[PendingLegKey{TradeID: data.TradeId, Kind: portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_SHORT_OPEN}] = &PendingLeg{
			TradeID:     data.TradeId,
			OrderSagaID: data.OrderSagaId,
			Kind:        portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_SHORT_OPEN,
			Symbol:      data.Symbol,
			CashAmount:  0,
			SettlesAt:   data.SettlesAt.AsTime(),
			EmittedAt:   data.OpenedAt.AsTime(),
		}
	}
}

func (p *Portfolio) applyShortCoverHeld(data *portfoliov1.ShortCoverHeld) {
	p.ShortCoversHeld[data.Symbol] += data.Quantity
	p.ShortCoverHoldsBySaga[data.OrderSagaId] = &ShareHold{
		Symbol:   data.Symbol,
		Quantity: data.Quantity,
	}
}

func (p *Portfolio) applyShortCoverReleased(data *portfoliov1.ShortCoverReleased) {
	p.ShortCoversHeld[data.Symbol] -= data.Quantity
	if p.ShortCoversHeld[data.Symbol] <= 0 {
		delete(p.ShortCoversHeld, data.Symbol)
	}
	delete(p.ShortCoverHoldsBySaga, data.OrderSagaId)
}

func (p *Portfolio) applyShortCovered(data *portfoliov1.ShortCovered) {
	short := p.ShortPositions[data.Symbol]

	short.ProceedsHeld -= data.ProceedsReleased
	short.CollateralHeld -= data.CollateralReleased
	p.ProceedsPool -= data.ProceedsReleased
	p.CollateralPool -= data.CollateralReleased

	if cover, ok := p.ShortCoverHoldsBySaga[data.OrderSagaId]; ok {
		cover.Quantity -= data.Quantity
		p.ShortCoversHeld[data.Symbol] -= data.Quantity
		if cover.Quantity <= 0 {
			delete(p.ShortCoverHoldsBySaga, data.OrderSagaId)
		}
		if p.ShortCoversHeld[data.Symbol] <= 0 {
			delete(p.ShortCoversHeld, data.Symbol)
		}
	}

	// Pay cost from released proceeds first, then released collateral,
	// then CashBalance for any residual (loss beyond pooled collateral).
	paid := data.Cost
	fromProceeds := min(paid, data.ProceedsReleased)
	paid -= fromProceeds
	fromCollateral := min(paid, data.CollateralReleased)
	paid -= fromCollateral
	p.CashBalance -= paid
	cashDelta := -paid

	returned := (data.ProceedsReleased - fromProceeds) +
		(data.CollateralReleased - fromCollateral)
	p.CashBalance += returned
	cashDelta += returned

	short.Quantity -= data.Quantity
	if short.Quantity <= 0 {
		// Drain rounding remainders from prior partial covers.
		drain := short.ProceedsHeld + short.CollateralHeld
		p.CashBalance += drain
		cashDelta += drain
		p.ProceedsPool -= short.ProceedsHeld
		p.CollateralPool -= short.CollateralHeld
		delete(p.ShortPositions, data.Symbol)
	}

	p.markSettled(data.OrderSagaId, data.TradeId)

	if isInstantSettlement(data.SettlesAt, data.CoveredAt) {
		p.SettledCash += cashDelta
	} else {
		p.PendingLegs[PendingLegKey{TradeID: data.TradeId, Kind: portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_SHORT_COVER}] = &PendingLeg{
			TradeID:     data.TradeId,
			OrderSagaID: data.OrderSagaId,
			Kind:        portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_SHORT_COVER,
			Symbol:      data.Symbol,
			CashAmount:  cashDelta,
			SettlesAt:   data.SettlesAt.AsTime(),
			EmittedAt:   data.CoveredAt.AsTime(),
		}
	}
}

func (p *Portfolio) applyMarginCallIssued(data *portfoliov1.MarginCallIssued) {
	p.AccountID = data.AccountId
	call := &MarginCall{
		CallID:             data.CallId,
		TriggerTradeID:     data.TriggerTradeId,
		TriggerSymbol:      data.TriggerSymbol,
		MarkPrice:          data.MarkPrice,
		EquityAtIssue:      data.EquityAtIssue,
		RequirementAtIssue: data.MaintenanceRequirementAtIssue,
		IssuedAt:           data.IssuedAt.AsTime(),
	}
	if data.GraceExpiresAt != nil {
		call.GraceExpiresAt = data.GraceExpiresAt.AsTime()
	}
	p.ActiveMarginCall = call
}

func (p *Portfolio) applyMarginCallCovered(_ *portfoliov1.MarginCallCovered) {
	p.ActiveMarginCall = nil
}

func (p *Portfolio) applyMarginInterestAccrued(data *portfoliov1.MarginInterestAccrued) {
	p.CashBalance -= data.Amount
	p.advanceAccrualClock(data.PeriodEnd)
}

func (p *Portfolio) applyShortBorrowFeeAccrued(data *portfoliov1.ShortBorrowFeeAccrued) {
	p.CashBalance -= data.Amount
	p.advanceAccrualClock(data.PeriodEnd)
}

func (p *Portfolio) applyTransactionFeeCharged(data *portfoliov1.TransactionFeeCharged) {
	p.CashBalance -= data.Amount
}

func (p *Portfolio) advanceAccrualClock(end *timestamppb.Timestamp) {
	if end == nil {
		return
	}
	if t := end.AsTime(); t.After(p.LastAccruedAt) {
		p.LastAccruedAt = t
	}
}

func (p *Portfolio) applySettlementCleared(data *portfoliov1.SettlementCleared) {
	key := PendingLegKey{TradeID: data.TradeId, Kind: data.Kind}
	if _, ok := p.PendingLegs[key]; !ok {
		// Idempotent: already cleared, or the leg never existed
		// (replay of a SettlementCleared whose event predates the
		// snapshot we restored from). CashBalance was unaffected
		// by clearing, so leaving SettledCash untouched preserves
		// the invariant.
		return
	}
	p.SettledCash += data.CashAmount
	delete(p.PendingLegs, key)
}

// isInstantSettlement reports whether a trade-date settlement event
// should apply its cash leg to SettledCash immediately (true) or defer
// it to a SettlementCleared event (false). Treats a zero or equal-to-
// trade-date settlesAt as instant; this keeps legacy events (no
// settles_at field at all) on the existing fast path.
func isInstantSettlement(settlesAt, tradeDate *timestamppb.Timestamp) bool {
	if settlesAt == nil {
		return true
	}
	s := settlesAt.AsTime()
	if s.IsZero() {
		return true
	}
	if tradeDate == nil {
		return false
	}
	return !s.After(tradeDate.AsTime())
}
