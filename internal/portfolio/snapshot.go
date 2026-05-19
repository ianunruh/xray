package portfolio

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
)

// Snapshot serializes the portfolio state into a protobuf message.
func (p *Portfolio) Snapshot() (proto.Message, error) {
	snap := &portfoliov1.PortfolioSnapshot{
		AccountId:               p.AccountID,
		CashBalance:             p.CashBalance,
		CashHeld:                p.CashHeld,
		Holdings:                make(map[string]*portfoliov1.HoldingSnapshot, len(p.Holdings)),
		HoldsBySaga:             make(map[string]int64, len(p.HoldsBySaga)),
		SharesHeld:              make(map[string]int64, len(p.SharesHeld)),
		ShareHoldsBySaga:        make(map[string]*portfoliov1.ShareHoldSnapshot, len(p.ShareHoldsBySaga)),
		SettledTrades:           make(map[string]*portfoliov1.SettledTradeSet, len(p.SettledTrades)),
		ShortPositions:          make(map[string]*portfoliov1.ShortPositionSnapshot, len(p.ShortPositions)),
		ProceedsPool:            p.ProceedsPool,
		CollateralPool:          p.CollateralPool,
		CollateralHeldBySaga:    make(map[string]*portfoliov1.CollateralHoldSnapshot, len(p.CollateralHeldBySaga)),
		ShortCoversHeld:         make(map[string]int64, len(p.ShortCoversHeld)),
		ShortCoverHoldsBySaga:   make(map[string]*portfoliov1.ShareHoldSnapshot, len(p.ShortCoverHoldsBySaga)),
	}

	for sym, h := range p.Holdings {
		snap.Holdings[sym] = &portfoliov1.HoldingSnapshot{
			Quantity:  h.Quantity,
			TotalCost: h.TotalCost,
		}
	}
	for sagaID, amt := range p.HoldsBySaga {
		snap.HoldsBySaga[sagaID] = amt
	}
	for sym, qty := range p.SharesHeld {
		snap.SharesHeld[sym] = qty
	}
	for sagaID, sh := range p.ShareHoldsBySaga {
		snap.ShareHoldsBySaga[sagaID] = &portfoliov1.ShareHoldSnapshot{
			Symbol:   sh.Symbol,
			Quantity: sh.Quantity,
		}
	}
	for sagaID, set := range p.SettledTrades {
		ids := make([]string, 0, len(set))
		for tid := range set {
			ids = append(ids, tid)
		}
		snap.SettledTrades[sagaID] = &portfoliov1.SettledTradeSet{TradeIds: ids}
	}
	for sym, s := range p.ShortPositions {
		snap.ShortPositions[sym] = &portfoliov1.ShortPositionSnapshot{
			Quantity:       s.Quantity,
			ProceedsHeld:   s.ProceedsHeld,
			CollateralHeld: s.CollateralHeld,
			AvgOpenPrice:   s.AvgOpenPrice,
		}
	}
	for sagaID, c := range p.CollateralHeldBySaga {
		snap.CollateralHeldBySaga[sagaID] = &portfoliov1.CollateralHoldSnapshot{
			Symbol:   c.Symbol,
			Quantity: c.Quantity,
			Amount:   c.Amount,
		}
	}
	for sym, qty := range p.ShortCoversHeld {
		snap.ShortCoversHeld[sym] = qty
	}
	for sagaID, sh := range p.ShortCoverHoldsBySaga {
		snap.ShortCoverHoldsBySaga[sagaID] = &portfoliov1.ShareHoldSnapshot{
			Symbol:   sh.Symbol,
			Quantity: sh.Quantity,
		}
	}
	if p.ActiveMarginCall != nil {
		mc := &portfoliov1.MarginCallSnapshot{
			CallId:             p.ActiveMarginCall.CallID,
			TriggerTradeId:     p.ActiveMarginCall.TriggerTradeID,
			TriggerSymbol:      p.ActiveMarginCall.TriggerSymbol,
			MarkPrice:          p.ActiveMarginCall.MarkPrice,
			EquityAtIssue:      p.ActiveMarginCall.EquityAtIssue,
			RequirementAtIssue: p.ActiveMarginCall.RequirementAtIssue,
			IssuedAt:           timestamppb.New(p.ActiveMarginCall.IssuedAt),
		}
		if !p.ActiveMarginCall.GraceExpiresAt.IsZero() {
			mc.GraceExpiresAt = timestamppb.New(p.ActiveMarginCall.GraceExpiresAt)
		}
		snap.ActiveMarginCall = mc
	}
	if !p.LastAccruedAt.IsZero() {
		snap.LastAccruedAt = timestamppb.New(p.LastAccruedAt)
	}
	snap.SettledCash = p.SettledCash
	for _, leg := range p.PendingLegs {
		ls := &portfoliov1.PendingLegSnapshot{
			TradeId:     leg.TradeID,
			OrderSagaId: leg.OrderSagaID,
			Kind:        leg.Kind,
			Symbol:      leg.Symbol,
			CashAmount:  leg.CashAmount,
			Quantity:    leg.Quantity,
		}
		if !leg.SettlesAt.IsZero() {
			ls.SettlesAt = timestamppb.New(leg.SettlesAt)
		}
		if !leg.EmittedAt.IsZero() {
			ls.EmittedAt = timestamppb.New(leg.EmittedAt)
		}
		snap.PendingLegs = append(snap.PendingLegs, ls)
	}
	if len(p.PendingShareCredits) > 0 {
		snap.PendingShareCredits = make(map[string]int64, len(p.PendingShareCredits))
		for sym, qty := range p.PendingShareCredits {
			snap.PendingShareCredits[sym] = qty
		}
	}

	return snap, nil
}

// RestoreSnapshot rebuilds the portfolio from a snapshot protobuf message.
func (p *Portfolio) RestoreSnapshot(msg proto.Message) error {
	snap, ok := msg.(*portfoliov1.PortfolioSnapshot)
	if !ok {
		return fmt.Errorf("expected *PortfolioSnapshot, got %T", msg)
	}

	p.AccountID = snap.AccountId
	p.CashBalance = snap.CashBalance
	p.CashHeld = snap.CashHeld
	p.ProceedsPool = snap.ProceedsPool
	p.CollateralPool = snap.CollateralPool

	p.Holdings = make(map[string]*Holding, len(snap.Holdings))
	for sym, h := range snap.Holdings {
		p.Holdings[sym] = &Holding{Quantity: h.Quantity, TotalCost: h.TotalCost}
	}
	p.HoldsBySaga = make(map[string]int64, len(snap.HoldsBySaga))
	for sagaID, amt := range snap.HoldsBySaga {
		p.HoldsBySaga[sagaID] = amt
	}
	p.SharesHeld = make(map[string]int64, len(snap.SharesHeld))
	for sym, qty := range snap.SharesHeld {
		p.SharesHeld[sym] = qty
	}
	p.ShareHoldsBySaga = make(map[string]*ShareHold, len(snap.ShareHoldsBySaga))
	for sagaID, sh := range snap.ShareHoldsBySaga {
		p.ShareHoldsBySaga[sagaID] = &ShareHold{Symbol: sh.Symbol, Quantity: sh.Quantity}
	}
	p.SettledTrades = make(map[string]map[string]struct{}, len(snap.SettledTrades))
	for sagaID, set := range snap.SettledTrades {
		tids := make(map[string]struct{}, len(set.TradeIds))
		for _, tid := range set.TradeIds {
			tids[tid] = struct{}{}
		}
		p.SettledTrades[sagaID] = tids
	}
	p.ShortPositions = make(map[string]*ShortPosition, len(snap.ShortPositions))
	for sym, s := range snap.ShortPositions {
		p.ShortPositions[sym] = &ShortPosition{
			Quantity:       s.Quantity,
			ProceedsHeld:   s.ProceedsHeld,
			CollateralHeld: s.CollateralHeld,
			AvgOpenPrice:   s.AvgOpenPrice,
		}
	}
	p.CollateralHeldBySaga = make(map[string]*CollateralHold, len(snap.CollateralHeldBySaga))
	for sagaID, c := range snap.CollateralHeldBySaga {
		p.CollateralHeldBySaga[sagaID] = &CollateralHold{
			Symbol:   c.Symbol,
			Quantity: c.Quantity,
			Amount:   c.Amount,
		}
	}
	p.ShortCoversHeld = make(map[string]int64, len(snap.ShortCoversHeld))
	for sym, qty := range snap.ShortCoversHeld {
		p.ShortCoversHeld[sym] = qty
	}
	p.ShortCoverHoldsBySaga = make(map[string]*ShareHold, len(snap.ShortCoverHoldsBySaga))
	for sagaID, sh := range snap.ShortCoverHoldsBySaga {
		p.ShortCoverHoldsBySaga[sagaID] = &ShareHold{Symbol: sh.Symbol, Quantity: sh.Quantity}
	}

	p.ActiveMarginCall = nil
	if snap.ActiveMarginCall != nil {
		mc := &MarginCall{
			CallID:             snap.ActiveMarginCall.CallId,
			TriggerTradeID:     snap.ActiveMarginCall.TriggerTradeId,
			TriggerSymbol:      snap.ActiveMarginCall.TriggerSymbol,
			MarkPrice:          snap.ActiveMarginCall.MarkPrice,
			EquityAtIssue:      snap.ActiveMarginCall.EquityAtIssue,
			RequirementAtIssue: snap.ActiveMarginCall.RequirementAtIssue,
		}
		if snap.ActiveMarginCall.IssuedAt != nil {
			mc.IssuedAt = snap.ActiveMarginCall.IssuedAt.AsTime()
		}
		if snap.ActiveMarginCall.GraceExpiresAt != nil {
			mc.GraceExpiresAt = snap.ActiveMarginCall.GraceExpiresAt.AsTime()
		}
		p.ActiveMarginCall = mc
	}

	if snap.LastAccruedAt != nil {
		p.LastAccruedAt = snap.LastAccruedAt.AsTime()
	}

	p.SettledCash = snap.SettledCash
	// Mark the lazy seed as already done so Apply() doesn't clobber
	// the snapshot's SettledCash on the next event. Snapshots written
	// before T+1 settlement have SettledCash=0; Apply() will skip the
	// seed for them too and they'll diverge from CashBalance on the
	// first event applied. That's the intended migration story —
	// existing snapshots get a one-time "settle everything older
	// than this snapshot" effect, equivalent to flipping the toggle
	// on at snapshot time.
	p.settledCashSeeded = true
	p.PendingLegs = make(map[PendingLegKey]*PendingLeg, len(snap.PendingLegs))
	for _, ls := range snap.PendingLegs {
		leg := &PendingLeg{
			TradeID:     ls.TradeId,
			OrderSagaID: ls.OrderSagaId,
			Kind:        ls.Kind,
			Symbol:      ls.Symbol,
			CashAmount:  ls.CashAmount,
			Quantity:    ls.Quantity,
		}
		if ls.SettlesAt != nil {
			leg.SettlesAt = ls.SettlesAt.AsTime()
		}
		if ls.EmittedAt != nil {
			leg.EmittedAt = ls.EmittedAt.AsTime()
		}
		p.PendingLegs[PendingLegKey{TradeID: ls.TradeId, Kind: ls.Kind}] = leg
	}
	p.PendingShareCredits = make(map[string]int64, len(snap.PendingShareCredits))
	for sym, qty := range snap.PendingShareCredits {
		p.PendingShareCredits[sym] = qty
	}

	return nil
}

// SnapshotInterval returns the number of events between automatic snapshots.
// Portfolios get a lot of small events (each fill is a settle + per-fee), so
// the interval is tighter than the orderbook's.
func (p *Portfolio) SnapshotInterval() int {
	return 1000
}
