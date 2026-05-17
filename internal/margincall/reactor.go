// Package margincall implements the reactor that watches for margin
// breaches and auto-liquidates positions to bring accounts back into
// compliance.
//
// Trigger inputs:
//   - TradeExecuted updates a mark; for each account with an open
//     short in that symbol, recompute margin and act.
//   - OfficialCloseSet (session-end mark) does the same.
//   - ShortOpened / ShortCovered re-check the actor account directly.
//
// Liquidation strategy (v1):
//   - On the "becomes-breached" edge: emit IssueMarginCall + spawn a
//     market BUY-to-cover saga for the largest open short.
//   - On each subsequent trigger while still breached: spawn another
//     liquidation saga (deterministic ID derived from the trigger
//     event), targeting whatever the largest remaining short is.
//   - On the "breach resolved" edge: emit CoverMarginCall.
//
// Saga IDs are deterministic (`liquidation:{account_id}:{trigger_id}`)
// so replays don't double-spawn.
package margincall

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

type Reactor struct {
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	shorts           portfolio.ShortsTracker
	marker           portfolio.Marker
	log              *slog.Logger
}

func NewReactor(
	portfolioHandler *es.Handler[*portfolio.Portfolio],
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga],
	shorts portfolio.ShortsTracker,
	marker portfolio.Marker,
	log *slog.Logger,
) *Reactor {
	return &Reactor{
		portfolioHandler: portfolioHandler,
		orderSagaHandler: orderSagaHandler,
		shorts:           shorts,
		marker:           marker,
		log:              log,
	}
}

func (r *Reactor) HandleEvents(ctx context.Context, events []es.Event) error {
	var errs []error
	for _, evt := range events {
		if err := r.handleOne(ctx, evt); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Reactor) handleOne(ctx context.Context, evt es.Event) error {
	ctx = es.WithCausation(ctx, evt)
	switch data := evt.Data.(type) {
	case *orderbookv1.TradeExecuted:
		return r.onMarkUpdate(ctx, data.Symbol, data.TradeId, data.Price)
	case *orderbookv1.OfficialCloseSet:
		// Treat the official close like a final trade for the session.
		return r.onMarkUpdate(ctx, data.Symbol, "close:"+data.SessionDate+":"+data.Symbol, data.ClosePrice)
	case *portfoliov1.ShortOpened:
		return r.recheckAccount(ctx, data.AccountId, evt.ID, data.Symbol, data.PricePerShare)
	case *portfoliov1.ShortCovered:
		return r.recheckAccount(ctx, data.AccountId, evt.ID, data.Symbol, data.CostPerShare)
	case *portfoliov1.CashDeposited:
		// Deposits can clear an active call; recheck.
		return r.recheckAccount(ctx, data.AccountId, evt.ID, "", 0)
	case *portfoliov1.CashWithdrawn:
		// Withdrawals can push an account into call without a mark
		// move — recheck immediately rather than waiting for a trade.
		return r.recheckAccount(ctx, data.AccountId, evt.ID, "", 0)
	}
	return nil
}

// onMarkUpdate is the high-fanout path: a mark moved, so every account
// with an open short in that symbol needs a recheck. Triggered by
// TradeExecuted and OfficialCloseSet.
func (r *Reactor) onMarkUpdate(ctx context.Context, symbol, triggerID string, markPrice int64) error {
	accounts, err := r.shorts.AccountsWithShort(ctx, symbol)
	if err != nil {
		return fmt.Errorf("lookup shorts in %s: %w", symbol, err)
	}
	if len(accounts) == 0 {
		return nil
	}
	var errs []error
	for _, accountID := range accounts {
		if err := r.recheckAccount(ctx, accountID, triggerID, symbol, markPrice); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Reactor) recheckAccount(ctx context.Context, accountID, triggerID, triggerSymbol string, triggerMark int64) error {
	p, err := r.portfolioHandler.Load(ctx, portfolio.AggregateID(accountID))
	if err != nil {
		return fmt.Errorf("load portfolio %s: %w", accountID, err)
	}

	status := portfolio.ComputeMarginStatus(p, r.marker)

	// State transitions:
	//   1. Not in call, becomes in call -> issue + liquidate
	//   2. In call, becomes not in call -> cover
	//   3. In call and still in call -> spawn another liquidation
	//   4. Not in call, still not in call -> no-op
	hadCall := p.ActiveMarginCall != nil

	switch {
	case !hadCall && status.InCall:
		callID := fmt.Sprintf("call:%s:%s", accountID, triggerID)
		if err := r.issueCall(ctx, accountID, callID, triggerID, triggerSymbol, triggerMark, status); err != nil {
			return err
		}
		return r.spawnLiquidation(ctx, accountID, triggerID, callID, status)

	case hadCall && !status.InCall:
		return r.coverCall(ctx, accountID, status)

	case hadCall && status.InCall:
		// Chained liquidation — only when the prior cover trade
		// actually reduced something. Skip if no short remains
		// (status.LargestShortQty == 0 means we've covered them all
		// but equity is still below requirement; nothing more we can
		// liquidate on our own).
		if status.LargestShortQty == 0 {
			return nil
		}
		return r.spawnLiquidation(ctx, accountID, triggerID, p.ActiveMarginCall.CallID, status)
	}
	return nil
}

func (r *Reactor) issueCall(ctx context.Context, accountID, callID, triggerID, triggerSymbol string, triggerMark int64, status portfolio.MarginStatus) error {
	cmd := portfolio.IssueMarginCall{
		AccountID:                     accountID,
		CallID:                        callID,
		TriggerTradeID:                triggerID,
		TriggerSymbol:                 triggerSymbol,
		MarkPrice:                     triggerMark,
		EquityAtIssue:                 status.Equity,
		MaintenanceRequirementAtIssue: status.MaintenanceRequirement,
	}
	if err := r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteIssueMarginCall(p, cmd)
	}); err != nil {
		r.log.Error("margincall: failed to issue call", "account_id", accountID, "error", err)
		return err
	}
	r.log.Warn("margincall: issued",
		"account_id", accountID, "call_id", callID,
		"equity", status.Equity, "requirement", status.MaintenanceRequirement)
	return nil
}

func (r *Reactor) coverCall(ctx context.Context, accountID string, status portfolio.MarginStatus) error {
	cmd := portfolio.CoverMarginCall{
		AccountID:                     accountID,
		EquityAtCover:                 status.Equity,
		MaintenanceRequirementAtCover: status.MaintenanceRequirement,
	}
	if err := r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCoverMarginCall(p, cmd)
	}); err != nil {
		r.log.Error("margincall: failed to cover call", "account_id", accountID, "error", err)
		return err
	}
	r.log.Info("margincall: covered", "account_id", accountID, "equity", status.Equity)
	return nil
}

// spawnLiquidation issues a market BUY-to-cover saga for the account's
// largest open short. SagaID is deterministic from (account, triggerID)
// so replays produce no-ops. Returns nil if there's nothing to liquidate
// (no open shorts with marks).
func (r *Reactor) spawnLiquidation(ctx context.Context, accountID, triggerID, callID string, status portfolio.MarginStatus) error {
	if status.LargestShortSymbol == "" || status.LargestShortQty == 0 {
		return nil
	}
	sagaID := fmt.Sprintf("liquidation:%s:%s", accountID, triggerID)
	cmd := ordersaga.StartOrderSaga{
		SagaID:       sagaID,
		AccountID:    accountID,
		Symbol:       status.LargestShortSymbol,
		Side:         orderbookv1.Side_SIDE_BUY,
		Quantity:     status.LargestShortQty,
		OrderType:    orderbookv1.OrderType_ORDER_TYPE_MARKET,
		TimeInForce:  orderbookv1.TimeInForce_TIME_IN_FORCE_IOC,
		PositionSide: orderbookv1.PositionSide_POSITION_SIDE_SHORT,
		CauseEventID: callID,
		Initiator:    sagav1.Initiator_INITIATOR_MARGIN_CALL,
	}
	if err := r.orderSagaHandler.Handle(ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	}); err != nil {
		// ErrInvalidState means we already spawned this saga (replay) —
		// idempotent, treat as success.
		if errors.Is(err, ordersaga.ErrInvalidState) {
			return nil
		}
		r.log.Error("margincall: failed to spawn liquidation",
			"account_id", accountID, "saga_id", sagaID, "error", err)
		return err
	}
	r.log.Warn("margincall: liquidation spawned",
		"account_id", accountID, "saga_id", sagaID,
		"symbol", status.LargestShortSymbol, "qty", status.LargestShortQty)
	return nil
}

// Tick is exposed for the periodic reconciler to drive a fresh
// recheck (e.g. catching cases where a mark stagnated past the call).
// Not currently wired in cmd/xray; left as an extension point.
func (r *Reactor) Tick(ctx context.Context, accountID string, now time.Time) error {
	return r.recheckAccount(ctx, accountID, fmt.Sprintf("tick:%d", now.UnixNano()), "", 0)
}
