package corpaction

import (
	"context"
	"errors"
	"fmt"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

// HoldersReader returns accounts holding a symbol. Satisfied by
// portfolio.PgPortfolioProjection.
type HoldersReader interface {
	HoldersOfSymbol(ctx context.Context, symbol string) ([]string, error)
}

// OpenOrder is the minimal shape Coordinator needs for resting-order
// cancellation. Avoids depending on orderbookv1 / orderbook types
// from this package's public interface.
type OpenOrder struct {
	OrderID string
	Symbol  string
}

// OrderLister enumerates resting orders for a symbol. Satisfied by
// a closure around orderbook.PgOrderProjection.ListOrders.
type OrderLister interface {
	OpenOrdersBySymbol(symbol string) []OpenOrder
}

// SagaInfo is the minimal shape Coordinator needs for saga
// cancellation — just enough to identify the saga for the canceler.
type SagaInfo struct {
	SagaID string
}

// SagaLister enumerates in-flight (status=ACTIVE) sagas for a
// symbol. Satisfied by a closure around sagasvc.PgProjection.List.
type SagaLister interface {
	ActiveSagasBySymbol(ctx context.Context, symbol string) ([]SagaInfo, error)
}

// OrderCanceler cancels one resting orderbook order. Satisfied by a
// closure around the orderbook handler.
type OrderCanceler interface {
	CancelOrder(ctx context.Context, symbol, orderID, reason string) error
}

// SagaCanceler cancels one in-flight saga. Satisfied by
// sagasvc.Server.CancelByID.
type SagaCanceler interface {
	CancelByID(ctx context.Context, sagaID string) error
}

// Coordinator is the concrete Applier that fans corporate actions
// out across portfolios, the orderbook, and in-flight sagas. Each
// dependency is a narrow interface so the dispatcher can be
// unit-tested with fakes.
type Coordinator struct {
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	holders          HoldersReader
	orders           OrderLister
	sagas            SagaLister
	cancelOrder      OrderCanceler
	cancelSaga       SagaCanceler
}

func NewCoordinator(
	portfolioHandler *es.Handler[*portfolio.Portfolio],
	holders HoldersReader,
	orders OrderLister,
	sagas SagaLister,
	cancelOrder OrderCanceler,
	cancelSaga SagaCanceler,
) *Coordinator {
	return &Coordinator{
		portfolioHandler: portfolioHandler,
		holders:          holders,
		orders:           orders,
		sagas:            sagas,
		cancelOrder:      cancelOrder,
		cancelSaga:       cancelSaga,
	}
}

// ApplyAction dispatches on action type. Currently SPLIT is the only
// fully-implemented branch (phase 4); CASH_DIVIDEND and SYMBOL_CHANGE
// return ErrNotImplemented so the reactor leaves them Declared until
// later phases land.
func (c *Coordinator) ApplyAction(ctx context.Context, action ActionRow) (FanoutCounts, error) {
	switch action.Type {
	case corpactionv1.ActionType_ACTION_TYPE_SPLIT:
		return c.applySplit(ctx, action)
	case corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND:
		return FanoutCounts{}, ErrNotImplemented
	case corpactionv1.ActionType_ACTION_TYPE_SYMBOL_CHANGE:
		return FanoutCounts{}, ErrNotImplemented
	default:
		return FanoutCounts{}, fmt.Errorf("unknown action type: %v", action.Type)
	}
}

// SnapshotDividendHolders is the phase-5 entry point. For now it's a
// no-op so the reactor's tick path doesn't error on dividends that
// reach record_date.
func (c *Coordinator) SnapshotDividendHolders(_ context.Context, _ ActionRow) (int32, error) {
	return 0, nil
}

// ErrNotImplemented surfaces from action types that aren't yet wired.
// The reactor logs it and leaves the action Declared — when the
// later phase lands and the binary restarts, the next tick picks
// the action up again.
var ErrNotImplemented = errors.New("corpaction: action type not yet implemented")

func (c *Coordinator) applySplit(ctx context.Context, action ActionRow) (FanoutCounts, error) {
	var counts FanoutCounts

	// 1. Cancel resting orders on the symbol. Order-level cancel
	//    failures are recoverable (already-gone is fine); we count
	//    only successful cancels so the recorded total reflects
	//    reality.
	open := c.orders.OpenOrdersBySymbol(action.Symbol)
	reason := fmt.Sprintf("corporate_action:split:%s", action.ActionID)
	for _, o := range open {
		if err := c.cancelOrder.CancelOrder(ctx, action.Symbol, o.OrderID, reason); err != nil {
			return counts, fmt.Errorf("cancel order %s: %w", o.OrderID, err)
		}
		counts.Orders++
	}

	// 2. Cancel in-flight sagas. Same rationale — we cancel rather
	//    than adjust because TWAP slice math, bracket trigger
	//    prices, and OCO stops all need careful per-kind rewrites
	//    and the alternative is "user re-submits post-split."
	active, err := c.sagas.ActiveSagasBySymbol(ctx, action.Symbol)
	if err != nil {
		return counts, fmt.Errorf("list active sagas: %w", err)
	}
	for _, s := range active {
		if err := c.cancelSaga.CancelByID(ctx, s.SagaID); err != nil {
			return counts, fmt.Errorf("cancel saga %s: %w", s.SagaID, err)
		}
		counts.Sagas++
	}

	// 3. Adjust holdings for every account holding the symbol.
	//    Idempotent via AppliedActions on the portfolio; per-account
	//    failures abort the fan-out so the action stays Declared
	//    and the reactor retries next tick.
	accounts, err := c.holders.HoldersOfSymbol(ctx, action.Symbol)
	if err != nil {
		return counts, fmt.Errorf("list holders: %w", err)
	}
	for _, accountID := range accounts {
		cmd := portfolio.AdjustHolding{
			AccountID:   accountID,
			ActionID:    action.ActionID,
			Symbol:      action.Symbol,
			Numerator:   action.SplitNumerator,
			Denominator: action.SplitDenominator,
		}
		if err := c.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteAdjustHolding(p, cmd)
		}); err != nil {
			return counts, fmt.Errorf("adjust holding for %s: %w", accountID, err)
		}
		counts.Holders++
	}
	return counts, nil
}
