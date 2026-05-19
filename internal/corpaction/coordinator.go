package corpaction

import (
	"context"
	"errors"
	"fmt"
	"time"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

// HoldersReader returns accounts holding a symbol. Satisfied by
// portfolio.PgPortfolioProjection.
type HoldersReader interface {
	HoldersOfSymbol(ctx context.Context, symbol string) ([]string, error)
}

// HoldingsReader returns (account, shares) pairs for everyone
// holding a symbol — the dividend record-date snapshot needs share
// counts, not just account IDs. Satisfied by
// portfolio.PgPortfolioProjection.HoldingsForSymbol via a small
// adapter (defined where the coordinator is constructed).
type HoldingsReader interface {
	HoldingsForSymbol(ctx context.Context, symbol string) ([]HolderShares, error)
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
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	holders          HoldersReader
	holdings         HoldingsReader
	orders           OrderLister
	sagas            SagaLister
	cancelOrder      OrderCanceler
	cancelSaga       SagaCanceler
	dividendHolders  DividendHoldersStore
}

func NewCoordinator(
	portfolioHandler *es.Handler[*portfolio.Portfolio],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	holders HoldersReader,
	holdings HoldingsReader,
	orders OrderLister,
	sagas SagaLister,
	cancelOrder OrderCanceler,
	cancelSaga SagaCanceler,
	dividendHolders DividendHoldersStore,
) *Coordinator {
	return &Coordinator{
		portfolioHandler: portfolioHandler,
		orderbookHandler: orderbookHandler,
		holders:          holders,
		holdings:         holdings,
		orders:           orders,
		sagas:            sagas,
		cancelOrder:      cancelOrder,
		cancelSaga:       cancelSaga,
		dividendHolders:  dividendHolders,
	}
}

// ApplyAction dispatches on action type. SPLIT and CASH_DIVIDEND
// are fully wired; SYMBOL_CHANGE returns ErrNotImplemented until
// phase 6 lands.
func (c *Coordinator) ApplyAction(ctx context.Context, action ActionRow) (FanoutCounts, error) {
	switch action.Type {
	case corpactionv1.ActionType_ACTION_TYPE_SPLIT:
		return c.applySplit(ctx, action)
	case corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND:
		return c.applyDividend(ctx, action)
	case corpactionv1.ActionType_ACTION_TYPE_SYMBOL_CHANGE:
		return c.applyRename(ctx, action)
	default:
		return FanoutCounts{}, fmt.Errorf("unknown action type: %v", action.Type)
	}
}

// SnapshotDividendHolders writes the per-action record-date snapshot
// of every (account, shares) pair holding the symbol. Idempotent at
// the store layer.
func (c *Coordinator) SnapshotDividendHolders(ctx context.Context, action ActionRow) (int32, error) {
	rows, err := c.holdings.HoldingsForSymbol(ctx, action.Symbol)
	if err != nil {
		return 0, fmt.Errorf("list holdings: %w", err)
	}
	count, err := c.dividendHolders.SaveSnapshot(ctx, action.ActionID, rows, time.Now())
	if err != nil {
		return 0, fmt.Errorf("save snapshot: %w", err)
	}
	return count, nil
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

// applyRename cancels orders + sagas for the old symbol, migrates
// per-account positional state from old to new, then permanently
// terminates the old orderbook aggregate. Order: cancellations
// first (so holds release while the aggregate still accepts cancel
// commands), then portfolio migration, then orderbook closure.
func (c *Coordinator) applyRename(ctx context.Context, action ActionRow) (FanoutCounts, error) {
	var counts FanoutCounts
	if action.NewSymbol == "" {
		return counts, errors.New("rename action missing new_symbol")
	}

	// 1. Cancel resting orders on the old symbol.
	open := c.orders.OpenOrdersBySymbol(action.Symbol)
	reason := fmt.Sprintf("corporate_action:rename:%s", action.ActionID)
	for _, o := range open {
		if err := c.cancelOrder.CancelOrder(ctx, action.Symbol, o.OrderID, reason); err != nil {
			return counts, fmt.Errorf("cancel order %s: %w", o.OrderID, err)
		}
		counts.Orders++
	}

	// 2. Cancel in-flight sagas on the old symbol.
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

	// 3. Migrate holdings for every long-position holder. Short
	//    positions also migrate via the same aggregate-level rewrite;
	//    holders enumeration walks projection_holdings (long-only)
	//    today — v1 limitation, documented in the plan as a
	//    follow-up.
	accounts, err := c.holders.HoldersOfSymbol(ctx, action.Symbol)
	if err != nil {
		return counts, fmt.Errorf("list holders: %w", err)
	}
	for _, accountID := range accounts {
		cmd := portfolio.MigrateSymbol{
			AccountID: accountID,
			ActionID:  action.ActionID,
			OldSymbol: action.Symbol,
			NewSymbol: action.NewSymbol,
		}
		if err := c.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteMigrateSymbol(p, cmd)
		}); err != nil {
			return counts, fmt.Errorf("migrate symbol for %s: %w", accountID, err)
		}
		counts.Holders++
	}

	// 4. Terminate the old orderbook aggregate. After this, PlaceOrder
	//    against orderbook:<old_symbol> returns ErrSymbolRenamed.
	markCmd := orderbook.MarkRenamed{
		Symbol:    action.Symbol,
		NewSymbol: action.NewSymbol,
		ActionID:  action.ActionID,
	}
	if err := c.orderbookHandler.Handle(ctx, markCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteMarkRenamed(book, markCmd)
	}); err != nil {
		return counts, fmt.Errorf("mark orderbook renamed: %w", err)
	}
	return counts, nil
}

// applyDividend reads the record-date snapshot and credits the
// per-share dividend to every entitled account. The reactor has
// already verified DividendSnapshotted=true on the aggregate, so a
// missing or empty snapshot here is a bug (treat as zero holders —
// the action applies but credits nothing, and the audit trail still
// records that the apply happened).
func (c *Coordinator) applyDividend(ctx context.Context, action ActionRow) (FanoutCounts, error) {
	var counts FanoutCounts
	holders, err := c.dividendHolders.LoadSnapshot(ctx, action.ActionID)
	if err != nil {
		return counts, fmt.Errorf("load dividend snapshot: %w", err)
	}
	for _, h := range holders {
		cmd := portfolio.CreditDividend{
			AccountID:      h.AccountID,
			ActionID:       action.ActionID,
			Symbol:         action.Symbol,
			SharesOfRecord: h.Shares,
			PerShare:       action.DividendPerShare,
		}
		if err := c.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteCreditDividend(p, cmd)
		}); err != nil {
			return counts, fmt.Errorf("credit dividend for %s: %w", h.AccountID, err)
		}
		counts.Holders++
	}
	return counts, nil
}
