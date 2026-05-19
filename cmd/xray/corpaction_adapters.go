package main

import (
	"context"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/corpaction"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/internal/sagasvc"
	"github.com/ianunruh/xray/pkg/es"
)

// Adapters wrapping concrete projection/handler types to satisfy the
// narrow corpaction.* interfaces. Live in cmd/xray rather than
// internal/corpaction so the package itself stays free of the broader
// dependency graph.

// holdingsAdapter wraps PgPortfolioProjection so HoldingsForSymbol
// returns corpaction.HolderShares.
type holdingsAdapter struct {
	proj *portfolio.PgPortfolioProjection
}

func (a holdingsAdapter) HoldingsForSymbol(ctx context.Context, symbol string) ([]corpaction.HolderShares, error) {
	rows, err := a.proj.HoldingsForSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	out := make([]corpaction.HolderShares, 0, len(rows))
	for _, r := range rows {
		out = append(out, corpaction.HolderShares{AccountID: r.AccountID, Shares: r.Shares})
	}
	return out, nil
}

// orderListerAdapter wraps PgOrderProjection.ListOrders to return
// the corpaction.OpenOrder shape.
type orderListerAdapter struct {
	proj *orderbook.PgOrderProjection
}

func (a orderListerAdapter) OpenOrdersBySymbol(symbol string) []corpaction.OpenOrder {
	summaries := a.proj.ListOrders(symbol)
	out := make([]corpaction.OpenOrder, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, corpaction.OpenOrder{OrderID: s.OrderId, Symbol: s.Symbol})
	}
	return out
}

// sagaListerAdapter wraps sagasvc.PgProjection.List, filtering for
// active sagas of any kind in the symbol.
type sagaListerAdapter struct {
	proj *sagasvc.PgProjection
}

func (a sagaListerAdapter) ActiveSagasBySymbol(ctx context.Context, symbol string) ([]corpaction.SagaInfo, error) {
	rows, err := a.proj.List(ctx, "", symbol, sagav1.SagaKind_SAGA_KIND_UNSPECIFIED, sagav1.SagaStatus_SAGA_STATUS_ACTIVE)
	if err != nil {
		return nil, err
	}
	out := make([]corpaction.SagaInfo, 0, len(rows))
	for _, r := range rows {
		out = append(out, corpaction.SagaInfo{SagaID: r.SagaID})
	}
	return out, nil
}

// orderCancelerAdapter wraps the orderbook handler in a small
// helper that issues CancelOrder with a structured reason.
// Already-gone errors are swallowed (the order may have filled or
// cancelled between projection read and command issue — benign).
type orderCancelerAdapter struct {
	handler *es.Handler[*orderbook.OrderBook]
}

func (a orderCancelerAdapter) CancelOrder(ctx context.Context, symbol, orderID, reason string) error {
	cmd := orderbook.CancelOrder{Symbol: symbol, OrderID: orderID, Reason: reason}
	if err := a.handler.Handle(ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cmd)
	}); err != nil {
		// Already-gone is a benign race; the corpaction reactor
		// counts only successful cancellations and that's fine.
		if isBenignCancelError(err) {
			return nil
		}
		return err
	}
	return nil
}

func isBenignCancelError(err error) bool {
	// Mirror sagasvc.cancelOrderbookOrder's tolerance.
	for _, sentinel := range []error{orderbook.ErrOrderNotFound, orderbook.ErrNoRemainingQty} {
		if errorsIs(err, sentinel) {
			return true
		}
	}
	return false
}

// errorsIs is a tiny wrapper so the import block stays in
// adapters.go and the sentinel check above reads naturally.
func errorsIs(err, target error) bool {
	type causer interface{ Unwrap() error }
	for err != nil {
		if err == target {
			return true
		}
		if c, ok := err.(causer); ok {
			err = c.Unwrap()
		} else {
			break
		}
	}
	return false
}

// Compile-time interface assertions.
var (
	_ corpaction.HoldingsReader = holdingsAdapter{}
	_ corpaction.OrderLister    = orderListerAdapter{}
	_ corpaction.SagaLister     = sagaListerAdapter{}
	_ corpaction.OrderCanceler  = orderCancelerAdapter{}
)

// Reference imports kept so go vet doesn't flag them.
var _ = corpactionv1.ActionType_ACTION_TYPE_SPLIT
