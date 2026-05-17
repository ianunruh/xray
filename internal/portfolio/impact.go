package portfolio

import (
	"context"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/margin"
)

// OrderPlan is the subset of order parameters needed to compute its
// margin impact. Independent of any specific proto request so callers
// can build it from a SagaService.Place plan, a PreviewOrderImpact
// request, or a saga aggregate's state.
type OrderPlan struct {
	Symbol       string
	Side         orderbookv1.Side
	PositionSide orderbookv1.PositionSide
	OrderType    orderbookv1.OrderType
	Price        int64
	Quantity     int64
}

// BookEstimator walks the orderbook to estimate market-order fill
// prices. Implemented by *orderbook.OrderBook; passed in so callers
// that already hold a book don't re-load it from the store. Pass nil
// to skip the walk (limit orders use the typed price directly).
type BookEstimator interface {
	EstimateMarketBuyCost(quantity int64) (int64, bool)
	EstimateMarketSellProceeds(quantity int64) (int64, bool)
}

// OrderImpact is the slim, transport-agnostic version of
// PreviewOrderImpactResponse. Server.PreviewOrderImpact wraps this;
// sagasvc and the ordersaga reactor call it directly for submit-time
// + defensive over-leverage gates.
type OrderImpact struct {
	BuyingPowerImpact               int64
	ProjectedEquity                 int64
	ProjectedMaintenanceRequirement int64
	ProjectedMarginExcess           int64
	ProjectedInCall                 bool
	SufficientBuyingPower           bool
	EstimatedFillPrice              int64
	Warnings                        []string
}

// ComputeOrderImpact projects what an order would do to the account's
// margin state if it filled at fillPrice (or the book-walked average
// for market orders). Cash-neutral order kinds (long sell, short
// cover, OCO exit) report BuyingPowerImpact=0; the projected equity
// still moves to reflect realized PnL and mark differences.
//
// Pass book=nil for limit orders or when the caller can't supply a
// book — the fill price will fall back to plan.Price and a warning
// notes the missing estimate.
func ComputeOrderImpact(ctx context.Context, p *Portfolio, marker Marker, book BookEstimator, plan OrderPlan) OrderImpact {
	var impact OrderImpact

	current := ComputeMarginStatus(p, marker)
	currentBP := margin.BuyingPower(current.Equity, current.MaintenanceRequirement)

	fillPrice, fillWarn := estimateFillPrice(plan, book)
	if fillWarn != "" {
		impact.Warnings = append(impact.Warnings, fillWarn)
	}
	impact.EstimatedFillPrice = fillPrice

	mark, _, hasMark := lookupMark(marker, plan.Symbol)
	if !hasMark {
		mark = fillPrice
	}

	qty := plan.Quantity
	isShort := plan.PositionSide == orderbookv1.PositionSide_POSITION_SIDE_SHORT
	switch {
	case plan.Side == orderbookv1.Side_SIDE_BUY && !isShort:
		if fillPrice > 0 {
			impact.BuyingPowerImpact = fillPrice * qty
		}
		impact.ProjectedEquity = current.Equity + qty*(mark-fillPrice)
		impact.ProjectedMaintenanceRequirement = current.MaintenanceRequirement + margin.MaintenanceForLong(mark, qty)
	case plan.Side == orderbookv1.Side_SIDE_SELL && !isShort:
		impact.ProjectedEquity = current.Equity + qty*(fillPrice-mark)
		impact.ProjectedMaintenanceRequirement = current.MaintenanceRequirement - margin.MaintenanceForLong(mark, qty)
	case plan.Side == orderbookv1.Side_SIDE_SELL && isShort:
		if fillPrice > 0 {
			impact.BuyingPowerImpact = margin.CollateralForShortOpen(fillPrice, qty)
		}
		impact.ProjectedEquity = current.Equity + qty*(fillPrice-mark)
		impact.ProjectedMaintenanceRequirement = current.MaintenanceRequirement + margin.MaintenanceRequirement(mark, qty)
	case plan.Side == orderbookv1.Side_SIDE_BUY && isShort:
		short := p.ShortPositions[plan.Symbol]
		if short != nil {
			impact.ProjectedEquity = current.Equity + qty*(short.AvgOpenPrice-fillPrice)
			impact.ProjectedMaintenanceRequirement = current.MaintenanceRequirement - margin.MaintenanceRequirement(mark, qty)
		} else {
			impact.Warnings = append(impact.Warnings, "no open short in this symbol to cover")
			impact.ProjectedEquity = current.Equity
			impact.ProjectedMaintenanceRequirement = current.MaintenanceRequirement
		}
	}

	impact.ProjectedMarginExcess = impact.ProjectedEquity - impact.ProjectedMaintenanceRequirement
	impact.ProjectedInCall = impact.ProjectedMarginExcess < 0
	impact.SufficientBuyingPower = currentBP >= impact.BuyingPowerImpact

	if !impact.SufficientBuyingPower {
		impact.Warnings = append(impact.Warnings,
			"insufficient buying power")
	}
	if impact.ProjectedInCall && !current.InCall {
		impact.Warnings = append(impact.Warnings,
			"this order would put the account in margin call")
	}
	return impact
}

// estimateFillPrice mirrors the server's preview logic. Limit orders
// use the typed price as-is; market orders need a book to walk.
func estimateFillPrice(plan OrderPlan, book BookEstimator) (int64, string) {
	if plan.OrderType != orderbookv1.OrderType_ORDER_TYPE_MARKET {
		return plan.Price, ""
	}
	if book == nil {
		return plan.Price, "market order preview without book estimator — falling back to typed price"
	}
	if plan.Side == orderbookv1.Side_SIDE_BUY {
		cost, ok := book.EstimateMarketBuyCost(plan.Quantity)
		if !ok {
			return 0, "no ask liquidity for market buy"
		}
		return cost / plan.Quantity, ""
	}
	proceeds, ok := book.EstimateMarketSellProceeds(plan.Quantity)
	if !ok {
		return 0, "no bid liquidity for market sell"
	}
	return proceeds / plan.Quantity, ""
}
