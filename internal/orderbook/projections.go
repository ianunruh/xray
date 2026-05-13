package orderbook

import orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"

// TradeReader provides read access to the trade projection.
type TradeReader interface {
	ListTrades(symbol string) []*orderbookv1.Trade
}

// OrderReader provides read access to the order projection.
type OrderReader interface {
	GetOrder(symbol, orderID string) (*orderbookv1.OrderSummary, bool)
	ListOrders(symbol string) []*orderbookv1.OrderSummary
}

// DepthReader provides read access to the market depth projection.
type DepthReader interface {
	GetDepth(symbol string, depth int32) (bids, asks []*orderbookv1.PriceLevel)
}
