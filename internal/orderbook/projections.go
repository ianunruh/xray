package orderbook

import orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"

// TradeReader provides read access to the trade projection.
type TradeReader interface {
	ListTrades(symbol string) []*orderbookv1.Trade
}

// OrderReader provides read access to the order projection.
type OrderReader interface {
	ListOrders(symbol string) []*orderbookv1.OrderSummary
}
