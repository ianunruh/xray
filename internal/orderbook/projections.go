package orderbook

import (
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

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

// StatusReader provides read access to the per-symbol session metadata
// projection backing GetMarketStatus. LULD bands and halt-timer state
// ride alongside on GetLULDStatus.
type StatusReader interface {
	GetStatus(symbol string) (phase orderbookv1.MarketPhase, lastTradePrice, sessionVolume int64)
	GetLULDStatus(symbol string) LULDStatus
}

// CandleReader provides read access to the OHLC candle projection.
type CandleReader interface {
	GetCandles(symbol string, interval orderbookv1.CandleInterval, from, to time.Time) []*orderbookv1.Candle
	GetLatestCandle(symbol string, interval orderbookv1.CandleInterval) *orderbookv1.Candle
}

// SymbolReader provides read access to the list of known symbols.
type SymbolReader interface {
	ListSymbols() []string
}
