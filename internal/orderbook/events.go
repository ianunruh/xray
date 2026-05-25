package orderbook

import (
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const (
	EventOrderPlaced             = "OrderPlaced"
	EventTradeExecuted           = "TradeExecuted"
	EventOrderCancelled          = "OrderCancelled"
	EventStopTriggered           = "StopTriggered"
	EventTrailingStopAdjusted    = "TrailingStopAdjusted"
	EventIcebergSliceReplenished = "IcebergSliceReplenished"
	EventMarketClosed            = "MarketClosed"
	EventMarketPhaseChanged      = "MarketPhaseChanged"
	EventAuctionUncrossed        = "AuctionUncrossed"
	EventOfficialCloseSet        = "OfficialCloseSet"
	EventSymbolRenamed           = "SymbolRenamed"
	EventLULDBandsSet            = "LULDBandsSet"
	EventLULDLimitStateEntered   = "LULDLimitStateEntered"
	EventLULDLimitStateExited    = "LULDLimitStateExited"
	EventTradingHalted           = "TradingHalted"
	EventTradingResumed          = "TradingResumed"
)

func RegisterEvents(r *es.Registry) {
	r.Register(EventOrderPlaced, func() proto.Message { return new(orderbookv1.OrderPlaced) })
	r.Register(EventTradeExecuted, func() proto.Message { return new(orderbookv1.TradeExecuted) })
	r.Register(EventOrderCancelled, func() proto.Message { return new(orderbookv1.OrderCancelled) })
	r.Register(EventStopTriggered, func() proto.Message { return new(orderbookv1.StopTriggered) })
	r.Register(EventTrailingStopAdjusted, func() proto.Message { return new(orderbookv1.TrailingStopAdjusted) })
	r.Register(EventIcebergSliceReplenished, func() proto.Message { return new(orderbookv1.IcebergSliceReplenished) })
	r.Register(EventMarketClosed, func() proto.Message { return new(orderbookv1.MarketClosed) })
	r.Register(EventMarketPhaseChanged, func() proto.Message { return new(orderbookv1.MarketPhaseChanged) })
	r.Register(EventAuctionUncrossed, func() proto.Message { return new(orderbookv1.AuctionUncrossed) })
	r.Register(EventOfficialCloseSet, func() proto.Message { return new(orderbookv1.OfficialCloseSet) })
	r.Register(EventSymbolRenamed, func() proto.Message { return new(orderbookv1.SymbolRenamed) })
	r.Register(EventLULDBandsSet, func() proto.Message { return new(orderbookv1.LULDBandsSet) })
	r.Register(EventLULDLimitStateEntered, func() proto.Message { return new(orderbookv1.LULDLimitStateEntered) })
	r.Register(EventLULDLimitStateExited, func() proto.Message { return new(orderbookv1.LULDLimitStateExited) })
	r.Register(EventTradingHalted, func() proto.Message { return new(orderbookv1.TradingHalted) })
	r.Register(EventTradingResumed, func() proto.Message { return new(orderbookv1.TradingResumed) })
}
