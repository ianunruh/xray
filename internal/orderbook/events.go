package orderbook

import (
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const (
	EventOrderPlaced        = "OrderPlaced"
	EventTradeExecuted      = "TradeExecuted"
	EventOrderCancelled     = "OrderCancelled"
	EventStopTriggered      = "StopTriggered"
	EventMarketClosed       = "MarketClosed"
	EventMarketPhaseChanged = "MarketPhaseChanged"
	EventAuctionUncrossed   = "AuctionUncrossed"
)

func RegisterEvents(r *es.Registry) {
	r.Register(EventOrderPlaced, func() proto.Message { return new(orderbookv1.OrderPlaced) })
	r.Register(EventTradeExecuted, func() proto.Message { return new(orderbookv1.TradeExecuted) })
	r.Register(EventOrderCancelled, func() proto.Message { return new(orderbookv1.OrderCancelled) })
	r.Register(EventStopTriggered, func() proto.Message { return new(orderbookv1.StopTriggered) })
	r.Register(EventMarketClosed, func() proto.Message { return new(orderbookv1.MarketClosed) })
	r.Register(EventMarketPhaseChanged, func() proto.Message { return new(orderbookv1.MarketPhaseChanged) })
	r.Register(EventAuctionUncrossed, func() proto.Message { return new(orderbookv1.AuctionUncrossed) })
}
