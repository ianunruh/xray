package orderbook

import (
	"fmt"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const AggregateType = "orderbook"

func AggregateID(symbol string) string {
	return AggregateType + ":" + symbol
}

// OrderBook is the event-sourced aggregate for a single symbol's order book.
type OrderBook struct {
	es.AggregateBase

	Symbol     string
	PriceScale int
	Bids       *priceSide
	Asks       *priceSide
	Orders     map[string]*Order
	BuyStops   *stopSide
	SellStops  *stopSide
}

// NewOrderBook creates a new OrderBook aggregate with the given ID.
func NewOrderBook(id string) *OrderBook {
	ob := &OrderBook{
		PriceScale: 4,
		Bids:       newBidSide(),
		Asks:       newAskSide(),
		Orders:     make(map[string]*Order),
		BuyStops:   newBuyStopSide(),
		SellStops:  newSellStopSide(),
	}
	ob.SetID(id)
	return ob
}

// Apply updates the order book state from a domain event.
func (ob *OrderBook) Apply(evt es.Event) error {
	switch data := evt.Data.(type) {
	case *orderbookv1.OrderPlaced:
		ob.applyOrderPlaced(data)
	case *orderbookv1.TradeExecuted:
		ob.applyTradeExecuted(data)
	case *orderbookv1.OrderCancelled:
		ob.applyOrderCancelled(data)
	case *orderbookv1.StopTriggered:
		ob.applyStopTriggered(data)
	case *orderbookv1.MarketClosed:
		// State changes are handled by the subsequent OrderCancelled events.
	default:
		return fmt.Errorf("unknown event type: %T", evt.Data)
	}
	ob.IncrementVersion()
	return nil
}

func (ob *OrderBook) applyOrderPlaced(data *orderbookv1.OrderPlaced) {
	ob.Symbol = data.Symbol

	order := &Order{
		ID:           data.OrderId,
		AccountID:    data.AccountId,
		Side:         SideFromProto(data.Side),
		Price:        data.Price,
		StopPrice:    data.StopPrice,
		Quantity:     data.Quantity,
		RemainingQty: data.Quantity,
		PlacedAt:     data.PlacedAt.AsTime(),
		OrderType:    OrderTypeFromProto(data.OrderType),
		TimeInForce:  TimeInForceFromProto(data.TimeInForce),
	}

	ob.Orders[order.ID] = order

	if order.OrderType == StopMarket || order.OrderType == StopLimit {
		switch order.Side {
		case Buy:
			ob.BuyStops.Insert(order)
		case Sell:
			ob.SellStops.Insert(order)
		}
		return
	}

	switch order.Side {
	case Buy:
		ob.Bids.Insert(order)
	case Sell:
		ob.Asks.Insert(order)
	}
}

func (ob *OrderBook) applyTradeExecuted(data *orderbookv1.TradeExecuted) {
	buyOrder := ob.Orders[data.BuyOrderId]
	sellOrder := ob.Orders[data.SellOrderId]

	if buyOrder != nil {
		buyOrder.RemainingQty -= data.Quantity
		if buyOrder.RemainingQty <= 0 {
			ob.Bids.Remove(buyOrder)
		}
	}

	if sellOrder != nil {
		sellOrder.RemainingQty -= data.Quantity
		if sellOrder.RemainingQty <= 0 {
			ob.Asks.Remove(sellOrder)
		}
	}
}

func (ob *OrderBook) applyOrderCancelled(data *orderbookv1.OrderCancelled) {
	order, ok := ob.Orders[data.OrderId]
	if !ok {
		return
	}

	if order.OrderType == StopMarket || order.OrderType == StopLimit {
		switch order.Side {
		case Buy:
			ob.BuyStops.Remove(order.ID)
		case Sell:
			ob.SellStops.Remove(order.ID)
		}
	} else {
		switch order.Side {
		case Buy:
			ob.Bids.Remove(order)
		case Sell:
			ob.Asks.Remove(order)
		}
	}

	delete(ob.Orders, order.ID)
}

func (ob *OrderBook) applyStopTriggered(data *orderbookv1.StopTriggered) {
	order, ok := ob.Orders[data.OrderId]
	if !ok {
		return
	}

	switch order.Side {
	case Buy:
		ob.BuyStops.Remove(order.ID)
	case Sell:
		ob.SellStops.Remove(order.ID)
	}

	switch order.OrderType {
	case StopMarket:
		order.OrderType = Market
		order.Price = 0
	case StopLimit:
		order.OrderType = Limit
	}

	switch order.Side {
	case Buy:
		ob.Bids.Insert(order)
	case Sell:
		ob.Asks.Insert(order)
	}
}
