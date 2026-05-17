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
	Phase      MarketPhase
	Bids       *priceSide
	Asks       *priceSide
	Orders     map[string]*Order
	BuyStops   *stopSide
	SellStops  *stopSide
	// OCOGroups maps an OCO group ID to the set of order IDs in it.
	// Used by the matching engine to cancel siblings when any member
	// of the group trades.
	OCOGroups map[string]map[string]struct{}
	// OpeningBook holds AT_OPEN orders (MOO/LOO). Kept separate from
	// the continuous Bids/Asks so depth streams stay clean — the uncross
	// algorithm merges them in at clearing time.
	OpeningBook *auctionBook
	// ClosingBook holds AT_CLOSE orders (MOC/LOC) staged for the next
	// closing uncross. AT_CLOSE is accepted while CONTINUOUS or
	// CLOSING_AUCTION; orders may sit here for a while before clearing.
	ClosingBook *auctionBook
	// LastTradePrice tracks the most recent continuous-trade print; the
	// uncross algorithm uses it as the tie-break reference when the
	// matched range is balanced and brackets the prior print.
	LastTradePrice int64
}

// NewOrderBook creates a new OrderBook aggregate with the given ID.
func NewOrderBook(id string) *OrderBook {
	ob := &OrderBook{
		PriceScale:  4,
		Bids:        newBidSide(),
		Asks:        newAskSide(),
		Orders:      make(map[string]*Order),
		BuyStops:    newBuyStopSide(),
		SellStops:   newSellStopSide(),
		OCOGroups:   make(map[string]map[string]struct{}),
		OpeningBook: newAuctionBook(),
		ClosingBook: newAuctionBook(),
	}
	ob.SetID(id)
	return ob
}

// EstimateMarketBuyCost walks the ask side in price-time priority and sums
// the cost of buying `quantity` shares against current resting liquidity.
// If depth is insufficient, the remainder is extrapolated at the deepest
// observed ask price. Returns (0, false) if there is no ask liquidity.
func (ob *OrderBook) EstimateMarketBuyCost(quantity int64) (int64, bool) {
	if quantity <= 0 {
		return 0, false
	}
	var cost, lastPrice int64
	remaining := quantity
	for order := range ob.Asks.All() {
		if remaining <= 0 {
			break
		}
		take := min(order.RemainingQty, remaining)
		cost += take * order.Price
		remaining -= take
		lastPrice = order.Price
	}
	if lastPrice == 0 {
		return 0, false
	}
	if remaining > 0 {
		cost += remaining * lastPrice
	}
	return cost, true
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
	case *orderbookv1.MarketPhaseChanged:
		ob.Phase = MarketPhaseFromProto(data.Phase)
	case *orderbookv1.AuctionUncrossed:
		// Header event for an uncross batch — fully described by the
		// TradeExecuted events that follow it, plus the subsequent
		// MarketPhaseChanged. No aggregate state to mutate here.
	case *orderbookv1.OfficialCloseSet:
		// Pure projection event — the daily_close projection consumes
		// it. The aggregate has no state to maintain for closes.
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
		OCOGroupID:   data.OcoGroupId,
	}

	ob.Orders[order.ID] = order
	if order.OCOGroupID != "" {
		group := ob.OCOGroups[order.OCOGroupID]
		if group == nil {
			group = make(map[string]struct{})
			ob.OCOGroups[order.OCOGroupID] = group
		}
		group[order.ID] = struct{}{}
	}

	// AT_OPEN/AT_CLOSE route to the auction book partition; they never
	// participate in continuous matching.
	if order.TimeInForce == AtOpen {
		ob.OpeningBook.Insert(order)
		return
	}
	if order.TimeInForce == AtClose {
		ob.ClosingBook.Insert(order)
		return
	}

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

	ob.LastTradePrice = data.Price
}

func (ob *OrderBook) applyOrderCancelled(data *orderbookv1.OrderCancelled) {
	order, ok := ob.Orders[data.OrderId]
	if !ok {
		return
	}

	switch {
	case order.TimeInForce == AtOpen:
		ob.OpeningBook.Remove(order.ID, order.Side)
	case order.TimeInForce == AtClose:
		ob.ClosingBook.Remove(order.ID, order.Side)
	case order.OrderType == StopMarket || order.OrderType == StopLimit:
		switch order.Side {
		case Buy:
			ob.BuyStops.Remove(order.ID)
		case Sell:
			ob.SellStops.Remove(order.ID)
		}
	default:
		switch order.Side {
		case Buy:
			ob.Bids.Remove(order)
		case Sell:
			ob.Asks.Remove(order)
		}
	}

	if order.OCOGroupID != "" {
		if group := ob.OCOGroups[order.OCOGroupID]; group != nil {
			delete(group, order.ID)
			if len(group) == 0 {
				delete(ob.OCOGroups, order.OCOGroupID)
			}
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
