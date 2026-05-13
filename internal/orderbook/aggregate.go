package orderbook

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// OrderBook is the event-sourced aggregate for a single symbol's order book.
type OrderBook struct {
	es.AggregateBase

	Symbol     string
	PriceScale int
	Bids       []*Order // highest price first, then earliest time
	Asks       []*Order // lowest price first, then earliest time
	Orders     map[string]*Order
}

// NewOrderBook creates a new OrderBook aggregate with the given ID.
func NewOrderBook(id string) *OrderBook {
	ob := &OrderBook{
		PriceScale: 4,
		Orders:     make(map[string]*Order),
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
		Side:         sideFromProto(data.Side),
		Price:        data.Price,
		Quantity:     data.Quantity,
		RemainingQty: data.Quantity,
		PlacedAt:     data.PlacedAt.AsTime(),
	}

	ob.Orders[order.ID] = order

	switch order.Side {
	case Buy:
		ob.Bids = insertSorted(ob.Bids, order, bidLess)
	case Sell:
		ob.Asks = insertSorted(ob.Asks, order, askLess)
	}
}

func (ob *OrderBook) applyTradeExecuted(data *orderbookv1.TradeExecuted) {
	buyOrder := ob.Orders[data.BuyOrderId]
	sellOrder := ob.Orders[data.SellOrderId]

	if buyOrder != nil {
		buyOrder.RemainingQty -= data.Quantity
		if buyOrder.RemainingQty <= 0 {
			ob.removeBid(buyOrder.ID)
		}
	}

	if sellOrder != nil {
		sellOrder.RemainingQty -= data.Quantity
		if sellOrder.RemainingQty <= 0 {
			ob.removeAsk(sellOrder.ID)
		}
	}
}

func (ob *OrderBook) applyOrderCancelled(data *orderbookv1.OrderCancelled) {
	order, ok := ob.Orders[data.OrderId]
	if !ok {
		return
	}

	switch order.Side {
	case Buy:
		ob.removeBid(order.ID)
	case Sell:
		ob.removeAsk(order.ID)
	}

	delete(ob.Orders, order.ID)
}

func (ob *OrderBook) removeBid(id string) {
	for i, o := range ob.Bids {
		if o.ID == id {
			ob.Bids = append(ob.Bids[:i], ob.Bids[i+1:]...)
			return
		}
	}
}

func (ob *OrderBook) removeAsk(id string) {
	for i, o := range ob.Asks {
		if o.ID == id {
			ob.Asks = append(ob.Asks[:i], ob.Asks[i+1:]...)
			return
		}
	}
}

// bidLess returns true if a should appear before b in the bids slice
// (highest price first, then earliest time).
func bidLess(a, b *Order) bool {
	if a.Price != b.Price {
		return a.Price > b.Price
	}
	return a.PlacedAt.Before(b.PlacedAt)
}

// askLess returns true if a should appear before b in the asks slice
// (lowest price first, then earliest time).
func askLess(a, b *Order) bool {
	if a.Price != b.Price {
		return a.Price < b.Price
	}
	return a.PlacedAt.Before(b.PlacedAt)
}

// insertSorted inserts order into the slice at the position determined by
// binary search using the provided less function. O(log n) search + O(n) shift.
func insertSorted(orders []*Order, order *Order, less func(a, b *Order) bool) []*Order {
	i := sort.Search(len(orders), func(i int) bool {
		return !less(orders[i], order)
	})
	orders = append(orders, nil)
	copy(orders[i+1:], orders[i:])
	orders[i] = order
	return orders
}

// Snapshot serializes the order book state into a protobuf message.
func (ob *OrderBook) Snapshot() (proto.Message, error) {
	snap := &orderbookv1.OrderBookSnapshot{
		Symbol: ob.Symbol,
	}
	for _, order := range ob.Orders {
		snap.Orders = append(snap.Orders, &orderbookv1.OrderSnapshot{
			OrderId:           order.ID,
			Side:              sideToProto(order.Side),
			Price:             order.Price,
			Quantity:          order.Quantity,
			RemainingQuantity: order.RemainingQty,
			PlacedAt:          timestamppb.New(order.PlacedAt),
		})
	}
	return snap, nil
}

// RestoreSnapshot rebuilds the order book from a snapshot protobuf message.
func (ob *OrderBook) RestoreSnapshot(msg proto.Message) error {
	snap, ok := msg.(*orderbookv1.OrderBookSnapshot)
	if !ok {
		return fmt.Errorf("expected *OrderBookSnapshot, got %T", msg)
	}

	ob.Symbol = snap.Symbol
	ob.Orders = make(map[string]*Order, len(snap.Orders))
	ob.Bids = nil
	ob.Asks = nil

	for _, os := range snap.Orders {
		order := &Order{
			ID:           os.OrderId,
			Side:         sideFromProto(os.Side),
			Price:        os.Price,
			Quantity:     os.Quantity,
			RemainingQty: os.RemainingQuantity,
			PlacedAt:     os.PlacedAt.AsTime(),
		}
		ob.Orders[order.ID] = order

		switch order.Side {
		case Buy:
			ob.Bids = insertSorted(ob.Bids, order, bidLess)
		case Sell:
			ob.Asks = insertSorted(ob.Asks, order, askLess)
		}
	}

	return nil
}

// SnapshotInterval returns the number of events between automatic snapshots.
func (ob *OrderBook) SnapshotInterval() int {
	return 100
}

func sideFromProto(s orderbookv1.Side) Side {
	switch s {
	case orderbookv1.Side_SIDE_BUY:
		return Buy
	case orderbookv1.Side_SIDE_SELL:
		return Sell
	default:
		return 0
	}
}

func sideToProto(s Side) orderbookv1.Side {
	switch s {
	case Buy:
		return orderbookv1.Side_SIDE_BUY
	case Sell:
		return orderbookv1.Side_SIDE_SELL
	default:
		return orderbookv1.Side_SIDE_UNSPECIFIED
	}
}
