package orderbook

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

// Snapshot serializes the order book state into a protobuf message.
func (ob *OrderBook) Snapshot() (proto.Message, error) {
	snap := &orderbookv1.OrderBookSnapshot{
		Symbol: ob.Symbol,
	}
	for _, order := range ob.Orders {
		snap.Orders = append(snap.Orders, &orderbookv1.OrderSnapshot{
			OrderId:           order.ID,
			Side:              SideToProto(order.Side),
			Price:             order.Price,
			StopPrice:         order.StopPrice,
			Quantity:          order.Quantity,
			RemainingQuantity: order.RemainingQty,
			PlacedAt:          timestamppb.New(order.PlacedAt),
			OrderType:         orderTypeToProto(order.OrderType),
			TimeInForce:       tifToProto(order.TimeInForce),
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
	ob.Bids.Reset()
	ob.Asks.Reset()
	ob.BuyStops.Reset()
	ob.SellStops.Reset()

	for _, os := range snap.Orders {
		order := &Order{
			ID:           os.OrderId,
			Side:         SideFromProto(os.Side),
			Price:        os.Price,
			StopPrice:    os.StopPrice,
			Quantity:     os.Quantity,
			RemainingQty: os.RemainingQuantity,
			PlacedAt:     os.PlacedAt.AsTime(),
			OrderType:    orderTypeFromProto(os.OrderType),
			TimeInForce:  tifFromProto(os.TimeInForce),
		}
		ob.Orders[order.ID] = order

		if order.OrderType == StopMarket || order.OrderType == StopLimit {
			switch order.Side {
			case Buy:
				ob.BuyStops.Insert(order)
			case Sell:
				ob.SellStops.Insert(order)
			}
			continue
		}

		switch order.Side {
		case Buy:
			ob.Bids.Insert(order)
		case Sell:
			ob.Asks.Insert(order)
		}
	}

	return nil
}

// SnapshotInterval returns the number of events between automatic snapshots.
func (ob *OrderBook) SnapshotInterval() int {
	return 5000
}
