package orderbook

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

// timeToProto maps a Go time to a protobuf Timestamp, preserving the
// "unset" zero value as a nil pointer so consumers can distinguish
// never-set from epoch.
func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// timeFromProto is the reverse of timeToProto.
func timeFromProto(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// Snapshot serializes the order book state into a protobuf message.
func (ob *OrderBook) Snapshot() (proto.Message, error) {
	snap := &orderbookv1.OrderBookSnapshot{
		Symbol:                   ob.Symbol,
		Phase:                    MarketPhaseToProto(ob.Phase),
		SessionVolume:            ob.SessionVolume,
		LuldReferencePrice:       ob.LULDReferencePrice,
		LuldUpperBand:            ob.LULDUpperBand,
		LuldLowerBand:            ob.LULDLowerBand,
		LuldBandBps:              ob.LULDBandBps,
		LuldLimitStateStartedAt:  timeToProto(ob.LULDLimitStateStartedAt),
		LuldHaltDeadline:         timeToProto(ob.LULDHaltDeadline),
		LuldHaltStartedAt:        timeToProto(ob.LULDHaltStartedAt),
		LuldReopenAt:             timeToProto(ob.LULDReopenAt),
		LuldRearmAt:              timeToProto(ob.LULDRearmAt),
	}
	for _, order := range ob.Orders {
		snap.Orders = append(snap.Orders, &orderbookv1.OrderSnapshot{
			OrderId:            order.ID,
			AccountId:          order.AccountID,
			Side:               SideToProto(order.Side),
			Price:              order.Price,
			StopPrice:          order.StopPrice,
			Quantity:           order.Quantity,
			RemainingQuantity:  order.RemainingQty,
			DisplayQuantity:    order.DisplayQty,
			DisplayedRemaining: order.Displayed,
			TrailAmount:        order.TrailAmount,
			TrailOffsetBps:     order.TrailOffsetBps,
			LimitOffset:        order.LimitOffset,
			PlacedAt:           timestamppb.New(order.PlacedAt),
			OrderType:          OrderTypeToProto(order.OrderType),
			TimeInForce:        TimeInForceToProto(order.TimeInForce),
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
	ob.Phase = MarketPhaseFromProto(snap.Phase)
	ob.SessionVolume = snap.SessionVolume
	ob.LULDReferencePrice = snap.LuldReferencePrice
	ob.LULDUpperBand = snap.LuldUpperBand
	ob.LULDLowerBand = snap.LuldLowerBand
	ob.LULDBandBps = snap.LuldBandBps
	ob.LULDLimitStateStartedAt = timeFromProto(snap.LuldLimitStateStartedAt)
	ob.LULDHaltDeadline = timeFromProto(snap.LuldHaltDeadline)
	ob.LULDHaltStartedAt = timeFromProto(snap.LuldHaltStartedAt)
	ob.LULDReopenAt = timeFromProto(snap.LuldReopenAt)
	ob.LULDRearmAt = timeFromProto(snap.LuldRearmAt)
	ob.Orders = make(map[string]*Order, len(snap.Orders))
	ob.Bids.Reset()
	ob.Asks.Reset()
	ob.BuyStops.Reset()
	ob.SellStops.Reset()
	if ob.OpeningBook == nil {
		ob.OpeningBook = newAuctionBook()
	} else {
		ob.OpeningBook.Reset()
	}
	if ob.ClosingBook == nil {
		ob.ClosingBook = newAuctionBook()
	} else {
		ob.ClosingBook.Reset()
	}

	for _, os := range snap.Orders {
		order := &Order{
			ID:             os.OrderId,
			AccountID:      os.AccountId,
			Side:           SideFromProto(os.Side),
			Price:          os.Price,
			StopPrice:      os.StopPrice,
			Quantity:       os.Quantity,
			RemainingQty:   os.RemainingQuantity,
			DisplayQty:     os.DisplayQuantity,
			Displayed:      os.DisplayedRemaining,
			TrailAmount:    os.TrailAmount,
			TrailOffsetBps: os.TrailOffsetBps,
			LimitOffset:    os.LimitOffset,
			PlacedAt:       os.PlacedAt.AsTime(),
			OrderType:      OrderTypeFromProto(os.OrderType),
			TimeInForce:    TimeInForceFromProto(os.TimeInForce),
		}
		ob.Orders[order.ID] = order

		switch {
		case order.TimeInForce == AtOpen:
			ob.OpeningBook.Insert(order)
		case order.TimeInForce == AtClose:
			ob.ClosingBook.Insert(order)
		case order.OrderType.IsStop():
			switch order.Side {
			case Buy:
				ob.BuyStops.Insert(order)
			case Sell:
				ob.SellStops.Insert(order)
			}
		default:
			switch order.Side {
			case Buy:
				ob.Bids.Insert(order)
			case Sell:
				ob.Asks.Insert(order)
			}
		}
	}

	return nil
}

// SnapshotInterval returns the number of events between automatic snapshots.
func (ob *OrderBook) SnapshotInterval() int {
	return 1000
}
