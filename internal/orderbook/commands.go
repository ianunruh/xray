package orderbook

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// PlaceOrder is a command to place a new limit order on the book.
type PlaceOrder struct {
	Symbol   string
	Side     Side
	Price    int64
	Quantity int64
}

func (c PlaceOrder) AggregateID() string {
	return "orderbook:" + c.Symbol
}

// CancelOrder is a command to cancel an existing order.
type CancelOrder struct {
	Symbol  string
	OrderID string
}

func (c CancelOrder) AggregateID() string {
	return "orderbook:" + c.Symbol
}

// ExecutePlaceOrder produces events for placing and matching a new order.
func ExecutePlaceOrder(book *OrderBook, cmd PlaceOrder) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, fmt.Errorf("quantity must be positive")
	}
	if cmd.Price <= 0 {
		return nil, fmt.Errorf("price must be positive")
	}

	now := time.Now()
	orderID := uuid.New().String()

	placedEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        "OrderPlaced",
		Timestamp:   now,
		Data: &orderbookv1.OrderPlaced{
			OrderId:  orderID,
			Symbol:   cmd.Symbol,
			Side:     sideToProto(cmd.Side),
			Price:    cmd.Price,
			Quantity: cmd.Quantity,
			PlacedAt: timestamppb.New(now),
		},
	}

	// Apply the placement so the order is on the book for matching.
	if err := book.Apply(placedEvt); err != nil {
		return nil, fmt.Errorf("apply order placed: %w", err)
	}

	incoming := book.Orders[orderID]
	tradeProtos := Match(book, incoming, now)

	events := []es.Event{placedEvt}
	for _, trade := range tradeProtos {
		tradeEvt := es.Event{
			AggregateID: book.AggregateID(),
			Type:        "TradeExecuted",
			Timestamp:   now,
			Data:        trade,
		}
		if err := book.Apply(tradeEvt); err != nil {
			return nil, fmt.Errorf("apply trade executed: %w", err)
		}
		events = append(events, tradeEvt)
	}

	return events, nil
}

// ExecuteCancelOrder produces an OrderCancelled event if the order exists.
func ExecuteCancelOrder(book *OrderBook, cmd CancelOrder) ([]es.Event, error) {
	order, ok := book.Orders[cmd.OrderID]
	if !ok {
		return nil, fmt.Errorf("order %s not found", cmd.OrderID)
	}
	if order.RemainingQty <= 0 {
		return nil, fmt.Errorf("order %s has no remaining quantity", cmd.OrderID)
	}

	return []es.Event{
		{
			AggregateID: book.AggregateID(),
			Type:        "OrderCancelled",
			Timestamp:   time.Now(),
			Data: &orderbookv1.OrderCancelled{
				OrderId: cmd.OrderID,
				Symbol:  cmd.Symbol,
			},
		},
	}, nil
}
