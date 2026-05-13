package orderbook

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

var (
	ErrInvalidPrice          = errors.New("price must be positive")
	ErrInvalidQuantity       = errors.New("quantity must be positive")
	ErrOrderNotFound         = errors.New("order not found")
	ErrNoRemainingQty        = errors.New("order has no remaining quantity")
	ErrMarketGTC             = errors.New("market orders cannot use GTC time-in-force")
	ErrInsufficientLiquidity = errors.New("insufficient liquidity for FOK order")
	ErrMarketRequiresZeroPrice = errors.New("market orders must have zero price")
)

// PlaceOrder is a command to place a new order on the book.
type PlaceOrder struct {
	Symbol      string
	Side        Side
	Price       int64
	Quantity    int64
	OrderType   OrderType
	TimeInForce TimeInForce
}

func (c PlaceOrder) AggregateID() string {
	return AggregateID(c.Symbol)
}

// CancelOrder is a command to cancel an existing order.
type CancelOrder struct {
	Symbol  string
	OrderID string
}

func (c CancelOrder) AggregateID() string {
	return AggregateID(c.Symbol)
}

// ExecutePlaceOrder produces events for placing and matching a new order.
func ExecutePlaceOrder(book *OrderBook, cmd PlaceOrder) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}

	// Validate order type / TIF / price combinations.
	switch cmd.OrderType {
	case Market:
		if cmd.TimeInForce == GTC {
			return nil, ErrMarketGTC
		}
		if cmd.Price != 0 {
			return nil, ErrMarketRequiresZeroPrice
		}
	default: // Limit
		if cmd.Price <= 0 {
			return nil, ErrInvalidPrice
		}
	}

	tif := cmd.TimeInForce

	// FOK pre-check: ensure enough liquidity before emitting any events.
	if tif == FOK {
		avail := AvailableQty(book, cmd.Side, cmd.Price, cmd.OrderType == Market)
		if avail < cmd.Quantity {
			return nil, ErrInsufficientLiquidity
		}
	}

	now := time.Now()
	orderID := uuid.New().String()

	placedEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        "OrderPlaced",
		Timestamp:   now,
		Data: &orderbookv1.OrderPlaced{
			OrderId:     orderID,
			Symbol:      cmd.Symbol,
			Side:        sideToProto(cmd.Side),
			Price:       cmd.Price,
			Quantity:    cmd.Quantity,
			PlacedAt:    timestamppb.New(now),
			OrderType:   orderTypeToProto(cmd.OrderType),
			TimeInForce: tifToProto(tif),
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

	// IOC / Market: cancel any unfilled remainder.
	if tif == IOC || cmd.OrderType == Market {
		var filledQty int64
		for _, trade := range tradeProtos {
			filledQty += trade.Quantity
		}
		if remaining := cmd.Quantity - filledQty; remaining > 0 {
			cancelEvt := es.Event{
				AggregateID: book.AggregateID(),
				Type:        "OrderCancelled",
				Timestamp:   now,
				Data: &orderbookv1.OrderCancelled{
					OrderId: orderID,
					Symbol:  cmd.Symbol,
				},
			}
			if err := book.Apply(cancelEvt); err != nil {
				return nil, fmt.Errorf("apply order cancelled: %w", err)
			}
			events = append(events, cancelEvt)
		}
	}

	return events, nil
}

// ExecuteCancelOrder produces an OrderCancelled event if the order exists.
func ExecuteCancelOrder(book *OrderBook, cmd CancelOrder) ([]es.Event, error) {
	order, ok := book.Orders[cmd.OrderID]
	if !ok {
		return nil, ErrOrderNotFound
	}
	if order.RemainingQty <= 0 {
		return nil, ErrNoRemainingQty
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
