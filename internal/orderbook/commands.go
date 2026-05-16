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
	ErrInvalidPrice                = errors.New("price must be positive")
	ErrInvalidQuantity             = errors.New("quantity must be positive")
	ErrOrderNotFound               = errors.New("order not found")
	ErrNoRemainingQty              = errors.New("order has no remaining quantity")
	ErrMarketGTC                   = errors.New("market orders cannot use GTC time-in-force")
	ErrInsufficientLiquidity       = errors.New("insufficient liquidity for FOK order")
	ErrMarketRequiresZeroPrice     = errors.New("market orders must have zero price")
	ErrStopRequiresStopPrice       = errors.New("stop orders require a positive stop price")
	ErrStopMarketRequiresZeroPrice = errors.New("stop-market orders must have zero price")
	ErrStopLimitRequiresPrice      = errors.New("stop-limit orders require a positive limit price")
	ErrAccountMismatch             = errors.New("old order belongs to a different account")
)

// PlaceOrder is a command to place a new order on the book.
type PlaceOrder struct {
	Symbol      string
	Side        Side
	Price       int64
	StopPrice   int64
	Quantity    int64
	OrderType   OrderType
	TimeInForce TimeInForce
	OrderID     string
	AccountID   string
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

// ReplaceOrder is a command to atomically cancel an existing order and place a
// new one. The old order is cancelled and the new order is placed and matched
// in a single event batch.
type ReplaceOrder struct {
	Symbol      string
	OldOrderID  string
	NewOrderID  string
	Side        Side
	Price       int64
	Quantity    int64
	OrderType   OrderType
	TimeInForce TimeInForce
	AccountID   string
}

func (c ReplaceOrder) AggregateID() string {
	return AggregateID(c.Symbol)
}

// ExecutePlaceOrder produces events for placing and matching a new order.
// When cmd.OrderID is provided and the book already contains an order with
// that ID, the call is treated as a duplicate and returns no events. This
// lets callers retry placement safely after a crash between PlaceOrder
// succeeding and the caller's follow-up write.
func ExecutePlaceOrder(book *OrderBook, cmd PlaceOrder) ([]es.Event, error) {
	if cmd.OrderID != "" {
		if _, exists := book.Orders[cmd.OrderID]; exists {
			return nil, nil
		}
	}
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}

	switch cmd.OrderType {
	case Market:
		if cmd.TimeInForce == GTC || cmd.TimeInForce == Day {
			return nil, ErrMarketGTC
		}
		if cmd.Price != 0 {
			return nil, ErrMarketRequiresZeroPrice
		}
	case StopMarket:
		if cmd.StopPrice <= 0 {
			return nil, ErrStopRequiresStopPrice
		}
		if cmd.Price != 0 {
			return nil, ErrStopMarketRequiresZeroPrice
		}
	case StopLimit:
		if cmd.StopPrice <= 0 {
			return nil, ErrStopRequiresStopPrice
		}
		if cmd.Price <= 0 {
			return nil, ErrStopLimitRequiresPrice
		}
	default: // Limit
		if cmd.Price <= 0 {
			return nil, ErrInvalidPrice
		}
	}

	tif := cmd.TimeInForce

	// FOK pre-check: ensure enough liquidity before emitting any events.
	if tif == FOK {
		avail := AvailableQty(book, cmd.Side, cmd.Price, cmd.OrderType == Market, cmd.AccountID)
		if avail < cmd.Quantity {
			return nil, ErrInsufficientLiquidity
		}
	}

	now := time.Now()
	orderID := cmd.OrderID
	if orderID == "" {
		orderID = uuid.New().String()
	}

	placedEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventOrderPlaced,
		Timestamp:   now,
		Data: &orderbookv1.OrderPlaced{
			OrderId:     orderID,
			Symbol:      cmd.Symbol,
			Side:        SideToProto(cmd.Side),
			Price:       cmd.Price,
			StopPrice:   cmd.StopPrice,
			Quantity:    cmd.Quantity,
			PlacedAt:    timestamppb.New(now),
			OrderType:   OrderTypeToProto(cmd.OrderType),
			TimeInForce: TimeInForceToProto(tif),
			AccountId:   cmd.AccountID,
		},
	}

	if err := book.Apply(placedEvt); err != nil {
		return nil, fmt.Errorf("apply order placed: %w", err)
	}

	events := []es.Event{placedEvt}

	// Stop orders rest until triggered — no immediate matching.
	if cmd.OrderType == StopMarket || cmd.OrderType == StopLimit {
		return events, nil
	}

	incoming := book.Orders[orderID]
	events, selfTradePrevented := matchAndAppend(book, incoming, events, now)

	// Cancel unfilled remainder for IOC/Market, or when self-trade prevention fires.
	if tif == IOC || cmd.OrderType == Market || selfTradePrevented {
		reason := "no liquidity"
		if selfTradePrevented {
			reason = "self-trade prevention"
		}
		events = cancelUnfilled(book, incoming, events, now, reason)
	}

	events = triggerStops(book, events, now)

	return events, nil
}

func matchAndAppend(book *OrderBook, incoming *Order, events []es.Event, now time.Time) ([]es.Event, bool) {
	result := Match(book, incoming, now)
	for _, trade := range result.Trades {
		tradeEvt := es.Event{
			AggregateID: book.AggregateID(),
			Type:        EventTradeExecuted,
			Timestamp:   now,
			Data:        trade,
		}
		book.Apply(tradeEvt)
		events = append(events, tradeEvt)
	}
	if result.SelfTradePrevented {
		events = cancelUnfilled(book, incoming, events, now, "self-trade prevention")
	}
	return events, result.SelfTradePrevented
}

func cancelUnfilled(book *OrderBook, order *Order, events []es.Event, now time.Time, reason string) []es.Event {
	if order.RemainingQty <= 0 {
		return events
	}
	if _, ok := book.Orders[order.ID]; !ok {
		return events
	}
	cancelEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventOrderCancelled,
		Timestamp:   now,
		Data: &orderbookv1.OrderCancelled{
			OrderId: order.ID,
			Symbol:  book.Symbol,
			Reason:  reason,
		},
	}
	book.Apply(cancelEvt)
	events = append(events, cancelEvt)
	return events
}

func triggerStops(book *OrderBook, events []es.Event, now time.Time) []es.Event {
	for {
		lastTradePrice, ok := lastTradePriceFrom(events)
		if !ok {
			break
		}

		triggered := collectTriggeredStops(book, lastTradePrice)
		if len(triggered) == 0 {
			break
		}

		for _, stopOrder := range triggered {
			triggerEvt := es.Event{
				AggregateID: book.AggregateID(),
				Type:        EventStopTriggered,
				Timestamp:   now,
				Data: &orderbookv1.StopTriggered{
					OrderId:      stopOrder.ID,
					Symbol:       book.Symbol,
					StopPrice:    stopOrder.StopPrice,
					TriggerPrice: lastTradePrice,
					ActivatedAs:  activatedOrderType(stopOrder.OrderType),
				},
			}
			book.Apply(triggerEvt)
			events = append(events, triggerEvt)

			activated := book.Orders[stopOrder.ID]
			var stp bool
			events, stp = matchAndAppend(book, activated, events, now)

			if stopOrder.OrderType == StopMarket || stp {
				reason := "no liquidity"
				if stp {
					reason = "self-trade prevention"
				}
				events = cancelUnfilled(book, activated, events, now, reason)
			}
		}
	}
	return events
}

func lastTradePriceFrom(events []es.Event) (int64, bool) {
	for i := len(events) - 1; i >= 0; i-- {
		if trade, ok := events[i].Data.(*orderbookv1.TradeExecuted); ok {
			return trade.Price, true
		}
	}
	return 0, false
}

func collectTriggeredStops(book *OrderBook, tradePrice int64) []*Order {
	triggered := book.SellStops.Triggered(tradePrice)
	triggered = append(triggered, book.BuyStops.Triggered(tradePrice)...)
	return triggered
}

func activatedOrderType(ot OrderType) orderbookv1.OrderType {
	switch ot {
	case StopMarket:
		return orderbookv1.OrderType_ORDER_TYPE_MARKET
	case StopLimit:
		return orderbookv1.OrderType_ORDER_TYPE_LIMIT
	default:
		return orderbookv1.OrderType_ORDER_TYPE_UNSPECIFIED
	}
}

// CloseMarket is a command to close the market for a symbol, cancelling all Day orders.
type CloseMarket struct {
	Symbol string
}

func (c CloseMarket) AggregateID() string {
	return AggregateID(c.Symbol)
}

// ExecuteCloseMarket produces a MarketClosed event followed by OrderCancelled
// events for every resting order with Day time-in-force.
func ExecuteCloseMarket(book *OrderBook, cmd CloseMarket) ([]es.Event, error) {
	now := time.Now()

	closedEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventMarketClosed,
		Timestamp:   now,
		Data: &orderbookv1.MarketClosed{
			Symbol:   cmd.Symbol,
			ClosedAt: timestamppb.New(now),
		},
	}

	if err := book.Apply(closedEvt); err != nil {
		return nil, fmt.Errorf("apply market closed: %w", err)
	}

	events := []es.Event{closedEvt}

	var dayOrders []*Order
	for _, order := range book.Orders {
		if order.TimeInForce == Day && order.RemainingQty > 0 {
			dayOrders = append(dayOrders, order)
		}
	}

	for _, order := range dayOrders {
		cancelEvt := es.Event{
			AggregateID: book.AggregateID(),
			Type:        EventOrderCancelled,
			Timestamp:   now,
			Data: &orderbookv1.OrderCancelled{
				OrderId: order.ID,
				Symbol:  cmd.Symbol,
				Reason:  "market closed",
			},
		}
		book.Apply(cancelEvt)
		events = append(events, cancelEvt)
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

	evt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventOrderCancelled,
		Timestamp:   time.Now(),
		Data: &orderbookv1.OrderCancelled{
			OrderId: cmd.OrderID,
			Symbol:  cmd.Symbol,
			Reason:  "user requested",
		},
	}

	if err := book.Apply(evt); err != nil {
		return nil, fmt.Errorf("apply order cancelled: %w", err)
	}

	return []es.Event{evt}, nil
}

// ExecuteReplaceOrder atomically cancels an existing order and places a new one.
// It produces OrderCancelled (for the old order), OrderPlaced (for the new
// order), and any TradeExecuted events from matching the new order.
//
// When cmd.NewOrderID is provided and already exists on the book, the call
// is treated as a duplicate and returns no events — the replacement has
// already happened.
func ExecuteReplaceOrder(book *OrderBook, cmd ReplaceOrder) ([]es.Event, error) {
	if cmd.NewOrderID != "" {
		if _, exists := book.Orders[cmd.NewOrderID]; exists {
			return nil, nil
		}
	}
	oldOrder, ok := book.Orders[cmd.OldOrderID]
	if !ok {
		return nil, ErrOrderNotFound
	}
	if oldOrder.RemainingQty <= 0 {
		return nil, ErrNoRemainingQty
	}
	if cmd.AccountID != "" && oldOrder.AccountID != cmd.AccountID {
		return nil, ErrAccountMismatch
	}

	now := time.Now()

	cancelEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventOrderCancelled,
		Timestamp:   now,
		Data: &orderbookv1.OrderCancelled{
			OrderId: cmd.OldOrderID,
			Symbol:  cmd.Symbol,
			Reason:  "replaced",
		},
	}
	if err := book.Apply(cancelEvt); err != nil {
		return nil, fmt.Errorf("apply cancel old order: %w", err)
	}

	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}

	switch cmd.OrderType {
	case Market:
		if cmd.TimeInForce == GTC || cmd.TimeInForce == Day {
			return nil, ErrMarketGTC
		}
		if cmd.Price != 0 {
			return nil, ErrMarketRequiresZeroPrice
		}
	case StopMarket:
		return nil, errors.New("stop orders cannot be used with replace")
	case StopLimit:
		return nil, errors.New("stop orders cannot be used with replace")
	default: // Limit
		if cmd.Price <= 0 {
			return nil, ErrInvalidPrice
		}
	}

	tif := cmd.TimeInForce

	if tif == FOK {
		avail := AvailableQty(book, cmd.Side, cmd.Price, cmd.OrderType == Market, cmd.AccountID)
		if avail < cmd.Quantity {
			return nil, ErrInsufficientLiquidity
		}
	}

	orderID := cmd.NewOrderID
	if orderID == "" {
		orderID = uuid.New().String()
	}

	placedEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventOrderPlaced,
		Timestamp:   now,
		Data: &orderbookv1.OrderPlaced{
			OrderId:     orderID,
			Symbol:      cmd.Symbol,
			Side:        SideToProto(cmd.Side),
			Price:       cmd.Price,
			Quantity:    cmd.Quantity,
			PlacedAt:    timestamppb.New(now),
			OrderType:   OrderTypeToProto(cmd.OrderType),
			TimeInForce: TimeInForceToProto(tif),
			AccountId:   cmd.AccountID,
		},
	}

	if err := book.Apply(placedEvt); err != nil {
		return nil, fmt.Errorf("apply order placed: %w", err)
	}

	events := []es.Event{cancelEvt, placedEvt}

	incoming := book.Orders[orderID]
	events, selfTradePrevented := matchAndAppend(book, incoming, events, now)

	if tif == IOC || cmd.OrderType == Market || selfTradePrevented {
		reason := "no liquidity"
		if selfTradePrevented {
			reason = "self-trade prevention"
		}
		events = cancelUnfilled(book, incoming, events, now, reason)
	}

	events = triggerStops(book, events, now)

	return events, nil
}
