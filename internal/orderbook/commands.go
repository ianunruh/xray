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
	ErrAuctionRejectsIOC           = errors.New("IOC/FOK orders cannot be placed during an auction")
	ErrAuctionRejectsMarket        = errors.New("market orders cannot be placed during an auction without AT_OPEN/AT_CLOSE")
	ErrMarketClosed                = errors.New("market is closed")
	ErrAlreadyInAuction            = errors.New("market is already in an auction phase")
	ErrNotInAuction                = errors.New("uncross requires the market to be in an auction phase")
	ErrCannotOpenAuction           = errors.New("opening auction can only be entered from CONTINUOUS or CLOSED")
	ErrAtOpenOutsideAuction          = errors.New("AT_OPEN orders are only accepted during the opening auction")
	ErrAtCloseOutsideAcceptanceWindow = errors.New("AT_CLOSE orders are only accepted in CONTINUOUS or CLOSING_AUCTION")
	ErrAuctionStopNotAllowed         = errors.New("stop orders cannot be combined with AT_OPEN/AT_CLOSE")
	ErrCannotBeginClosing             = errors.New("closing auction can only be entered from CONTINUOUS")
	ErrClosingAuctionRejectsRegular   = errors.New("regular orders are not accepted during the closing auction (only AT_CLOSE)")
	ErrIcebergRequiresLimit           = errors.New("iceberg orders must be limit orders")
	ErrIcebergRequiresRestingTIF      = errors.New("iceberg orders require a resting time-in-force (GTC or DAY)")
	ErrIcebergDisplayExceedsQuantity  = errors.New("display_quantity must be <= quantity")
	ErrIcebergNotAllowedWithReplace   = errors.New("iceberg orders cannot be used with ReplaceOrder")
	ErrTrailingStopRequiresTrail      = errors.New("trailing stops require exactly one of trail_amount or trail_offset_bps (> 0)")
	ErrTrailingStopAmbiguousTrail     = errors.New("trailing stops must specify trail_amount OR trail_offset_bps, not both")
	ErrTrailingStopLimitRequiresOffset = errors.New("trailing-stop-limit orders require a positive limit_offset")
	ErrTrailingStopRejectsLimitOffset = errors.New("trailing-stop-market orders must not specify limit_offset")
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
	OCOGroupID  string
	// DisplayQty, when > 0, makes this an iceberg: only DisplayQty is
	// exposed to matching at a time. Must be <= Quantity; requires
	// Limit + GTC/DAY (no Market/IOC/FOK/Stop/auction-TIF).
	DisplayQty int64
	// Trailing-stop fields; required when OrderType is
	// TrailingStopMarket / TrailingStopLimit. Exactly one of TrailAmount
	// or TrailOffsetBps must be > 0. LimitOffset is only valid for
	// TrailingStopLimit.
	TrailAmount    int64
	TrailOffsetBps int32
	LimitOffset    int64
}

func (c PlaceOrder) AggregateID() string {
	return AggregateID(c.Symbol)
}

// CancelOrder is a command to cancel an existing order.
type CancelOrder struct {
	Symbol  string
	OrderID string
	// Reason is recorded on OrderCancelled and propagates through any
	// owning saga's failure path. Empty defaults to "user requested".
	Reason string
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
	DisplayQty  int64
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
	case TrailingStopMarket:
		if cmd.StopPrice <= 0 {
			return nil, ErrStopRequiresStopPrice
		}
		if cmd.Price != 0 {
			return nil, ErrStopMarketRequiresZeroPrice
		}
		if err := validateTrailingParams(cmd, false); err != nil {
			return nil, err
		}
	case TrailingStopLimit:
		if cmd.StopPrice <= 0 {
			return nil, ErrStopRequiresStopPrice
		}
		// Price is derived from StopPrice + LimitOffset at trigger time,
		// so the caller should not pre-supply a limit price.
		if cmd.Price != 0 {
			return nil, ErrStopMarketRequiresZeroPrice
		}
		if err := validateTrailingParams(cmd, true); err != nil {
			return nil, err
		}
	default: // Limit
		if cmd.Price <= 0 {
			return nil, ErrInvalidPrice
		}
	}

	tif := cmd.TimeInForce

	// Stop orders can't be auction-bound — they need a last-trade
	// reference to trigger from, which doesn't exist mid-auction.
	if (tif == AtOpen || tif == AtClose) && cmd.OrderType.IsStop() {
		return nil, ErrAuctionStopNotAllowed
	}

	if cmd.DisplayQty > 0 {
		if cmd.OrderType != Limit {
			return nil, ErrIcebergRequiresLimit
		}
		if tif != GTC && tif != Day {
			return nil, ErrIcebergRequiresRestingTIF
		}
		if cmd.DisplayQty > cmd.Quantity {
			return nil, ErrIcebergDisplayExceedsQuantity
		}
	}

	// Phase-aware validation matrix. Each phase has a different set of
	// acceptable (OrderType, TimeInForce) combinations.
	switch book.Phase {
	case PhaseClosed:
		return nil, ErrMarketClosed

	case PhaseContinuous:
		// AT_OPEN can't be staged outside the opening auction.
		if tif == AtOpen {
			return nil, ErrAtOpenOutsideAuction
		}
		// AT_CLOSE is fine — rests in ClosingBook until the next close.
		// Everything else follows the existing continuous-matching rules.

	case PhaseAuction:
		// Opening auction: regular limits + AT_OPEN (incl. market AT_OPEN).
		if tif == AtClose {
			return nil, ErrAtCloseOutsideAcceptanceWindow
		}
		if tif == IOC || tif == FOK {
			return nil, ErrAuctionRejectsIOC
		}
		if cmd.OrderType == Market && tif != AtOpen {
			return nil, ErrAuctionRejectsMarket
		}

	case PhaseClosingAuction:
		// Closing auction: continuous is frozen. Only AT_CLOSE accepted.
		if tif != AtClose {
			return nil, ErrClosingAuctionRejectsRegular
		}
	}

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
			OrderId:         orderID,
			Symbol:          cmd.Symbol,
			Side:            SideToProto(cmd.Side),
			Price:           cmd.Price,
			StopPrice:       cmd.StopPrice,
			Quantity:        cmd.Quantity,
			PlacedAt:        timestamppb.New(now),
			OrderType:       OrderTypeToProto(cmd.OrderType),
			TimeInForce:     TimeInForceToProto(tif),
			AccountId:       cmd.AccountID,
			OcoGroupId:      cmd.OCOGroupID,
			DisplayQuantity: cmd.DisplayQty,
			TrailAmount:     cmd.TrailAmount,
			TrailOffsetBps:  cmd.TrailOffsetBps,
			LimitOffset:     cmd.LimitOffset,
		},
	}

	if err := book.Apply(placedEvt); err != nil {
		return nil, fmt.Errorf("apply order placed: %w", err)
	}

	events := []es.Event{placedEvt}

	// During an auction, orders rest in the book without matching.
	// The uncross algorithm will pair them at a single clearing price.
	if book.Phase == PhaseAuction || book.Phase == PhaseClosingAuction {
		return events, nil
	}

	// Stop orders rest until triggered — no immediate matching.
	if cmd.OrderType.IsStop() {
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
	// Iceberg orders only expose one slice at a time. When a slice fully
	// fills the match loop will skip past the resting iceberg (its
	// VisibleQty drops to 0); we then emit IcebergSliceReplenished,
	// reseat the iceberg at the back of its price level, and retry
	// matching so the incoming order can keep consuming liquidity at the
	// same level if any was hidden behind that iceberg.
	for {
		result := Match(book, incoming, now)
		replenished := false
		for _, trade := range result.Trades {
			restingID := trade.BuyOrderId
			if restingID == incoming.ID {
				restingID = trade.SellOrderId
			}
			resting := book.Orders[restingID]

			tradeEvt := es.Event{
				AggregateID: book.AggregateID(),
				Type:        EventTradeExecuted,
				Timestamp:   now,
				Data:        trade,
			}
			book.Apply(tradeEvt)
			events = append(events, tradeEvt)

			if resting != nil && resting.OCOGroupID != "" {
				events = cancelOCOSiblings(book, resting, events, now)
			}

			if resting != nil && resting.DisplayQty > 0 && resting.Displayed <= 0 && resting.RemainingQty > 0 {
				replenishEvt := makeIcebergReplenish(book, resting, now)
				book.Apply(replenishEvt)
				events = append(events, replenishEvt)
				replenished = true
			}

			// Each trade is a new mark observation; ratchet any
			// resting trailing stops before the next trigger pass.
			events = ratchetTrailingStops(book, trade.Price, events, now)
		}
		if result.SelfTradePrevented {
			events = cancelUnfilled(book, incoming, events, now, "self-trade prevention")
			return events, true
		}
		if !replenished || incoming.RemainingQty <= 0 {
			return events, false
		}
	}
}

func makeIcebergReplenish(book *OrderBook, order *Order, now time.Time) es.Event {
	newDisplay := min(order.DisplayQty, order.RemainingQty)
	hidden := order.RemainingQty - newDisplay
	return es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventIcebergSliceReplenished,
		Timestamp:   now,
		Data: &orderbookv1.IcebergSliceReplenished{
			OrderId:         order.ID,
			Symbol:          book.Symbol,
			Side:            SideToProto(order.Side),
			Price:           order.Price,
			NewDisplayedQty: newDisplay,
			HiddenRemaining: hidden,
			ReplenishedAt:   timestamppb.New(now),
		},
	}
}

// cancelOCOSiblings emits OrderCancelled events for every other member
// of `winner`'s OCO group, and removes `winner` from the group so a
// subsequent fill of the same resting order doesn't fire the cancels
// again. Idempotent at the orderbook layer — cancelling an order that's
// already gone is a no-op.
func cancelOCOSiblings(book *OrderBook, winner *Order, events []es.Event, now time.Time) []es.Event {
	group := book.OCOGroups[winner.OCOGroupID]
	if group == nil {
		return events
	}
	delete(group, winner.ID)
	if len(group) == 0 {
		delete(book.OCOGroups, winner.OCOGroupID)
		return events
	}
	// Snapshot the membership; cancelling each one mutates `group` via
	// applyOrderCancelled, so iterating it directly would be unsafe.
	siblings := make([]string, 0, len(group))
	for id := range group {
		siblings = append(siblings, id)
	}
	for _, id := range siblings {
		if _, ok := book.Orders[id]; !ok {
			continue
		}
		cancelEvt := es.Event{
			AggregateID: book.AggregateID(),
			Type:        EventOrderCancelled,
			Timestamp:   now,
			Data: &orderbookv1.OrderCancelled{
				OrderId: id,
				Symbol:  book.Symbol,
				Reason:  "oco_triggered",
			},
		}
		book.Apply(cancelEvt)
		events = append(events, cancelEvt)
	}
	return events
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

			if stopOrder.OrderType == StopMarket || stopOrder.OrderType == TrailingStopMarket || stp {
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
	case StopMarket, TrailingStopMarket:
		return orderbookv1.OrderType_ORDER_TYPE_MARKET
	case StopLimit, TrailingStopLimit:
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

	reason := cmd.Reason
	if reason == "" {
		reason = "user requested"
	}
	evt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventOrderCancelled,
		Timestamp:   time.Now(),
		Data: &orderbookv1.OrderCancelled{
			OrderId: cmd.OrderID,
			Symbol:  cmd.Symbol,
			Reason:  reason,
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
	// Replace assumes continuous matching semantics; it's not meaningful
	// mid-auction. Callers should Cancel + Place instead during an auction.
	switch book.Phase {
	case PhaseClosed:
		return nil, ErrMarketClosed
	case PhaseAuction, PhaseClosingAuction:
		return nil, ErrAuctionRejectsIOC
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
	case StopMarket, StopLimit, TrailingStopMarket, TrailingStopLimit:
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

	if cmd.DisplayQty > 0 {
		if cmd.OrderType != Limit {
			return nil, ErrIcebergRequiresLimit
		}
		if tif != GTC && tif != Day {
			return nil, ErrIcebergRequiresRestingTIF
		}
		if cmd.DisplayQty > cmd.Quantity {
			return nil, ErrIcebergDisplayExceedsQuantity
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
			OrderId:         orderID,
			Symbol:          cmd.Symbol,
			Side:            SideToProto(cmd.Side),
			Price:           cmd.Price,
			Quantity:        cmd.Quantity,
			PlacedAt:        timestamppb.New(now),
			OrderType:       OrderTypeToProto(cmd.OrderType),
			TimeInForce:     TimeInForceToProto(tif),
			AccountId:       cmd.AccountID,
			DisplayQuantity: cmd.DisplayQty,
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
