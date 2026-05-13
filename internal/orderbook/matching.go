package orderbook

import (
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

// Match runs price-time priority matching for the incoming order against the
// resting orders on the book. It returns TradeExecuted events for each fill.
// The incoming order's RemainingQty is decremented in place during matching.
func Match(book *OrderBook, incoming *Order, now time.Time) []*orderbookv1.TradeExecuted {
	var trades []*orderbookv1.TradeExecuted

	switch incoming.Side {
	case Buy:
		trades = matchBuy(book, incoming, now)
	case Sell:
		trades = matchSell(book, incoming, now)
	}

	return trades
}

func matchBuy(book *OrderBook, incoming *Order, now time.Time) []*orderbookv1.TradeExecuted {
	var trades []*orderbookv1.TradeExecuted

	for _, ask := range book.Asks {
		if incoming.RemainingQty <= 0 {
			break
		}
		if incoming.OrderType != Market && ask.Price > incoming.Price {
			break // asks are sorted lowest first; no more matches possible
		}

		qty := min(incoming.RemainingQty, ask.RemainingQty)
		trades = append(trades, &orderbookv1.TradeExecuted{
			TradeId:     uuid.New().String(),
			BuyOrderId:  incoming.ID,
			SellOrderId: ask.ID,
			Symbol:      book.Symbol,
			Price:       ask.Price, // trade at resting order's price
			Quantity:    qty,
			ExecutedAt:  timestamppb.New(now),
		})

		incoming.RemainingQty -= qty
		ask.RemainingQty -= qty
	}

	return trades
}

func matchSell(book *OrderBook, incoming *Order, now time.Time) []*orderbookv1.TradeExecuted {
	var trades []*orderbookv1.TradeExecuted

	for _, bid := range book.Bids {
		if incoming.RemainingQty <= 0 {
			break
		}
		if incoming.OrderType != Market && bid.Price < incoming.Price {
			break // bids are sorted highest first; no more matches possible
		}

		qty := min(incoming.RemainingQty, bid.RemainingQty)
		trades = append(trades, &orderbookv1.TradeExecuted{
			TradeId:     uuid.New().String(),
			BuyOrderId:  bid.ID,
			SellOrderId: incoming.ID,
			Symbol:      book.Symbol,
			Price:       bid.Price, // trade at resting order's price
			Quantity:    qty,
			ExecutedAt:  timestamppb.New(now),
		})

		incoming.RemainingQty -= qty
		bid.RemainingQty -= qty
	}

	return trades
}

// AvailableQty returns the total resting quantity available on the given side
// at or better than the given price. For market orders (isMarket=true), all
// resting quantity on the side is counted regardless of price.
func AvailableQty(book *OrderBook, side Side, price int64, isMarket bool) int64 {
	var total int64
	switch side {
	case Buy:
		// Buying: count asks at or below the price
		for _, ask := range book.Asks {
			if !isMarket && ask.Price > price {
				break
			}
			total += ask.RemainingQty
		}
	case Sell:
		// Selling: count bids at or above the price
		for _, bid := range book.Bids {
			if !isMarket && bid.Price < price {
				break
			}
			total += bid.RemainingQty
		}
	}
	return total
}
