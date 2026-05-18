package orderbook

import (
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

type MatchResult struct {
	Trades             []*orderbookv1.TradeExecuted
	SelfTradePrevented bool
}

// Match runs price-time priority matching for the incoming order against the
// resting orders on the book. It returns TradeExecuted events for each fill.
// The incoming order's RemainingQty is decremented in place during matching.
// If the incoming order would match against a resting order from the same
// account (non-empty AccountID), matching stops and SelfTradePrevented is set.
func Match(book *OrderBook, incoming *Order, now time.Time) MatchResult {
	switch incoming.Side {
	case Buy:
		return matchBuy(book, incoming, now)
	case Sell:
		return matchSell(book, incoming, now)
	}
	return MatchResult{}
}

func matchBuy(book *OrderBook, incoming *Order, now time.Time) MatchResult {
	var trades []*orderbookv1.TradeExecuted
	remainingQty := incoming.RemainingQty

	for ask := range book.Asks.All() {
		if remainingQty <= 0 {
			break
		}
		if incoming.OrderType != Market && ask.Price > incoming.Price {
			break
		}
		if incoming.AccountID != "" && ask.AccountID == incoming.AccountID {
			return MatchResult{Trades: trades, SelfTradePrevented: true}
		}

		// For iceberg resters, only the displayed slice is fillable.
		// Once it's exhausted the matching loop must move on to the next
		// resting order; the caller emits IcebergSliceReplenished and
		// reseats the iceberg at the back of the queue.
		askQty := ask.VisibleQty()
		if askQty <= 0 {
			continue
		}
		qty := min(remainingQty, askQty)
		trades = append(trades, &orderbookv1.TradeExecuted{
			TradeId:     uuid.New().String(),
			BuyOrderId:  incoming.ID,
			SellOrderId: ask.ID,
			Symbol:      book.Symbol,
			Price:       ask.Price,
			Quantity:    qty,
			ExecutedAt:  timestamppb.New(now),
		})

		remainingQty -= qty
	}

	return MatchResult{Trades: trades}
}

func matchSell(book *OrderBook, incoming *Order, now time.Time) MatchResult {
	var trades []*orderbookv1.TradeExecuted
	remainingQty := incoming.RemainingQty

	for bid := range book.Bids.All() {
		if remainingQty <= 0 {
			break
		}
		if incoming.OrderType != Market && bid.Price < incoming.Price {
			break
		}
		if incoming.AccountID != "" && bid.AccountID == incoming.AccountID {
			return MatchResult{Trades: trades, SelfTradePrevented: true}
		}

		bidQty := bid.VisibleQty()
		if bidQty <= 0 {
			continue
		}
		qty := min(remainingQty, bidQty)
		trades = append(trades, &orderbookv1.TradeExecuted{
			TradeId:     uuid.New().String(),
			BuyOrderId:  bid.ID,
			SellOrderId: incoming.ID,
			Symbol:      book.Symbol,
			Price:       bid.Price,
			Quantity:    qty,
			ExecutedAt:  timestamppb.New(now),
		})

		remainingQty -= qty
	}

	return MatchResult{Trades: trades}
}

// AvailableQty returns the total resting quantity available on the given side
// at or better than the given price. For market orders (isMarket=true), all
// resting quantity on the side is counted regardless of price.
// If excludeAccountID is non-empty, counting stops at the first resting order
// from that account (mirroring cancel-incoming self-trade prevention).
func AvailableQty(book *OrderBook, side Side, price int64, isMarket bool, excludeAccountID string) int64 {
	var total int64
	switch side {
	case Buy:
		for ask := range book.Asks.All() {
			if !isMarket && ask.Price > price {
				break
			}
			if excludeAccountID != "" && ask.AccountID == excludeAccountID {
				break
			}
			total += ask.RemainingQty
		}
	case Sell:
		for bid := range book.Bids.All() {
			if !isMarket && bid.Price < price {
				break
			}
			if excludeAccountID != "" && bid.AccountID == excludeAccountID {
				break
			}
			total += bid.RemainingQty
		}
	}
	return total
}
