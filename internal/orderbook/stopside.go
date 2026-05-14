package orderbook

import (
	"iter"
	"slices"
)

// stopSide holds stop orders sorted by stop price. Buy stops are sorted
// ascending (lowest trigger first), sell stops descending (highest trigger
// first). This lets the trigger-check loop early-exit once stop prices pass
// the trigger threshold.
type stopSide struct {
	orders []*Order
	less   func(a, b int64) bool
}

func newBuyStopSide() *stopSide {
	return &stopSide{
		less: func(a, b int64) bool { return a < b },
	}
}

func newSellStopSide() *stopSide {
	return &stopSide{
		less: func(a, b int64) bool { return a > b },
	}
}

func (ss *stopSide) Insert(order *Order) {
	i, _ := slices.BinarySearchFunc(ss.orders, order.StopPrice, func(o *Order, target int64) int {
		if ss.less(o.StopPrice, target) {
			return -1
		}
		if ss.less(target, o.StopPrice) {
			return 1
		}
		return 0
	})
	ss.orders = slices.Insert(ss.orders, i, order)
}

func (ss *stopSide) Remove(orderID string) {
	ss.orders = slices.DeleteFunc(ss.orders, func(o *Order) bool {
		return o.ID == orderID
	})
}

// All returns an iterator over all stop orders in trigger-priority order.
func (ss *stopSide) All() iter.Seq[*Order] {
	return func(yield func(*Order) bool) {
		for _, order := range ss.orders {
			if !yield(order) {
				return
			}
		}
	}
}

// Len returns the number of stop orders.
func (ss *stopSide) Len() int {
	return len(ss.orders)
}

// Reset clears all stop orders.
func (ss *stopSide) Reset() {
	ss.orders = nil
}

// Triggered returns all stop orders that should fire at the given trade price.
// Buy stops trigger when tradePrice >= stopPrice; sell stops trigger when
// tradePrice <= stopPrice.
func (ss *stopSide) Triggered(tradePrice int64) []*Order {
	var triggered []*Order
	for _, order := range ss.orders {
		if shouldTrigger(order, tradePrice) {
			triggered = append(triggered, order)
		} else {
			break
		}
	}
	return triggered
}

func shouldTrigger(order *Order, tradePrice int64) bool {
	switch order.Side {
	case Buy:
		return tradePrice >= order.StopPrice
	case Sell:
		return tradePrice <= order.StopPrice
	}
	return false
}

