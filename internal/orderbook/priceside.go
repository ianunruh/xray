package orderbook

import (
	"iter"
	"slices"
	"sort"
)

// priceSide groups orders by price level with a sorted index of distinct prices.
// Insert is O(log p) where p = distinct prices. Remove is O(k) where k = orders
// at that price level.
type priceSide struct {
	prices []int64            // sorted distinct price levels
	levels map[int64][]*Order // price -> FIFO queue of orders
	less   func(a, b int64) bool
	count  int
}

func newBidSide() *priceSide {
	return &priceSide{
		levels: make(map[int64][]*Order),
		less: func(a, b int64) bool {
			return a > b // descending: highest price first
		},
	}
}

func newAskSide() *priceSide {
	return &priceSide{
		levels: make(map[int64][]*Order),
		less: func(a, b int64) bool {
			return a < b // ascending: lowest price first
		},
	}
}

// Insert adds an order to the appropriate price level.
func (ps *priceSide) Insert(order *Order) {
	price := order.Price
	queue := ps.levels[price]
	if queue == nil {
		// New price level: insert into sorted prices index.
		i := sort.Search(len(ps.prices), func(i int) bool {
			return !ps.less(ps.prices[i], price)
		})
		ps.prices = slices.Insert(ps.prices, i, price)
	}
	ps.levels[price] = append(queue, order)
	ps.count++
}

// Remove removes an order from its price level. If the level becomes empty,
// it is removed from the prices index.
func (ps *priceSide) Remove(order *Order) {
	price := order.Price
	queue := ps.levels[price]
	for i, o := range queue {
		if o.ID == order.ID {
			queue = slices.Delete(queue, i, i+1)
			ps.count--
			if len(queue) == 0 {
				delete(ps.levels, price)
				// Remove from sorted prices index.
				j := sort.Search(len(ps.prices), func(j int) bool {
					return !ps.less(ps.prices[j], price)
				})
				if j < len(ps.prices) && ps.prices[j] == price {
					ps.prices = slices.Delete(ps.prices, j, j+1)
				}
			} else {
				ps.levels[price] = queue
			}
			return
		}
	}
}

// All returns an iterator that yields orders in price-time priority order.
func (ps *priceSide) All() iter.Seq[*Order] {
	return func(yield func(*Order) bool) {
		for _, price := range ps.prices {
			for _, order := range ps.levels[price] {
				if !yield(order) {
					return
				}
			}
		}
	}
}

// BestPrice returns the best (first) price level, or 0 if empty.
func (ps *priceSide) BestPrice() int64 {
	if len(ps.prices) == 0 {
		return 0
	}
	return ps.prices[0]
}

// Len returns the total number of orders across all price levels.
func (ps *priceSide) Len() int {
	return ps.count
}

// Reset clears all state.
func (ps *priceSide) Reset() {
	ps.prices = nil
	clear(ps.levels)
	ps.count = 0
}
