package orderbook

import "time"

// Side represents the direction of an order.
type Side int

const (
	Buy  Side = 1
	Sell Side = 2
)

// Order represents a limit order on the book.
type Order struct {
	ID           string
	Side         Side
	Price        int64
	Quantity     int64
	RemainingQty int64
	PlacedAt     time.Time
}
