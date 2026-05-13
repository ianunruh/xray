package orderbook

import "time"

// Side represents the direction of an order.
type Side int

const (
	Buy  Side = 1
	Sell Side = 2
)

// OrderType distinguishes limit from market orders.
type OrderType int

const (
	Limit  OrderType = 0
	Market OrderType = 1
)

// TimeInForce controls how long an order remains active.
type TimeInForce int

const (
	GTC TimeInForce = 0
	IOC TimeInForce = 1
	FOK TimeInForce = 2
)

// Order represents an order on the book.
type Order struct {
	ID           string
	Side         Side
	Price        int64
	Quantity     int64
	RemainingQty int64
	PlacedAt     time.Time
	OrderType    OrderType
	TimeInForce  TimeInForce
}
