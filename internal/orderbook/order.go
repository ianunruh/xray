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
	Limit      OrderType = 0
	Market     OrderType = 1
	StopMarket OrderType = 2
	StopLimit  OrderType = 3
)

// TimeInForce controls how long an order remains active.
type TimeInForce int

const (
	GTC     TimeInForce = 0
	IOC     TimeInForce = 1
	FOK     TimeInForce = 2
	Day     TimeInForce = 3
	AtOpen  TimeInForce = 4
	AtClose TimeInForce = 5
)

// MarketPhase gates how the orderbook handles incoming orders. The zero
// value is PhaseContinuous, so a freshly-created OrderBook (or one
// whose event stream contains no MarketPhaseChanged events) behaves
// exactly as before.
type MarketPhase int

const (
	PhaseContinuous     MarketPhase = 0
	PhaseAuction        MarketPhase = 1
	PhaseClosingAuction MarketPhase = 2
	PhaseClosed         MarketPhase = 3
)

// CrossType marks how a trade was produced.
type CrossType int

const (
	CrossNone       CrossType = 0
	CrossOpening    CrossType = 1
	CrossClosing    CrossType = 2
	CrossHaltReopen CrossType = 3
)

// Order represents an order on the book.
type Order struct {
	ID           string
	AccountID    string
	Side         Side
	Price        int64
	StopPrice    int64
	Quantity     int64
	RemainingQty int64
	PlacedAt     time.Time
	OrderType    OrderType
	TimeInForce  TimeInForce
	OCOGroupID   string
}
