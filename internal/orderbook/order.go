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
	Limit              OrderType = 0
	Market             OrderType = 1
	StopMarket         OrderType = 2
	StopLimit          OrderType = 3
	TrailingStopMarket OrderType = 4
	TrailingStopLimit  OrderType = 5
)

// IsStop reports whether the order rests as a stop order until
// triggered. Covers both fixed stops and trailing stops.
func (ot OrderType) IsStop() bool {
	return ot == StopMarket || ot == StopLimit || ot == TrailingStopMarket || ot == TrailingStopLimit
}

// IsTrailingStop reports whether the order's stop price ratchets with
// the mark.
func (ot OrderType) IsTrailingStop() bool {
	return ot == TrailingStopMarket || ot == TrailingStopLimit
}

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
//
// PhaseLimitState and PhaseHalted are LULD (limit-up/limit-down)
// volatility states. LimitState accepts limit orders priced at-or-better
// than the active band but rejects anything that would trade through
// it; Halted rejects every new order until a reopening auction
// completes.
type MarketPhase int

const (
	PhaseContinuous     MarketPhase = 0
	PhaseAuction        MarketPhase = 1
	PhaseClosingAuction MarketPhase = 2
	PhaseClosed         MarketPhase = 3
	PhaseLimitState     MarketPhase = 4
	PhaseHalted         MarketPhase = 5
)

// IsHalted reports whether the symbol is fully paused — every new order
// is rejected until a reopening auction runs.
func (p MarketPhase) IsHalted() bool { return p == PhaseHalted }

// IsLimitState reports whether the symbol is in an LULD limit state —
// limit orders may still rest at-or-better than the band, but
// through-the-band orders are rejected.
func (p MarketPhase) IsLimitState() bool { return p == PhaseLimitState }

// CanTrade reports whether continuous matching can produce trades
// against this phase right now. Used by strategy bots to skip cycles
// when the symbol is auctioning, halted, or in a limit state.
func (p MarketPhase) CanTrade() bool { return p == PhaseContinuous }

// CrossType marks how a trade was produced.
type CrossType int

const (
	CrossNone       CrossType = 0
	CrossOpening    CrossType = 1
	CrossClosing    CrossType = 2
	CrossHaltReopen CrossType = 3
)

// Order represents an order on the book.
//
// Iceberg orders carry DisplayQty > 0 and track Displayed separately
// from RemainingQty: Displayed is the visible slice the matching engine
// can fill, RemainingQty is the total (visible + hidden) unfilled qty.
// When Displayed reaches 0 with RemainingQty > 0, the engine emits
// IcebergSliceReplenished and re-inserts the order with a fresh PlacedAt
// (loses time priority at the same price level). For non-iceberg orders
// DisplayQty is 0 and Displayed mirrors RemainingQty.
//
// Trailing stops carry exactly one of TrailAmount or TrailOffsetBps;
// after every trade the engine ratchets StopPrice tighter when the
// mark has moved favorably (up for sells, down for buys), emitting
// TrailingStopAdjusted so replay reproduces the ratchet path. LimitOffset
// only applies to TrailingStopLimit and is the gap between the trigger
// and the placed limit price at activation time.
type Order struct {
	ID             string
	AccountID      string
	Side           Side
	Price          int64
	StopPrice      int64
	Quantity       int64
	RemainingQty   int64
	DisplayQty     int64
	Displayed      int64
	TrailAmount    int64
	TrailOffsetBps int32
	LimitOffset    int64
	PlacedAt       time.Time
	OrderType      OrderType
	TimeInForce    TimeInForce
	OCOGroupID     string
}

// VisibleQty returns the quantity the matching engine may fill against
// this order right now. For non-iceberg orders that's RemainingQty; for
// icebergs it's the current Displayed slice.
func (o *Order) VisibleQty() int64 {
	if o.DisplayQty > 0 {
		return o.Displayed
	}
	return o.RemainingQty
}
