package orderbook

import "slices"

// auctionBook partitions AT_OPEN / AT_CLOSE orders away from the
// continuous depth. Orders are stored in insertion order — uncross
// algorithms re-sort by (price, time) per side when computing fills.
type auctionBook struct {
	BuyOrders  []*Order
	SellOrders []*Order
}

func newAuctionBook() *auctionBook {
	return &auctionBook{}
}

// Insert appends the order to the appropriate side. Insertion order
// captures time priority; price priority is re-derived at uncross.
func (ab *auctionBook) Insert(o *Order) {
	switch o.Side {
	case Buy:
		ab.BuyOrders = append(ab.BuyOrders, o)
	case Sell:
		ab.SellOrders = append(ab.SellOrders, o)
	}
}

// Remove drops the order by ID from the side it was inserted on. No-op
// if not present.
func (ab *auctionBook) Remove(orderID string, side Side) {
	list := &ab.BuyOrders
	if side == Sell {
		list = &ab.SellOrders
	}
	for i, o := range *list {
		if o.ID == orderID {
			*list = slices.Delete(*list, i, i+1)
			return
		}
	}
}

// Reset clears all orders. Used by snapshot restore.
func (ab *auctionBook) Reset() {
	ab.BuyOrders = nil
	ab.SellOrders = nil
}

// Len returns the total order count across both sides.
func (ab *auctionBook) Len() int {
	return len(ab.BuyOrders) + len(ab.SellOrders)
}
