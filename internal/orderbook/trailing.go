package orderbook

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// validateTrailingParams enforces the "exactly one of trail_amount /
// trail_offset_bps, both > 0" rule for trailing stops, plus the
// limit_offset requirement for the limit variant.
func validateTrailingParams(cmd PlaceOrder, isLimit bool) error {
	hasAmount := cmd.TrailAmount > 0
	hasBps := cmd.TrailOffsetBps > 0
	if !hasAmount && !hasBps {
		return ErrTrailingStopRequiresTrail
	}
	if hasAmount && hasBps {
		return ErrTrailingStopAmbiguousTrail
	}
	if isLimit {
		if cmd.LimitOffset <= 0 {
			return ErrTrailingStopLimitRequiresOffset
		}
	} else if cmd.LimitOffset != 0 {
		return ErrTrailingStopRejectsLimitOffset
	}
	return nil
}

// trailingStopTrigger returns the trigger-price candidate implied by
// the given mark for this trailing order. A SELL trail at mark=M with
// amount=A becomes max(currentStop, M-A); BUY trail becomes
// min(currentStop, M+A). With bps, the absolute trail is M * bps / 10000.
func trailingStopTrigger(order *Order, mark int64) int64 {
	trail := order.TrailAmount
	if trail == 0 && order.TrailOffsetBps > 0 {
		trail = mark * int64(order.TrailOffsetBps) / 10000
	}
	if trail <= 0 {
		return order.StopPrice
	}
	switch order.Side {
	case Sell:
		candidate := mark - trail
		if candidate > order.StopPrice {
			return candidate
		}
	case Buy:
		candidate := mark + trail
		if candidate < order.StopPrice {
			return candidate
		}
	}
	return order.StopPrice
}

// ratchetTrailingStops checks every resting trailing stop and emits a
// TrailingStopAdjusted event for each one whose trigger should ratchet
// tighter at the given mark. The aggregate's Apply handles reseating in
// the stop side and updating StopPrice.
func ratchetTrailingStops(book *OrderBook, mark int64, events []es.Event, now time.Time) []es.Event {
	if mark <= 0 {
		return events
	}
	// Snapshot order IDs first since Apply mutates the stop sides.
	var candidates []*Order
	for o := range book.BuyStops.All() {
		if o.OrderType.IsTrailingStop() {
			candidates = append(candidates, o)
		}
	}
	for o := range book.SellStops.All() {
		if o.OrderType.IsTrailingStop() {
			candidates = append(candidates, o)
		}
	}
	for _, o := range candidates {
		next := trailingStopTrigger(o, mark)
		if next == o.StopPrice {
			continue
		}
		evt := es.Event{
			AggregateID: book.AggregateID(),
			Type:        EventTrailingStopAdjusted,
			Timestamp:   now,
			Data: &orderbookv1.TrailingStopAdjusted{
				OrderId:           o.ID,
				Symbol:            book.Symbol,
				PreviousStopPrice: o.StopPrice,
				NewStopPrice:      next,
				MarkPrice:         mark,
				AdjustedAt:        timestamppb.New(now),
			},
		}
		book.Apply(evt)
		events = append(events, evt)
	}
	return events
}
