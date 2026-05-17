package orderbook

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// OpenAuction transitions the orderbook into the opening auction phase.
// Subsequent PlaceOrder calls rest without matching until Uncross fires.
type OpenAuction struct {
	Symbol string
	Reason string
}

func (c OpenAuction) AggregateID() string {
	return AggregateID(c.Symbol)
}

// Uncross runs the equilibrium-price auction algorithm and flips back
// to continuous matching. The cross type (opening vs closing) and
// destination phase are derived from the current Phase.
type Uncross struct {
	Symbol string
}

func (c Uncross) AggregateID() string {
	return AggregateID(c.Symbol)
}

// ExecuteOpenAuction emits a MarketPhaseChanged(AUCTION) event. Valid
// only from CONTINUOUS or CLOSED — calling while already in an auction
// returns ErrAlreadyInAuction.
func ExecuteOpenAuction(book *OrderBook, cmd OpenAuction) ([]es.Event, error) {
	switch book.Phase {
	case PhaseAuction, PhaseClosingAuction:
		return nil, ErrAlreadyInAuction
	case PhaseContinuous, PhaseClosed:
		// ok
	default:
		return nil, ErrCannotOpenAuction
	}

	now := time.Now()
	reason := cmd.Reason
	if reason == "" {
		reason = "session_open"
	}

	evt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventMarketPhaseChanged,
		Timestamp:   now,
		Data: &orderbookv1.MarketPhaseChanged{
			Symbol: cmd.Symbol,
			Phase:  orderbookv1.MarketPhase_MARKET_PHASE_AUCTION,
			Reason: reason,
			At:     timestamppb.New(now),
		},
	}
	if err := book.Apply(evt); err != nil {
		return nil, fmt.Errorf("apply phase changed: %w", err)
	}
	return []es.Event{evt}, nil
}

// ExecuteUncross runs the auction uncross. From AUCTION, it produces
// opening-cross trades and flips to CONTINUOUS; from CLOSING_AUCTION
// (step 3) it produces closing-cross trades and flips to CLOSED.
func ExecuteUncross(book *OrderBook, cmd Uncross) ([]es.Event, error) {
	var ct CrossType
	var nextPhase MarketPhase

	switch book.Phase {
	case PhaseAuction:
		ct = CrossOpening
		nextPhase = PhaseContinuous
	case PhaseClosingAuction:
		ct = CrossClosing
		nextPhase = PhaseClosed
	default:
		return nil, ErrNotInAuction
	}

	now := time.Now()
	events, _ := uncross(book, ct, now)

	phaseEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventMarketPhaseChanged,
		Timestamp:   now,
		Data: &orderbookv1.MarketPhaseChanged{
			Symbol: book.Symbol,
			Phase:  MarketPhaseToProto(nextPhase),
			Reason: "uncross_complete",
			At:     timestamppb.New(now),
		},
	}
	if err := book.Apply(phaseEvt); err != nil {
		return nil, fmt.Errorf("apply phase changed: %w", err)
	}
	events = append(events, phaseEvt)

	// After an opening uncross, the clearing print may trigger pre-existing
	// stops. After a closing uncross, the book is dead (PhaseClosed) so
	// triggered stops would have nowhere to match — skip.
	if nextPhase == PhaseContinuous {
		events = triggerStops(book, events, now)
	}

	return events, nil
}

// auctionResult summarizes the outcome of an uncross — used by callers
// (and tests) to inspect what cleared without re-scanning the events.
type auctionResult struct {
	ClearingPrice int64
	MatchedQty    int64
	ImbalanceQty  int64
	ImbalanceSide Side
}

// uncross produces the events for an auction uncross: an AuctionUncrossed
// header, then per-pair TradeExecuted events at the clearing price, then
// any OCO sibling cancellations. Allocation walks both sides in
// price-time priority and skips self-trade pairs.
func uncross(book *OrderBook, ct CrossType, now time.Time) ([]es.Event, auctionResult) {
	res := computeClearing(book)

	auctionEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventAuctionUncrossed,
		Timestamp:   now,
		Data: &orderbookv1.AuctionUncrossed{
			Symbol:        book.Symbol,
			ClearingPrice: res.ClearingPrice,
			MatchedQty:    res.MatchedQty,
			ImbalanceQty:  res.ImbalanceQty,
			ImbalanceSide: SideToProto(res.ImbalanceSide),
			CrossType:     CrossTypeToProto(ct),
			At:            timestamppb.New(now),
		},
	}
	_ = book.Apply(auctionEvt)
	events := []es.Event{auctionEvt}

	if res.MatchedQty == 0 {
		return events, res
	}

	events = allocateUncross(book, res.ClearingPrice, ct, events, now)
	return events, res
}

// allocateUncross walks the bid and ask books at and through the
// clearing price, pairing orders in price-time priority. Each pair
// trades at clearing_price (uniform-price auction). Self-trade pairs
// are not paired — the scan looks past them so the buy/sell can still
// match against another account on the opposite side.
//
// The realized matched quantity may be less than the headline
// matched_qty when self-trade prevention blocks some pairings.
func allocateUncross(book *OrderBook, clearing int64, ct CrossType, events []es.Event, now time.Time) []es.Event {
	// Snapshot pointers before mutating the priceSide index.
	var buys []*Order
	for o := range book.Bids.All() {
		if o.Price >= clearing {
			buys = append(buys, o)
		}
	}
	var sells []*Order
	for o := range book.Asks.All() {
		if o.Price <= clearing {
			sells = append(sells, o)
		}
	}

	for i := 0; i < len(buys); i++ {
		b := buys[i]
		for b.RemainingQty > 0 {
			if _, ok := book.Orders[b.ID]; !ok {
				break // cancelled mid-allocation (OCO sibling)
			}

			// Find the highest-priority sell that's still on the book,
			// has remaining qty, and isn't a self-trade against b.
			matched := -1
			for k := 0; k < len(sells); k++ {
				s := sells[k]
				if _, ok := book.Orders[s.ID]; !ok {
					continue
				}
				if s.RemainingQty <= 0 {
					continue
				}
				if b.AccountID != "" && b.AccountID == s.AccountID {
					continue
				}
				matched = k
				break
			}
			if matched == -1 {
				break // no eligible sell left for this buy
			}
			s := sells[matched]

			qty := minInt64(b.RemainingQty, s.RemainingQty)
			tradeEvt := es.Event{
				AggregateID: book.AggregateID(),
				Type:        EventTradeExecuted,
				Timestamp:   now,
				Data: &orderbookv1.TradeExecuted{
					TradeId:     uuid.New().String(),
					BuyOrderId:  b.ID,
					SellOrderId: s.ID,
					Symbol:      book.Symbol,
					Price:       clearing,
					Quantity:    qty,
					ExecutedAt:  timestamppb.New(now),
					CrossType:   CrossTypeToProto(ct),
				},
			}
			_ = book.Apply(tradeEvt)
			events = append(events, tradeEvt)

			// OCO siblings of any filled order get cancelled. cancelOCOSiblings
			// removes the winner from its own group so we don't double-fire on
			// a subsequent partial fill of the same order.
			if b.OCOGroupID != "" {
				events = cancelOCOSiblings(book, b, events, now)
			}
			if s.OCOGroupID != "" {
				events = cancelOCOSiblings(book, s, events, now)
			}
		}
	}

	return events
}

// computeClearing finds the equilibrium clearing price using cumulative
// supply/demand curves over the regular limit book.
//
// Algorithm:
//  1. Candidate prices = all distinct limit prices on either side.
//  2. For each candidate p, matched(p) = min(buyQty≥p, sellQty≤p).
//  3. Take candidates with max matched.
//  4. Among those, take candidates with min |imbalance|.
//  5. Tie-break by imbalance direction: buy-heavy → highest price,
//     sell-heavy → lowest price, balanced → reference (last trade) if
//     within range, else midpoint.
func computeClearing(book *OrderBook) auctionResult {
	bids := collectLevels(book.Bids)
	asks := collectLevels(book.Asks)

	// One-sided or empty book: no crossing possible, but report the
	// standing imbalance so consumers can see how lopsided things are.
	if len(bids) == 0 || len(asks) == 0 {
		var r auctionResult
		switch {
		case len(bids) > 0:
			r.ImbalanceSide = Buy
			r.ImbalanceQty = totalLevelQty(bids)
		case len(asks) > 0:
			r.ImbalanceSide = Sell
			r.ImbalanceQty = totalLevelQty(asks)
		}
		return r
	}

	// Distinct candidate prices across both sides.
	seen := make(map[int64]struct{}, len(bids)+len(asks))
	candidates := make([]int64, 0, len(bids)+len(asks))
	for _, l := range bids {
		if _, ok := seen[l.price]; !ok {
			seen[l.price] = struct{}{}
			candidates = append(candidates, l.price)
		}
	}
	for _, l := range asks {
		if _, ok := seen[l.price]; !ok {
			seen[l.price] = struct{}{}
			candidates = append(candidates, l.price)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })

	type cand struct {
		price     int64
		matched   int64
		imbalance int64 // signed: buyQty - sellQty
	}
	cs := make([]cand, 0, len(candidates))
	var best int64
	for _, p := range candidates {
		bq := cumulativeAtOrBetter(bids, p, true)
		sq := cumulativeAtOrBetter(asks, p, false)
		m := minInt64(bq, sq)
		cs = append(cs, cand{price: p, matched: m, imbalance: bq - sq})
		if m > best {
			best = m
		}
	}

	if best == 0 {
		// Both sides exist but the best bid is below the best ask — no
		// equilibrium price exists. Report no match; imbalance covers
		// the (smaller) side that would have crossed.
		return auctionResult{}
	}

	maxMatched := cs[:0]
	for _, c := range cs {
		if c.matched == best {
			maxMatched = append(maxMatched, c)
		}
	}

	minAbs := absInt64(maxMatched[0].imbalance)
	for _, c := range maxMatched {
		if a := absInt64(c.imbalance); a < minAbs {
			minAbs = a
		}
	}

	finalists := make([]cand, 0, len(maxMatched))
	for _, c := range maxMatched {
		if absInt64(c.imbalance) == minAbs {
			finalists = append(finalists, c)
		}
	}

	var chosen cand
	if len(finalists) == 1 {
		chosen = finalists[0]
	} else {
		anyPositive := false
		anyNegative := false
		for _, c := range finalists {
			if c.imbalance > 0 {
				anyPositive = true
			} else if c.imbalance < 0 {
				anyNegative = true
			}
		}

		switch {
		case anyPositive && !anyNegative:
			// Buy-heavy throughout: pick the highest price (rewards sellers).
			chosen = finalists[0]
			for _, c := range finalists {
				if c.price > chosen.price {
					chosen = c
				}
			}
		case anyNegative && !anyPositive:
			// Sell-heavy throughout: pick the lowest price (rewards buyers).
			chosen = finalists[0]
			for _, c := range finalists {
				if c.price < chosen.price {
					chosen = c
				}
			}
		default:
			// Balanced or mixed-sign across finalists: prefer the reference
			// price (last trade) if it lies within the range, else use the
			// midpoint. Pick the finalist closest to the target.
			minP, maxP := finalists[0].price, finalists[0].price
			for _, c := range finalists {
				if c.price < minP {
					minP = c.price
				}
				if c.price > maxP {
					maxP = c.price
				}
			}
			target := book.LastTradePrice
			if target < minP || target > maxP {
				target = (minP + maxP) / 2
			}
			chosen = finalists[0]
			bestDist := absInt64(chosen.price - target)
			for _, c := range finalists {
				if d := absInt64(c.price - target); d < bestDist {
					bestDist = d
					chosen = c
				}
			}
		}
	}

	res := auctionResult{
		ClearingPrice: chosen.price,
		MatchedQty:    chosen.matched,
	}
	switch {
	case chosen.imbalance > 0:
		res.ImbalanceQty = chosen.imbalance
		res.ImbalanceSide = Buy
	case chosen.imbalance < 0:
		res.ImbalanceQty = -chosen.imbalance
		res.ImbalanceSide = Sell
	}
	return res
}

type priceLevel struct {
	price int64
	qty   int64
}

// collectLevels aggregates RemainingQty per price level, in priority
// order (best first). The aggregation is necessary because priceSide
// iterates individual orders, but the uncross curves only care about
// total quantity per level.
func collectLevels(side *priceSide) []priceLevel {
	var out []priceLevel
	for o := range side.All() {
		if n := len(out); n > 0 && out[n-1].price == o.Price {
			out[n-1].qty += o.RemainingQty
			continue
		}
		out = append(out, priceLevel{price: o.Price, qty: o.RemainingQty})
	}
	return out
}

func totalLevelQty(levels []priceLevel) int64 {
	var sum int64
	for _, l := range levels {
		sum += l.qty
	}
	return sum
}

// cumulativeAtOrBetter sums quantity for levels eligible at clearing p.
// Bids are eligible at price ≥ p; asks at price ≤ p.
func cumulativeAtOrBetter(levels []priceLevel, p int64, isBuy bool) int64 {
	var sum int64
	for _, l := range levels {
		if isBuy {
			if l.price >= p {
				sum += l.qty
			}
		} else {
			if l.price <= p {
				sum += l.qty
			}
		}
	}
	return sum
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func absInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
