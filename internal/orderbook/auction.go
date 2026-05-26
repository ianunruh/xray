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

// BeginClosingAuction freezes continuous matching and enters the
// closing-auction phase. Only AT_CLOSE orders and cancellations are
// accepted from here until Uncross fires.
type BeginClosingAuction struct {
	Symbol string
	Reason string
}

func (c BeginClosingAuction) AggregateID() string {
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
// from CONTINUOUS, CLOSED, or HALTED — calling while already in an
// auction returns ErrAlreadyInAuction. The HALTED path is the
// LULD halt-reopen entry point; the subsequent Uncross emits a
// TradingResumed marker (see ExecuteUncross).
func ExecuteOpenAuction(book *OrderBook, cmd OpenAuction) ([]es.Event, error) {
	switch book.Phase {
	case PhaseAuction, PhaseClosingAuction:
		return nil, ErrAlreadyInAuction
	case PhaseContinuous, PhaseClosed, PhaseHalted:
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

// ExecuteBeginClosingAuction emits MarketPhaseChanged(CLOSING_AUCTION).
// Valid only from CONTINUOUS.
func ExecuteBeginClosingAuction(book *OrderBook, cmd BeginClosingAuction) ([]es.Event, error) {
	if book.Phase != PhaseContinuous {
		return nil, ErrCannotBeginClosing
	}

	now := time.Now()
	reason := cmd.Reason
	if reason == "" {
		reason = "session_close"
	}

	evt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventMarketPhaseChanged,
		Timestamp:   now,
		Data: &orderbookv1.MarketPhaseChanged{
			Symbol: cmd.Symbol,
			Phase:  orderbookv1.MarketPhase_MARKET_PHASE_CLOSING_AUCTION,
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
// it produces closing-cross trades and flips to CLOSED. When the
// auction was a halt-reopen (LULDHaltStartedAt is non-zero), the
// cross is tagged CROSS_TYPE_HALT_REOPEN and a TradingResumed marker
// is appended after the phase change — that marker clears the halt
// timer fields and arms LULDRearmAt (a brief window during which the
// matcher does not enforce LULD bands so the reopening prints can
// settle).
func ExecuteUncross(book *OrderBook, cmd Uncross) ([]es.Event, error) {
	var ct CrossType
	var nextPhase MarketPhase
	isHaltReopen := false

	switch book.Phase {
	case PhaseAuction:
		if !book.LULDHaltStartedAt.IsZero() {
			ct = CrossHaltReopen
			isHaltReopen = true
		} else {
			ct = CrossOpening
		}
		nextPhase = PhaseContinuous
	case PhaseClosingAuction:
		ct = CrossClosing
		nextPhase = PhaseClosed
	default:
		return nil, ErrNotInAuction
	}

	now := time.Now()
	events, res := uncross(book, ct, now)

	phaseReason := "uncross_complete"
	if isHaltReopen {
		phaseReason = "halt_reopen_complete"
	}
	phaseEvt := es.Event{
		AggregateID: book.AggregateID(),
		Type:        EventMarketPhaseChanged,
		Timestamp:   now,
		Data: &orderbookv1.MarketPhaseChanged{
			Symbol: book.Symbol,
			Phase:  MarketPhaseToProto(nextPhase),
			Reason: phaseReason,
			At:     timestamppb.New(now),
		},
	}
	if err := book.Apply(phaseEvt); err != nil {
		return nil, fmt.Errorf("apply phase changed: %w", err)
	}
	events = append(events, phaseEvt)

	if isHaltReopen {
		resumedEvt := es.Event{
			AggregateID: book.AggregateID(),
			Type:        EventTradingResumed,
			Timestamp:   now,
			Data: &orderbookv1.TradingResumed{
				Symbol:    book.Symbol,
				At:        timestamppb.New(now),
				CrossType: CrossTypeToProto(CrossHaltReopen),
			},
		}
		if err := book.Apply(resumedEvt); err != nil {
			return nil, fmt.Errorf("apply trading resumed: %w", err)
		}
		events = append(events, resumedEvt)
	}

	// After an opening uncross, the clearing print may trigger pre-existing
	// stops. After a closing uncross, the book is dead (PhaseClosed) so
	// triggered stops would have nowhere to match — skip.
	if nextPhase == PhaseContinuous {
		events = triggerStops(book, events, now)
	}

	// Emit the canonical end-of-day mark on closing uncross, even if
	// matched_qty is zero (consumers still want to know the session
	// ended). When no trades cleared, fall back to the last continuous
	// trade price as the close.
	if ct == CrossClosing {
		closePrice := res.ClearingPrice
		if closePrice == 0 {
			closePrice = book.LastTradePrice
		}
		closeEvt := es.Event{
			AggregateID: book.AggregateID(),
			Type:        EventOfficialCloseSet,
			Timestamp:   now,
			Data: &orderbookv1.OfficialCloseSet{
				Symbol:      book.Symbol,
				SessionDate: now.UTC().Format("2006-01-02"),
				ClosePrice:  closePrice,
				CloseVolume: res.MatchedQty,
				At:          timestamppb.New(now),
			},
		}
		_ = book.Apply(closeEvt)
		events = append(events, closeEvt)
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
// any OCO sibling cancellations, then "missed_auction" cancellations for
// AT_OPEN/AT_CLOSE orders that didn't fill. Allocation walks both sides
// in price-time priority and skips self-trade pairs.
func uncross(book *OrderBook, ct CrossType, now time.Time) ([]es.Event, auctionResult) {
	res := computeClearing(book, ct)

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

	if res.MatchedQty > 0 {
		events = allocateUncross(book, res.ClearingPrice, ct, events, now)
	}

	// Sweep any AT_OPEN/AT_CLOSE orders that didn't fill. They're bound
	// to this single auction and don't carry over.
	events = cancelMissedAuctionOrders(book, ct, events, now)

	return events, res
}

// cancelMissedAuctionOrders emits OrderCancelled{reason:"missed_auction"}
// for AT_OPEN/AT_CLOSE orders still resting on their auction book. Run
// after allocation so any orders that filled (and were removed from the
// auction book by applyOrderCancelled / applyTradeExecuted bookkeeping)
// are skipped.
func cancelMissedAuctionOrders(book *OrderBook, ct CrossType, events []es.Event, now time.Time) []es.Event {
	var ab *auctionBook
	switch ct {
	case CrossOpening:
		ab = book.OpeningBook
	case CrossClosing:
		ab = book.ClosingBook
	}
	if ab == nil {
		return events
	}

	// Snapshot first: cancelling each order mutates the slice via
	// applyOrderCancelled.
	pending := make([]*Order, 0, len(ab.BuyOrders)+len(ab.SellOrders))
	pending = append(pending, ab.BuyOrders...)
	pending = append(pending, ab.SellOrders...)

	for _, o := range pending {
		if _, ok := book.Orders[o.ID]; !ok {
			continue
		}
		if o.RemainingQty <= 0 {
			continue
		}
		cancelEvt := es.Event{
			AggregateID: book.AggregateID(),
			Type:        EventOrderCancelled,
			Timestamp:   now,
			Data: &orderbookv1.OrderCancelled{
				OrderId: o.ID,
				Symbol:  book.Symbol,
				Reason:  "missed_auction",
			},
		}
		_ = book.Apply(cancelEvt)
		events = append(events, cancelEvt)
	}
	return events
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
	buys, sells := eligibleAuctionOrders(book, clearing, ct)

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
// supply/demand curves. It merges the continuous limit book with the
// auction-only AT_OPEN/AT_CLOSE book for the current cross type.
//
// Algorithm:
//  1. Candidate prices = all distinct limit prices on either side.
//  2. For each candidate p, matched(p) = min(buyQty≥p+marketBuyQty,
//     sellQty≤p+marketSellQty).
//  3. Take candidates with max matched.
//  4. Among those, take candidates with min |imbalance|.
//  5. Tie-break by imbalance direction: buy-heavy → highest price,
//     sell-heavy → lowest price, balanced → reference (last trade) if
//     within range, else midpoint.
//
// Edge case: when only market orders exist (no limit prices on either
// side), fall back to LastTradePrice as the clearing price. With no
// reference at all, no clearing is possible — emit zero matched and
// the caller will cancel market orders as "missed_auction".
func computeClearing(book *OrderBook, ct CrossType) auctionResult {
	inp := collectUncrossInputs(book, ct)

	totalBuy := totalLevelQty(inp.bidLevels) + inp.marketBuyQty
	totalSell := totalLevelQty(inp.askLevels) + inp.marketSellQty

	// One-sided or empty book: no crossing possible, but report the
	// standing imbalance so consumers can see how lopsided things are.
	if totalBuy == 0 || totalSell == 0 {
		var r auctionResult
		switch {
		case totalBuy > 0:
			r.ImbalanceSide = Buy
			r.ImbalanceQty = totalBuy
		case totalSell > 0:
			r.ImbalanceSide = Sell
			r.ImbalanceQty = totalSell
		}
		return r
	}

	// Pure-market crosses use LastTradePrice as the reference. If we've
	// never traded, there's no fair reference — bail out.
	if len(inp.bidLevels) == 0 && len(inp.askLevels) == 0 {
		if book.LastTradePrice == 0 {
			return auctionResult{}
		}
		matched := minInt64(inp.marketBuyQty, inp.marketSellQty)
		imb := inp.marketBuyQty - inp.marketSellQty
		res := auctionResult{ClearingPrice: book.LastTradePrice, MatchedQty: matched}
		switch {
		case imb > 0:
			res.ImbalanceQty = imb
			res.ImbalanceSide = Buy
		case imb < 0:
			res.ImbalanceQty = -imb
			res.ImbalanceSide = Sell
		}
		return res
	}

	// Distinct candidate prices across both sides. Markets contribute
	// quantity but no price candidates.
	seen := make(map[int64]struct{}, len(inp.bidLevels)+len(inp.askLevels))
	candidates := make([]int64, 0, len(inp.bidLevels)+len(inp.askLevels))
	for _, l := range inp.bidLevels {
		if _, ok := seen[l.price]; !ok {
			seen[l.price] = struct{}{}
			candidates = append(candidates, l.price)
		}
	}
	for _, l := range inp.askLevels {
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
		bq := cumulativeAtOrBetter(inp.bidLevels, p, true) + inp.marketBuyQty
		sq := cumulativeAtOrBetter(inp.askLevels, p, false) + inp.marketSellQty
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

// uncrossInputs is the merged view of continuous + auction-only orders
// that the equilibrium-price algorithm operates on.
type uncrossInputs struct {
	bidLevels     []priceLevel // aggregated limit bids, highest first
	askLevels     []priceLevel // aggregated limit asks, lowest first
	marketBuyQty  int64        // total qty of market AT_OPEN/AT_CLOSE buys
	marketSellQty int64
}

// collectUncrossInputs merges the continuous Bids/Asks with the
// AT_OPEN/AT_CLOSE orders in the appropriate auction book, aggregating
// by price level and separating market orders (which fill at any price).
func collectUncrossInputs(book *OrderBook, ct CrossType) uncrossInputs {
	var ab *auctionBook
	switch ct {
	case CrossOpening:
		ab = book.OpeningBook
	case CrossClosing:
		ab = book.ClosingBook
	}

	inp := uncrossInputs{}
	bidsByPrice := make(map[int64]int64)
	for o := range book.Bids.All() {
		bidsByPrice[o.Price] += o.RemainingQty
	}
	if ab != nil {
		for _, o := range ab.BuyOrders {
			switch o.OrderType {
			case Limit:
				bidsByPrice[o.Price] += o.RemainingQty
			case Market:
				inp.marketBuyQty += o.RemainingQty
			}
		}
	}
	for p, q := range bidsByPrice {
		inp.bidLevels = append(inp.bidLevels, priceLevel{price: p, qty: q})
	}
	sort.Slice(inp.bidLevels, func(i, j int) bool {
		return inp.bidLevels[i].price > inp.bidLevels[j].price
	})

	asksByPrice := make(map[int64]int64)
	for o := range book.Asks.All() {
		asksByPrice[o.Price] += o.RemainingQty
	}
	if ab != nil {
		for _, o := range ab.SellOrders {
			switch o.OrderType {
			case Limit:
				asksByPrice[o.Price] += o.RemainingQty
			case Market:
				inp.marketSellQty += o.RemainingQty
			}
		}
	}
	for p, q := range asksByPrice {
		inp.askLevels = append(inp.askLevels, priceLevel{price: p, qty: q})
	}
	sort.Slice(inp.askLevels, func(i, j int) bool {
		return inp.askLevels[i].price < inp.askLevels[j].price
	})

	return inp
}

// eligibleAuctionOrders returns the buys and sells eligible to trade
// at the clearing price, in priority order: market AT_OPEN/AT_CLOSE
// first (by placement time), then limits in price-time priority
// (continuous and auction-only limits merged).
func eligibleAuctionOrders(book *OrderBook, clearing int64, ct CrossType) (buys, sells []*Order) {
	var ab *auctionBook
	switch ct {
	case CrossOpening:
		ab = book.OpeningBook
	case CrossClosing:
		ab = book.ClosingBook
	}

	var marketBuys, marketSells []*Order
	var auctionLimitBuys, auctionLimitSells []*Order
	if ab != nil {
		for _, o := range ab.BuyOrders {
			switch {
			case o.OrderType == Market:
				marketBuys = append(marketBuys, o)
			case o.OrderType == Limit && o.Price >= clearing:
				auctionLimitBuys = append(auctionLimitBuys, o)
			}
		}
		for _, o := range ab.SellOrders {
			switch {
			case o.OrderType == Market:
				marketSells = append(marketSells, o)
			case o.OrderType == Limit && o.Price <= clearing:
				auctionLimitSells = append(auctionLimitSells, o)
			}
		}
	}
	sort.SliceStable(marketBuys, func(i, j int) bool {
		return marketBuys[i].PlacedAt.Before(marketBuys[j].PlacedAt)
	})
	sort.SliceStable(marketSells, func(i, j int) bool {
		return marketSells[i].PlacedAt.Before(marketSells[j].PlacedAt)
	})

	limitBuys := make([]*Order, 0)
	for o := range book.Bids.All() {
		if o.Price >= clearing {
			limitBuys = append(limitBuys, o)
		}
	}
	limitBuys = append(limitBuys, auctionLimitBuys...)
	sort.SliceStable(limitBuys, func(i, j int) bool {
		if limitBuys[i].Price != limitBuys[j].Price {
			return limitBuys[i].Price > limitBuys[j].Price
		}
		return limitBuys[i].PlacedAt.Before(limitBuys[j].PlacedAt)
	})

	limitSells := make([]*Order, 0)
	for o := range book.Asks.All() {
		if o.Price <= clearing {
			limitSells = append(limitSells, o)
		}
	}
	limitSells = append(limitSells, auctionLimitSells...)
	sort.SliceStable(limitSells, func(i, j int) bool {
		if limitSells[i].Price != limitSells[j].Price {
			return limitSells[i].Price < limitSells[j].Price
		}
		return limitSells[i].PlacedAt.Before(limitSells[j].PlacedAt)
	})

	buys = append(marketBuys, limitBuys...)
	sells = append(marketSells, limitSells...)
	return buys, sells
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
