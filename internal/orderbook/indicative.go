package orderbook

// IndicativeState is the live "what would uncross do right now" view
// for a symbol in an auction phase. The fields mirror auctionResult
// plus the symbol + phase context callers need to render the answer
// without separately querying the aggregate.
type IndicativeState struct {
	Symbol        string
	Phase         MarketPhase
	ClearingPrice int64
	MatchedQty    int64
	ImbalanceQty  int64
	ImbalanceSide Side
}

// ComputeIndicative runs the equilibrium-price computation against the
// auction book appropriate for the current phase, with no side effects
// on the book. Returns nil when the orderbook is not in an auction
// phase — callers use that as a "no banner needed" signal.
//
// The math is exactly what ExecuteUncross would run if invoked now;
// the projection diverges only in skipping the event emission and the
// downstream allocation walk.
func ComputeIndicative(book *OrderBook) *IndicativeState {
	var ct CrossType
	switch book.Phase {
	case PhaseAuction:
		ct = CrossOpening
	case PhaseClosingAuction:
		ct = CrossClosing
	default:
		return nil
	}
	res := computeClearing(book, ct)
	return &IndicativeState{
		Symbol:        book.Symbol,
		Phase:         book.Phase,
		ClearingPrice: res.ClearingPrice,
		MatchedQty:    res.MatchedQty,
		ImbalanceQty:  res.ImbalanceQty,
		ImbalanceSide: res.ImbalanceSide,
	}
}
