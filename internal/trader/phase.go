package trader

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
)

// PhaseWatcher polls GetOrderBook on a fixed interval and exposes the
// most-recently-observed market phase via atomic reads. Strategies
// consult it before acting so they don't try to place orders the server
// would reject during an auction (or the closed phase).
//
// Polling is fine for now — a server-pushed phase stream would be a
// natural follow-up but isn't needed for the toy strategies.
type PhaseWatcher struct {
	symbol string
	phase  atomic.Int32 // stores orderbookv1.MarketPhase as int32
}

func NewPhaseWatcher(symbol string) *PhaseWatcher {
	pw := &PhaseWatcher{symbol: symbol}
	// Default to CONTINUOUS so a watcher that hasn't ticked yet doesn't
	// block strategies that come up before the first poll completes.
	pw.phase.Store(int32(orderbookv1.MarketPhase_MARKET_PHASE_CONTINUOUS))
	return pw
}

func (pw *PhaseWatcher) Phase() orderbookv1.MarketPhase {
	return orderbookv1.MarketPhase(pw.phase.Load())
}

// IsContinuous reports whether the market is open for continuous trading.
// The zero value (UNSPECIFIED) — which the server returns for aggregates
// that have never seen a MarketPhaseChanged event — is treated as
// continuous so legacy symbols Just Work.
func (pw *PhaseWatcher) IsContinuous() bool {
	p := pw.Phase()
	return p == orderbookv1.MarketPhase_MARKET_PHASE_CONTINUOUS ||
		p == orderbookv1.MarketPhase_MARKET_PHASE_UNSPECIFIED
}

func (pw *PhaseWatcher) IsClosed() bool {
	return pw.Phase() == orderbookv1.MarketPhase_MARKET_PHASE_CLOSED
}

func (pw *PhaseWatcher) IsAuction() bool {
	p := pw.Phase()
	return p == orderbookv1.MarketPhase_MARKET_PHASE_AUCTION ||
		p == orderbookv1.MarketPhase_MARKET_PHASE_CLOSING_AUCTION
}

// IsHalted reports whether the symbol is in a full LULD trading halt —
// every new order is rejected until a reopening auction completes.
func (pw *PhaseWatcher) IsHalted() bool {
	return pw.Phase() == orderbookv1.MarketPhase_MARKET_PHASE_HALTED
}

// IsLimitState reports whether the symbol is in an LULD limit state —
// limits can rest at-or-better than the band but through-the-band
// orders are rejected.
func (pw *PhaseWatcher) IsLimitState() bool {
	return pw.Phase() == orderbookv1.MarketPhase_MARKET_PHASE_LIMIT_STATE
}

// CanTrade reports whether the bot can quote / cross / place orders
// right now. False during auctions, halts, limit states, and after
// close. Used by mm/noise/trend to short-circuit their cycle.
func (pw *PhaseWatcher) CanTrade() bool {
	return pw.IsContinuous()
}

// Watch blocks until ctx is cancelled, polling GetOrderBook on the
// given interval. It seeds an immediate poll before starting the timer.
func (pw *PhaseWatcher) Watch(
	ctx context.Context,
	client orderbookv1connect.OrderBookServiceClient,
	interval time.Duration,
	log *slog.Logger,
) {
	pw.tick(ctx, client, log)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pw.tick(ctx, client, log)
		}
	}
}

func (pw *PhaseWatcher) tick(
	ctx context.Context,
	client orderbookv1connect.OrderBookServiceClient,
	log *slog.Logger,
) {
	resp, err := client.GetOrderBook(ctx, connect.NewRequest(&orderbookv1.GetOrderBookRequest{
		Symbol: pw.symbol,
	}))
	if err != nil {
		log.Debug("phase poll failed", "symbol", pw.symbol, "error", err)
		return
	}
	next := resp.Msg.Phase
	if next == orderbookv1.MarketPhase_MARKET_PHASE_UNSPECIFIED {
		next = orderbookv1.MarketPhase_MARKET_PHASE_CONTINUOUS
	}
	prev := pw.Phase()
	if next != prev {
		log.Info("market phase changed", "symbol", pw.symbol, "from", prev.String(), "to", next.String())
		pw.phase.Store(int32(next))
	}
}
