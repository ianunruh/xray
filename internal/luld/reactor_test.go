package luld_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ianunruh/xray/internal/luld"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

// stubReference returns a fixed reference price for one symbol.
type stubReference struct {
	price int64
	ok    bool
}

func (s stubReference) GetReference(_ string, _ time.Time) (int64, bool) { return s.price, s.ok }

type env struct {
	ctx       context.Context
	obHandler *es.Handler[*orderbook.OrderBook]
	active    *luld.InMemoryActiveSymbols
	reactor   *luld.Reactor
}

func newEnv(t *testing.T, ref luld.ReferenceReader) *env {
	t.Helper()
	registry := es.NewRegistry()
	orderbook.RegisterEvents(registry)

	store := memstore.New()
	obHandler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())

	tiers := luld.NewTiers(map[string]int32{"AAPL": 500}, 1000)
	active := luld.NewInMemoryActiveSymbols()
	reactor := luld.NewReactor(obHandler, ref, active, tiers, luld.Config{
		HaltDuration: 5 * time.Minute,
	}, slog.Default())

	return &env{ctx: context.Background(), obHandler: obHandler, active: active, reactor: reactor}
}

// TestTiers_BandBpsDefault verifies tier lookup falls back to the
// default for unknown symbols.
func TestTiers_BandBpsDefault(t *testing.T) {
	tiers := luld.NewTiers(map[string]int32{"AAPL": 500}, 1000)
	assert.Equal(t, int32(500), tiers.BandBps("AAPL"))
	assert.Equal(t, int32(1000), tiers.BandBps("UNKNOWN"))
}

// TestComputeBands verifies the symmetric band math.
func TestComputeBands(t *testing.T) {
	lo, hi := luld.ComputeBands(1500000, 500) // ±5%
	assert.Equal(t, int64(1425000), lo)
	assert.Equal(t, int64(1575000), hi)

	// Zero / negative inputs return zero bands.
	lo, hi = luld.ComputeBands(0, 500)
	assert.Equal(t, int64(0), lo)
	assert.Equal(t, int64(0), hi)
}

// TestReactor_BandsUpdatedOnTrade verifies that a TradeExecuted at the
// reference price triggers a SetLULDBands command on the orderbook.
func TestReactor_BandsUpdatedOnTrade(t *testing.T) {
	e := newEnv(t, stubReference{price: 1500000, ok: true})

	require.NoError(t, e.reactor.HandleEvents(e.ctx, []es.Event{
		tradeEvent("AAPL", 1500000, 100),
	}))

	book, err := e.obHandler.Load(e.ctx, orderbook.AggregateID("AAPL"))
	require.NoError(t, err)
	assert.Equal(t, int64(1500000), book.LULDReferencePrice)
	assert.Equal(t, int64(1575000), book.LULDUpperBand)
	assert.Equal(t, int64(1425000), book.LULDLowerBand)
	assert.Equal(t, int32(500), book.LULDBandBps)
}

// TestReactor_BandsNoOpWhenReferenceMissing verifies that the reactor
// is silent when the reference projection has no data yet.
func TestReactor_BandsNoOpWhenReferenceMissing(t *testing.T) {
	e := newEnv(t, stubReference{ok: false})
	require.NoError(t, e.reactor.HandleEvents(e.ctx, []es.Event{
		tradeEvent("AAPL", 1500000, 100),
	}))

	book, err := e.obHandler.Load(e.ctx, orderbook.AggregateID("AAPL"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), book.LULDUpperBand)
}

// TestReactor_EvaluateExpiry_ResumesWhenSpreadInBand: at grace expiry
// with no through-band orders, the reactor lifts limit state.
func TestReactor_EvaluateExpiry_ResumesWhenSpreadInBand(t *testing.T) {
	e := newEnv(t, stubReference{price: 1500000, ok: true})
	now := time.Now()
	require.NoError(t, seedTrippedLimitState(t, e, "AAPL", 1500000, now))

	// Top of book is empty → in-band by default. Advance past deadline.
	require.NoError(t, e.reactor.EvaluateLULDExpiry(e.ctx, "AAPL",
		now.Add(orderbook.LULDLimitStateGrace).Add(1*time.Second)))

	book, err := e.obHandler.Load(e.ctx, orderbook.AggregateID("AAPL"))
	require.NoError(t, err)
	assert.Equal(t, orderbook.PhaseContinuous, book.Phase)
}

// TestReactor_EvaluateExpiry_EscalatesToHalt: at grace expiry with a
// through-band ask still resting, the reactor halts.
func TestReactor_EvaluateExpiry_EscalatesToHalt(t *testing.T) {
	e := newEnv(t, stubReference{price: 1500000, ok: true})
	now := time.Now()

	// Rest an ask above the upper band BEFORE the trip so it stays
	// resting when limit state activates.
	require.NoError(t, seedAsk(t, e, "AAPL", 1580000, 100))
	require.NoError(t, seedTrippedLimitState(t, e, "AAPL", 1500000, now))

	require.NoError(t, e.reactor.EvaluateLULDExpiry(e.ctx, "AAPL",
		now.Add(orderbook.LULDLimitStateGrace).Add(1*time.Second)))

	book, err := e.obHandler.Load(e.ctx, orderbook.AggregateID("AAPL"))
	require.NoError(t, err)
	assert.Equal(t, orderbook.PhaseHalted, book.Phase)
	assert.False(t, book.LULDReopenAt.IsZero())
}

// TestReactor_EvaluateExpiry_NoOpBeforeDeadline: before grace expires
// the reactor must not act, even when the top of book is pinned.
func TestReactor_EvaluateExpiry_NoOpBeforeDeadline(t *testing.T) {
	e := newEnv(t, stubReference{price: 1500000, ok: true})
	now := time.Now()
	require.NoError(t, seedTrippedLimitState(t, e, "AAPL", 1500000, now))

	require.NoError(t, e.reactor.EvaluateLULDExpiry(e.ctx, "AAPL",
		now.Add(orderbook.LULDLimitStateGrace/2)))

	book, err := e.obHandler.Load(e.ctx, orderbook.AggregateID("AAPL"))
	require.NoError(t, err)
	assert.Equal(t, orderbook.PhaseLimitState, book.Phase)
}

// TestActiveSymbols_Lifecycle verifies the watch set tracks
// LimitStateEntered → LimitStateExited and LimitStateEntered →
// TradingHalted → TradingResumed.
func TestActiveSymbols_Lifecycle(t *testing.T) {
	tracker := luld.NewInMemoryActiveSymbols()

	// Initially empty.
	assert.Empty(t, tracker.ListActiveSymbols(context.Background()))

	// Enter limit state.
	require.NoError(t, tracker.HandleEvents(context.Background(), []es.Event{
		limitStateEntered("AAPL"),
	}))
	active := tracker.ListActiveSymbols(context.Background())
	require.Len(t, active, 1)
	assert.Equal(t, "AAPL", active[0].Symbol)
	assert.Equal(t, luld.KindLimitState, active[0].Kind)

	// Recovery path: LimitStateExited with reason=price_returned_in_band
	// removes from active set.
	require.NoError(t, tracker.HandleEvents(context.Background(), []es.Event{
		limitStateExited("AAPL", "price_returned_in_band"),
	}))
	assert.Empty(t, tracker.ListActiveSymbols(context.Background()))

	// Halt path: enter limit state, then halt_triggered + TradingHalted
	// promotes to KindHalted.
	require.NoError(t, tracker.HandleEvents(context.Background(), []es.Event{
		limitStateEntered("MSFT"),
		limitStateExited("MSFT", "halt_triggered"),
		tradingHalted("MSFT"),
	}))
	active = tracker.ListActiveSymbols(context.Background())
	require.Len(t, active, 1)
	assert.Equal(t, "MSFT", active[0].Symbol)
	assert.Equal(t, luld.KindHalted, active[0].Kind)

	// Resume clears the halted symbol.
	require.NoError(t, tracker.HandleEvents(context.Background(), []es.Event{
		tradingResumed("MSFT"),
	}))
	assert.Empty(t, tracker.ListActiveSymbols(context.Background()))
}
