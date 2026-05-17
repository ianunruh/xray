package portfolio

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

// fakeMarker is a minimal in-memory Marker for testing buildMarginSnapshot.
type fakeMarker struct {
	prices map[string]int64
}

func (f fakeMarker) GetMarkPrice(symbol string) (int64, time.Time, bool) {
	p, ok := f.prices[symbol]
	if !ok {
		return 0, time.Time{}, false
	}
	return p, time.Now(), true
}

func TestBuildMarginSnapshot_LongOnly(t *testing.T) {
	p := NewPortfolio(AggregateID("acct-1"))
	p.CashBalance = 50_000_000
	p.Holdings["AAPL"] = &Holding{Quantity: 100, TotalCost: 150_000_000} // avg 1.5M

	marker := fakeMarker{prices: map[string]int64{"AAPL": 1_600_000}}
	resp := buildMarginSnapshot("acct-1", p, marker)

	assert.Equal(t, int64(50_000_000), resp.CashBalance)
	assert.Equal(t, int64(160_000_000), resp.LongMarketValue) // 100 * 1.6M
	assert.Equal(t, int64(0), resp.ShortLiability)
	// Long maintenance = 25% * 160M = 40M.
	assert.Equal(t, int64(40_000_000), resp.MaintenanceRequirement)
	assert.Equal(t, int64(40_000_000), resp.LongMaintenanceRequirement)
	// Equity = 50M cash + 160M long MV = 210M.
	assert.Equal(t, int64(210_000_000), resp.Equity)
	assert.Equal(t, int64(170_000_000), resp.MarginExcess) // 210 - 40
	assert.False(t, resp.MarginCall)

	require.Len(t, resp.Positions, 1)
	pos := resp.Positions[0]
	assert.Equal(t, "AAPL", pos.Symbol)
	assert.Equal(t, orderbookv1.PositionSide_POSITION_SIDE_LONG, pos.Side)
	assert.Equal(t, int64(1_500_000), pos.AvgPrice)
	assert.Equal(t, int64(1_600_000), pos.MarkPrice)
	assert.Equal(t, int64(160_000_000), pos.MarketValue)
	assert.Equal(t, int64(10_000_000), pos.UnrealizedPnl) // (1.6M - 1.5M) * 100
}

func TestBuildMarginSnapshot_ShortOnly_Healthy(t *testing.T) {
	p := NewPortfolio(AggregateID("acct-1"))
	p.CashBalance = 25_000_000
	p.CollateralPool = 75_000_000
	p.ProceedsPool = 150_000_000
	p.ShortPositions["AAPL"] = &ShortPosition{
		Quantity: 100, ProceedsHeld: 150_000_000,
		CollateralHeld: 75_000_000, AvgOpenPrice: 1_500_000,
	}

	// Mark at $140 — favorable for the short (mark < avg open).
	marker := fakeMarker{prices: map[string]int64{"AAPL": 1_400_000}}
	resp := buildMarginSnapshot("acct-1", p, marker)

	assert.Equal(t, int64(140_000_000), resp.ShortLiability)
	// Maintenance = 30% * 100 * 1.4M = 42M.
	assert.Equal(t, int64(42_000_000), resp.MaintenanceRequirement)
	// Equity = 25M + 75M + 150M - 140M = 110M.
	assert.Equal(t, int64(110_000_000), resp.Equity)
	// Excess = 110M - 42M = 68M.
	assert.Equal(t, int64(68_000_000), resp.MarginExcess)
	assert.False(t, resp.MarginCall)

	require.Len(t, resp.Positions, 1)
	pos := resp.Positions[0]
	assert.Equal(t, orderbookv1.PositionSide_POSITION_SIDE_SHORT, pos.Side)
	// Unrealized = (1.5M - 1.4M) * 100 = +10M (profit).
	assert.Equal(t, int64(10_000_000), pos.UnrealizedPnl)
}

func TestBuildMarginSnapshot_ShortOnly_MarginCall(t *testing.T) {
	p := NewPortfolio(AggregateID("acct-1"))
	p.CashBalance = 25_000_000
	p.CollateralPool = 75_000_000
	p.ProceedsPool = 150_000_000
	p.ShortPositions["AAPL"] = &ShortPosition{
		Quantity: 100, ProceedsHeld: 150_000_000,
		CollateralHeld: 75_000_000, AvgOpenPrice: 1_500_000,
	}

	// Mark spikes to $400 — short is deeply underwater.
	marker := fakeMarker{prices: map[string]int64{"AAPL": 4_000_000}}
	resp := buildMarginSnapshot("acct-1", p, marker)

	assert.Equal(t, int64(400_000_000), resp.ShortLiability)
	// Maintenance = 30% * 100 * 4M = 120M.
	assert.Equal(t, int64(120_000_000), resp.MaintenanceRequirement)
	// Equity = 25M + 75M + 150M - 400M = -150M.
	assert.Equal(t, int64(-150_000_000), resp.Equity)
	assert.Equal(t, int64(-270_000_000), resp.MarginExcess)
	assert.True(t, resp.MarginCall)
}

func TestBuildMarginSnapshot_Mixed(t *testing.T) {
	p := NewPortfolio(AggregateID("acct-1"))
	p.CashBalance = 100_000_000
	p.Holdings["NVDA"] = &Holding{Quantity: 50, TotalCost: 25_000_000}
	p.CollateralPool = 75_000_000
	p.ProceedsPool = 150_000_000
	p.ShortPositions["AAPL"] = &ShortPosition{
		Quantity: 100, ProceedsHeld: 150_000_000,
		CollateralHeld: 75_000_000, AvgOpenPrice: 1_500_000,
	}

	marker := fakeMarker{prices: map[string]int64{
		"NVDA": 600_000,
		"AAPL": 1_400_000,
	}}
	resp := buildMarginSnapshot("acct-1", p, marker)

	assert.Equal(t, int64(30_000_000), resp.LongMarketValue) // 50 * 600k
	assert.Equal(t, int64(140_000_000), resp.ShortLiability) // 100 * 1.4M
	// Long maint = 25% * 30M = 7.5M; short maint = 30% * 140M = 42M;
	// total = 49.5M.
	assert.Equal(t, int64(7_500_000), resp.LongMaintenanceRequirement)
	assert.Equal(t, int64(42_000_000), resp.ShortMaintenanceRequirement)
	assert.Equal(t, int64(49_500_000), resp.MaintenanceRequirement)

	// Equity = 100M + 75M coll + 150M proceeds + 30M long - 140M short = 215M.
	assert.Equal(t, int64(215_000_000), resp.Equity)
	assert.False(t, resp.MarginCall)
	assert.Len(t, resp.Positions, 2)
	// Long sorted before short by symbol order (AAPL < NVDA), but we
	// emit longs first then shorts within each group.
	assert.Equal(t, "NVDA", resp.Positions[0].Symbol)
	assert.Equal(t, "AAPL", resp.Positions[1].Symbol)
}

func TestBuildMarginSnapshot_MissingMarks(t *testing.T) {
	p := NewPortfolio(AggregateID("acct-1"))
	p.CashBalance = 50_000_000
	p.Holdings["AAPL"] = &Holding{Quantity: 100, TotalCost: 150_000_000}
	p.Holdings["NVDA"] = &Holding{Quantity: 20, TotalCost: 10_000_000}

	marker := fakeMarker{prices: map[string]int64{"AAPL": 1_600_000}}
	resp := buildMarginSnapshot("acct-1", p, marker)

	assert.Equal(t, []string{"NVDA"}, resp.MissingMarks)
	// NVDA contributes 0 to LongMarketValue.
	assert.Equal(t, int64(160_000_000), resp.LongMarketValue)

	// Find NVDA in positions and confirm mark_missing.
	var nvda *struct{ ok bool }
	for _, pos := range resp.Positions {
		if pos.Symbol == "NVDA" {
			assert.True(t, pos.MarkMissing)
			assert.Equal(t, int64(0), pos.MarketValue)
			nvda = &struct{ ok bool }{ok: true}
		}
	}
	require.NotNil(t, nvda, "NVDA position present in response")
}

func TestBuildMarginSnapshot_LongOnMargin(t *testing.T) {
	// Account deposited $50k cash, bought $100k of stock on margin.
	// Cash went negative by $50k (the loan); long MV = $100k.
	p := NewPortfolio(AggregateID("acct-1"))
	p.CashBalance = -50_000_000
	p.Holdings["AAPL"] = &Holding{Quantity: 100, TotalCost: 1_000_000_000}

	marker := fakeMarker{prices: map[string]int64{"AAPL": 10_000_000}}
	resp := buildMarginSnapshot("acct-1", p, marker)

	assert.Equal(t, int64(50_000_000), resp.MarginLoan)
	assert.Equal(t, int64(1_000_000_000), resp.LongMarketValue)
	// Long maintenance = 25% * 1B = 250M.
	assert.Equal(t, int64(250_000_000), resp.LongMaintenanceRequirement)
	assert.Equal(t, int64(250_000_000), resp.MaintenanceRequirement)
	// Equity = -50M cash + 1B long MV = 950M.
	assert.Equal(t, int64(950_000_000), resp.Equity)
	assert.Equal(t, int64(700_000_000), resp.MarginExcess)
	assert.False(t, resp.MarginCall)
	// Buying power = 2 * 700M = 1.4B.
	assert.Equal(t, int64(1_400_000_000), resp.BuyingPower)
}

func TestBuildMarginSnapshot_LongOnMargin_Breach(t *testing.T) {
	// Same as above but the stock fell to $4. Long MV = $400M;
	// equity = -50M + 400M = 350M. Maintenance = 25% * 400M = 100M.
	// Excess = 250M > 0, NOT breached at this level.
	// Let me push further: stock at $0.50 → MV = $50M.
	// Equity = -50M + 50M = 0. Maint = 25% * 50M = 12.5M.
	// Excess = -12.5M → breach.
	p := NewPortfolio(AggregateID("acct-1"))
	p.CashBalance = -50_000_000
	p.Holdings["AAPL"] = &Holding{Quantity: 100, TotalCost: 1_000_000_000}

	marker := fakeMarker{prices: map[string]int64{"AAPL": 500_000}}
	resp := buildMarginSnapshot("acct-1", p, marker)

	assert.Equal(t, int64(50_000_000), resp.MarginLoan)
	assert.Equal(t, int64(50_000_000), resp.LongMarketValue)
	assert.Equal(t, int64(12_500_000), resp.MaintenanceRequirement)
	assert.Equal(t, int64(0), resp.Equity) // -50M + 50M
	assert.Equal(t, int64(-12_500_000), resp.MarginExcess)
	assert.True(t, resp.MarginCall, "long-on-margin breach must fire a call")
	assert.Equal(t, int64(0), resp.BuyingPower)
}

func TestBuildMarginSnapshot_PreFillCollateralCounts(t *testing.T) {
	p := NewPortfolio(AggregateID("acct-1"))
	p.CashBalance = 25_000_000
	p.CollateralHeldBySaga["saga-1"] = &CollateralHold{
		Symbol: "AAPL", Quantity: 100, Amount: 75_000_000,
	}

	marker := fakeMarker{prices: map[string]int64{}}
	resp := buildMarginSnapshot("acct-1", p, marker)

	assert.Equal(t, int64(75_000_000), resp.CollateralHeldPreFill)
	// Equity = 25M + 75M pre-fill = 100M.
	assert.Equal(t, int64(100_000_000), resp.Equity)
}
