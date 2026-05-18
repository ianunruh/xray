package portfolio_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/portfolio"
)

func TestPortfolio_SnapshotInterval(t *testing.T) {
	p := portfolio.NewPortfolio(portfolio.AggregateID("acct-1"))
	assert.Equal(t, 1000, p.SnapshotInterval())
}

func TestPortfolio_SnapshotRoundTrip_Empty(t *testing.T) {
	p := portfolio.NewPortfolio(portfolio.AggregateID("acct-1"))
	msg, err := p.Snapshot()
	require.NoError(t, err)

	// Wire-roundtrip through proto.Marshal/Unmarshal to confirm what
	// actually lands in the store rehydrates cleanly.
	data, err := proto.Marshal(msg)
	require.NoError(t, err)
	var decoded portfoliov1.PortfolioSnapshot
	require.NoError(t, proto.Unmarshal(data, &decoded))

	restored := portfolio.NewPortfolio(portfolio.AggregateID("acct-1"))
	require.NoError(t, restored.RestoreSnapshot(&decoded))
	assert.Equal(t, p.CashBalance, restored.CashBalance)
	assert.Empty(t, restored.Holdings)
	assert.Empty(t, restored.SettledTrades)
}

func TestPortfolio_SnapshotRoundTrip_Populated(t *testing.T) {
	// Build a representative portfolio by hand and assert all fields
	// survive the wire round-trip. Touches every field the snapshot
	// covers so future drift fails the test loudly.
	now := time.Now().UTC().Truncate(time.Microsecond)
	p := portfolio.NewPortfolio(portfolio.AggregateID("acct-7"))
	p.AccountID = "acct-7"
	p.CashBalance = 12_345_678
	p.CashHeld = 1_000_000
	p.ProceedsPool = 500_000
	p.CollateralPool = 250_000
	p.LastAccruedAt = now

	p.Holdings["AAPL"] = &portfolio.Holding{Quantity: 100, TotalCost: 15_000_000}
	p.Holdings["GOOG"] = &portfolio.Holding{Quantity: 5, TotalCost: 8_000_000}
	p.HoldsBySaga["saga-1"] = 200_000
	p.HoldsBySaga["saga-2"] = 800_000
	p.SharesHeld["AAPL"] = 25
	p.ShareHoldsBySaga["saga-3"] = &portfolio.ShareHold{Symbol: "AAPL", Quantity: 25}
	p.SettledTrades["saga-1"] = map[string]struct{}{"trade-1": {}, "trade-2": {}}
	p.SettledTrades["saga-2"] = map[string]struct{}{"trade-3": {}}

	p.ShortPositions["TSLA"] = &portfolio.ShortPosition{
		Quantity:       50,
		ProceedsHeld:   500_000,
		CollateralHeld: 250_000,
		AvgOpenPrice:   2_000_000,
	}
	p.CollateralHeldBySaga["saga-4"] = &portfolio.CollateralHold{
		Symbol:   "TSLA",
		Quantity: 50,
		Amount:   250_000,
	}
	p.ShortCoversHeld["TSLA"] = 10
	p.ShortCoverHoldsBySaga["saga-5"] = &portfolio.ShareHold{Symbol: "TSLA", Quantity: 10}

	p.ActiveMarginCall = &portfolio.MarginCall{
		CallID:             "call-1",
		TriggerTradeID:     "trade-99",
		TriggerSymbol:      "TSLA",
		MarkPrice:          1_900_000,
		EquityAtIssue:      3_000_000,
		RequirementAtIssue: 4_500_000,
		IssuedAt:           now,
		GraceExpiresAt:     now.Add(30 * time.Second),
	}

	msg, err := p.Snapshot()
	require.NoError(t, err)
	data, err := proto.Marshal(msg)
	require.NoError(t, err)
	var decoded portfoliov1.PortfolioSnapshot
	require.NoError(t, proto.Unmarshal(data, &decoded))

	restored := portfolio.NewPortfolio(portfolio.AggregateID("acct-7"))
	require.NoError(t, restored.RestoreSnapshot(&decoded))

	assert.Equal(t, p.AccountID, restored.AccountID)
	assert.Equal(t, p.CashBalance, restored.CashBalance)
	assert.Equal(t, p.CashHeld, restored.CashHeld)
	assert.Equal(t, p.ProceedsPool, restored.ProceedsPool)
	assert.Equal(t, p.CollateralPool, restored.CollateralPool)
	assert.True(t, p.LastAccruedAt.Equal(restored.LastAccruedAt))

	assert.Equal(t, p.Holdings, restored.Holdings)
	assert.Equal(t, p.HoldsBySaga, restored.HoldsBySaga)
	assert.Equal(t, p.SharesHeld, restored.SharesHeld)
	assert.Equal(t, p.ShareHoldsBySaga, restored.ShareHoldsBySaga)
	assert.Equal(t, p.SettledTrades, restored.SettledTrades)
	assert.Equal(t, p.ShortPositions, restored.ShortPositions)
	assert.Equal(t, p.CollateralHeldBySaga, restored.CollateralHeldBySaga)
	assert.Equal(t, p.ShortCoversHeld, restored.ShortCoversHeld)
	assert.Equal(t, p.ShortCoverHoldsBySaga, restored.ShortCoverHoldsBySaga)

	require.NotNil(t, restored.ActiveMarginCall)
	assert.Equal(t, p.ActiveMarginCall.CallID, restored.ActiveMarginCall.CallID)
	assert.Equal(t, p.ActiveMarginCall.TriggerTradeID, restored.ActiveMarginCall.TriggerTradeID)
	assert.Equal(t, p.ActiveMarginCall.TriggerSymbol, restored.ActiveMarginCall.TriggerSymbol)
	assert.Equal(t, p.ActiveMarginCall.MarkPrice, restored.ActiveMarginCall.MarkPrice)
	assert.Equal(t, p.ActiveMarginCall.EquityAtIssue, restored.ActiveMarginCall.EquityAtIssue)
	assert.Equal(t, p.ActiveMarginCall.RequirementAtIssue, restored.ActiveMarginCall.RequirementAtIssue)
	assert.True(t, p.ActiveMarginCall.IssuedAt.Equal(restored.ActiveMarginCall.IssuedAt))
	assert.True(t, p.ActiveMarginCall.GraceExpiresAt.Equal(restored.ActiveMarginCall.GraceExpiresAt))
}
