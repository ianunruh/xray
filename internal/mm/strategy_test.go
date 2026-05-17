package mm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

func TestSpreadStrategy_BasicLevels(t *testing.T) {
	s := &SpreadStrategy{
		Spread:       10000, // $1.00
		Levels:       3,
		LevelSpacing: 10000,
		Quantity:     10,
	}

	inv := InventoryState{Position: 0, MaxPosition: 100}
	quotes := s.ComputeQuotes(1500000, inv) // ref = $150.00

	assert.Len(t, quotes, 6) // 3 bids + 3 asks

	// Bids: $149.50, $148.50, $147.50
	assert.Equal(t, orderbookv1.Side_SIDE_BUY, quotes[0].Side)
	assert.Equal(t, int64(1495000), quotes[0].Price)
	assert.Equal(t, int64(10), quotes[0].Quantity)

	assert.Equal(t, int64(1485000), quotes[1].Price)
	assert.Equal(t, int64(1475000), quotes[2].Price)

	// Asks: $150.50, $151.50, $152.50
	assert.Equal(t, orderbookv1.Side_SIDE_SELL, quotes[3].Side)
	assert.Equal(t, int64(1505000), quotes[3].Price)
	assert.Equal(t, int64(10), quotes[3].Quantity)

	assert.Equal(t, int64(1515000), quotes[4].Price)
	assert.Equal(t, int64(1525000), quotes[5].Price)
}

func TestSpreadStrategy_SingleLevel(t *testing.T) {
	s := &SpreadStrategy{
		Spread:       20000, // $2.00
		Levels:       1,
		LevelSpacing: 20000,
		Quantity:     5,
	}

	inv := InventoryState{Position: 0, MaxPosition: 50}
	quotes := s.ComputeQuotes(1000000, inv) // ref = $100.00

	assert.Len(t, quotes, 2)
	assert.Equal(t, int64(990000), quotes[0].Price)  // $99.00
	assert.Equal(t, int64(1010000), quotes[1].Price) // $101.00
}

func TestSpreadStrategy_MaxLongSuppressesBuys(t *testing.T) {
	s := &SpreadStrategy{
		Spread:       10000,
		Levels:       2,
		LevelSpacing: 10000,
		Quantity:     10,
	}

	inv := InventoryState{Position: 100, MaxPosition: 100}
	quotes := s.ComputeQuotes(1500000, inv)

	// Only asks, no bids
	assert.Len(t, quotes, 2)
	for _, q := range quotes {
		assert.Equal(t, orderbookv1.Side_SIDE_SELL, q.Side)
	}
}

func TestSpreadStrategy_MaxShortSuppressesSells(t *testing.T) {
	s := &SpreadStrategy{
		Spread:       10000,
		Levels:       2,
		LevelSpacing: 10000,
		Quantity:     10,
	}

	inv := InventoryState{Position: -100, MaxPosition: 100}
	quotes := s.ComputeQuotes(1500000, inv)

	// Only bids, no asks
	assert.Len(t, quotes, 2)
	for _, q := range quotes {
		assert.Equal(t, orderbookv1.Side_SIDE_BUY, q.Side)
	}
}

func TestSpreadStrategy_SkipsNegativePrices(t *testing.T) {
	s := &SpreadStrategy{
		Spread:       10000,
		Levels:       3,
		LevelSpacing: 10000,
		Quantity:     10,
	}

	// Very low ref price: some bid levels would go negative
	inv := InventoryState{Position: 0, MaxPosition: 100}
	quotes := s.ComputeQuotes(10000, inv) // ref = $1.00

	// halfSpread = 5000, so bid L0 = 5000 ($0.50), L1 = -5000 (skipped), L2 = -15000 (skipped)
	var bids []QuoteLevel
	for _, q := range quotes {
		if q.Side == orderbookv1.Side_SIDE_BUY {
			bids = append(bids, q)
		}
	}
	assert.Len(t, bids, 1)
	assert.Equal(t, int64(5000), bids[0].Price)
}

func TestSpreadStrategy_LongSkewsDown(t *testing.T) {
	s := &SpreadStrategy{
		Spread:       10000, // $1.00
		Levels:       1,
		LevelSpacing: 10000,
		Quantity:     10,
		MaxSkew:      10000, // $1.00 at full inventory
	}

	// Half-long: mid shifts down $0.50 → bid $149.00, ask $150.00
	inv := InventoryState{Position: 50, MaxPosition: 100}
	quotes := s.ComputeQuotes(1500000, inv)

	require.Len(t, quotes, 2)
	assert.Equal(t, orderbookv1.Side_SIDE_BUY, quotes[0].Side)
	assert.Equal(t, int64(1490000), quotes[0].Price)
	assert.Equal(t, orderbookv1.Side_SIDE_SELL, quotes[1].Side)
	assert.Equal(t, int64(1500000), quotes[1].Price)
}

func TestSpreadStrategy_ShortSkewsUp(t *testing.T) {
	s := &SpreadStrategy{
		Spread:       10000,
		Levels:       1,
		LevelSpacing: 10000,
		Quantity:     10,
		MaxSkew:      10000,
	}

	// Half-short: mid shifts up $0.50 → bid $150.00, ask $151.00
	inv := InventoryState{Position: -50, MaxPosition: 100}
	quotes := s.ComputeQuotes(1500000, inv)

	require.Len(t, quotes, 2)
	assert.Equal(t, int64(1500000), quotes[0].Price)
	assert.Equal(t, int64(1510000), quotes[1].Price)
}

func TestSpreadStrategy_ZeroSkewIgnoresPosition(t *testing.T) {
	s := &SpreadStrategy{
		Spread:       10000,
		Levels:       1,
		LevelSpacing: 10000,
		Quantity:     10,
		// MaxSkew: 0 — disabled
	}

	inv := InventoryState{Position: 50, MaxPosition: 100}
	quotes := s.ComputeQuotes(1500000, inv)

	require.Len(t, quotes, 2)
	// Unchanged from neutral position: bid $149.50, ask $150.50
	assert.Equal(t, int64(1495000), quotes[0].Price)
	assert.Equal(t, int64(1505000), quotes[1].Price)
}

func TestSpreadStrategy_EmptyWhenFullyLimited(t *testing.T) {
	s := &SpreadStrategy{
		Spread:       10000,
		Levels:       1,
		LevelSpacing: 10000,
		Quantity:     10,
	}

	// At max long AND max short simultaneously shouldn't happen,
	// but if MaxPosition is 0, both sides suppressed
	inv := InventoryState{Position: 0, MaxPosition: 0}
	quotes := s.ComputeQuotes(1500000, inv)
	assert.Empty(t, quotes)
}
