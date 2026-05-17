package margin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAccruedAmount_DailyMatchesAnnualRate(t *testing.T) {
	// $10,000 principal (= 100_000_000 4-dec) at 8% APR for one year
	// should be ~$800 = 8_000_000.
	const principal = int64(100_000_000)
	got := AccruedAmount(principal, 800, 365*24*time.Hour)
	assert.InDelta(t, 8_000_000, got, 100, "1 year of 8% on $10k ≈ $800")
}

func TestAccruedAmount_HourlyIsAnnualOver8760(t *testing.T) {
	// Same principal at 8% APR for 1 hour: 800 / 8760 ≈ 0.0913 = 913 (4-dec).
	const principal = int64(100_000_000)
	got := AccruedAmount(principal, 800, time.Hour)
	assert.InDelta(t, 913, got, 5)
}

func TestAccruedAmount_ZeroForNegativeInputs(t *testing.T) {
	assert.Equal(t, int64(0), AccruedAmount(0, 800, time.Hour))
	assert.Equal(t, int64(0), AccruedAmount(-100, 800, time.Hour))
	assert.Equal(t, int64(0), AccruedAmount(100, 0, time.Hour))
	assert.Equal(t, int64(0), AccruedAmount(100, 800, 0))
	assert.Equal(t, int64(0), AccruedAmount(100, 800, -time.Hour))
}

func TestAccruedAmount_HandlesLargePrincipals(t *testing.T) {
	// $1B principal — would overflow naive int64 multiplication.
	// Just check it doesn't panic and returns something sensible.
	const principal = int64(10_000_000_000_000) // $1B in 4-dec
	got := AccruedAmount(principal, 800, time.Hour)
	// 1B * 8% / 8760 ≈ $9132
	assert.InDelta(t, 91_324_200, got, 1_000_000)
}

func TestQtyToCureBreach_NoBreach(t *testing.T) {
	// Breach <= 0 means no liquidation needed.
	assert.Equal(t, int64(0), QtyToCureBreach(0, 1_000_000, 100_000, MaintenanceMarginBps))
	assert.Equal(t, int64(0), QtyToCureBreach(-500, 1_000_000, 100_000, MaintenanceMarginBps))
}

func TestQtyToCureBreach_ShortCoversBreachWithBuffer(t *testing.T) {
	// Account: equity $90, maint $100 -> breach $10. Mark $10.
	// Each share covered frees 30% * $10 = $3 in maintenance.
	// Target = $10 + 10%*$100 = $20. Need ceil(20 / 3) = 7 shares.
	const dollar = int64(10_000) // 4 implied decimals
	breach := int64(10 * dollar)
	maint := int64(100 * dollar)
	mark := int64(10 * dollar)
	got := QtyToCureBreach(breach, maint, mark, MaintenanceMarginBps)
	assert.Equal(t, int64(7), got)
}

func TestQtyToCureBreach_LongSellsBreachWithBuffer(t *testing.T) {
	// Same setup but long-side rate (25%).
	// Each share frees 25% * $10 = $2.50 in maintenance.
	// Target = $20. Need ceil(20 / 2.5) = 8 shares.
	const dollar = int64(10_000)
	breach := int64(10 * dollar)
	maint := int64(100 * dollar)
	mark := int64(10 * dollar)
	got := QtyToCureBreach(breach, maint, mark, MaintenanceMarginLongBps)
	assert.Equal(t, int64(8), got)
}

func TestQtyToCureBreach_RoundsUp(t *testing.T) {
	// Anything that doesn't divide evenly rounds up so the cure is
	// guaranteed to clear the buffer.
	const dollar = int64(10_000)
	// breach $1, maint $0 -> target = $1.
	// per-share-cure at $10 mark, 30% = $3.
	// $1 / $3 = 0 truncating, but ceil = 1.
	got := QtyToCureBreach(int64(1*dollar), 0, int64(10*dollar), MaintenanceMarginBps)
	assert.Equal(t, int64(1), got)
}

func TestQtyToCureBreach_SubBpsPrice(t *testing.T) {
	// Mark too small for bps precision: per-share-cure rounds to 0.
	// Returns the "go big" sentinel — caller caps to available qty.
	const dollar = int64(10_000)
	got := QtyToCureBreach(int64(1*dollar), 0, 1, MaintenanceMarginBps)
	assert.Greater(t, got, int64(0))
}
