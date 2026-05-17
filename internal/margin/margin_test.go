package margin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
