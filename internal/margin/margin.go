// Package margin holds policy parameters for short selling. The
// portfolio aggregate itself is policy-agnostic — callers compute hold
// amounts here and pass them to ExecuteHoldCollateral. The margin-call
// reactor uses the same constants to evaluate maintenance requirements.
package margin

// Short-side policy.
//
// InitialMarginBps is the additional cash collateral required on top of
// sale proceeds when opening a short. 5000 = 50% (Reg-T-style).
// MaintenanceMarginBps is the equity floor (as bps of short notional at
// mark) below which a margin call fires for shorts.
const (
	InitialMarginBps     int64 = 5000
	MaintenanceMarginBps int64 = 3000
)

// Long-side policy. Buying on margin means putting up part of the
// cash and borrowing the rest from the broker.
//
// InitialMarginLongBps is the user's equity contribution at order
// time, in bps of notional. 5000 = 50% (you put up half, broker
// lends half — i.e. 2x leverage).
// MaintenanceMarginLongBps is the equity floor on long market value
// below which a margin call fires. 2500 = 25%.
const (
	InitialMarginLongBps     int64 = 5000
	MaintenanceMarginLongBps int64 = 2500
)

// LeverageBps is the multiplier on excess equity that yields total
// buying power. 20000 = 2x.
const LeverageBps int64 = 20000

const bpsScale int64 = 10000

// CollateralForShortOpen returns the additional cash collateral required
// to open a short of qty shares at price. proceeds (price * qty) enter
// the portfolio's ProceedsPool automatically on settlement; this is the
// extra cushion the account must post from CashBalance.
func CollateralForShortOpen(price, qty int64) int64 {
	notional := price * qty
	return notional * InitialMarginBps / bpsScale
}

// MaintenanceRequirement returns the equity floor for an open short of
// quantity shares marked at markPrice.
func MaintenanceRequirement(markPrice, qty int64) int64 {
	notional := markPrice * qty
	return notional * MaintenanceMarginBps / bpsScale
}

// MaintenanceForLong returns the equity floor for an open long
// position of quantity shares marked at markPrice.
func MaintenanceForLong(markPrice, qty int64) int64 {
	notional := markPrice * qty
	return notional * MaintenanceMarginLongBps / bpsScale
}

// BuyingPower computes total buying power given equity and total
// maintenance requirement: leverage * (equity - maintenance). Floored
// at zero — a breached account has no headroom to leverage.
func BuyingPower(equity, maintenance int64) int64 {
	excess := equity - maintenance
	if excess <= 0 {
		return 0
	}
	return excess * LeverageBps / bpsScale
}
