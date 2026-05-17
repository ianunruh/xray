// Package margin holds policy parameters for short selling. The
// portfolio aggregate itself is policy-agnostic — callers compute hold
// amounts here and pass them to ExecuteHoldCollateral. The margin-call
// reactor uses the same constants to evaluate maintenance requirements.
package margin

import "time"

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

// Annualized financing rates charged by the broker on borrowed
// resources. Daily/hourly accruals are time-prorated from these.
// MarginLoanRateBps applies to the cash loan principal
// (CashBalance < 0). ShortBorrowRateBps applies to the mark-value
// notional of each open short.
const (
	MarginLoanRateBps  int64 = 800 // 8% APR
	ShortBorrowRateBps int64 = 300 // 3% APR
)

// AccruedAmount returns the time-prorated charge on principal at the
// annual rate, computed as principal * (rateBps/bpsScale) * (elapsed/year).
// Year fixed at 365 * 24h. Uses float64 internally so large principals
// don't overflow int64 mid-computation; sub-cent rounding is fine for
// a simulator. Returns 0 for non-positive inputs.
func AccruedAmount(principal int64, rateBps int64, elapsed time.Duration) int64 {
	if principal <= 0 || rateBps <= 0 || elapsed <= 0 {
		return 0
	}
	const yearSecs = float64(365 * 24 * 3600)
	amount := float64(principal) * float64(rateBps) * elapsed.Seconds() /
		(float64(bpsScale) * yearSecs)
	return int64(amount)
}

// LiquidationBufferBps is the headroom above maintenance the auto-
// liquidator targets, in bps of the pre-liquidation maintenance
// requirement. 1000 = 10%. Larger than zero so a small mark wobble
// doesn't re-breach the account on the next tick and trigger another
// liquidation immediately.
const LiquidationBufferBps int64 = 1000

// QtyToCureBreach returns the minimum number of shares the auto-
// liquidator should close at markPrice so the account exits the
// breach with LiquidationBufferBps of headroom. maintRateBps is the
// position side's maintenance rate (MaintenanceMarginBps for shorts,
// MaintenanceMarginLongBps for longs). Returns 0 when there's no
// breach to cure. Callers cap the result at the available position
// size.
//
// Derivation: liquidating qty shares leaves equity unchanged in mark
// terms (cash in == market value out, ignoring slippage) but reduces
// maintenance by maintRateBps/bpsScale * markPrice * qty. Solving
// E >= (M - rate*P*qty) + buffer gives qty >= (breach + buffer) / (rate*P).
func QtyToCureBreach(breach, maint, markPrice, maintRateBps int64) int64 {
	if breach <= 0 || markPrice <= 0 || maintRateBps <= 0 {
		return 0
	}
	target := breach + maint*LiquidationBufferBps/bpsScale
	perShareCure := markPrice * maintRateBps / bpsScale
	if perShareCure <= 0 {
		// Sub-cent prices fall below bps precision; return a
		// sentinel large enough that the caller's cap-at-available
		// step picks the whole position.
		return target
	}
	return (target + perShareCure - 1) / perShareCure
}
