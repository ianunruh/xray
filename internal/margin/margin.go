// Package margin holds policy parameters for short selling. The
// portfolio aggregate itself is policy-agnostic — callers compute hold
// amounts here and pass them to ExecuteHoldCollateral. The margin-call
// reactor uses the same constants to evaluate maintenance requirements.
package margin

// InitialMarginBps is the additional cash collateral required on top of
// sale proceeds when opening a short, expressed in basis points of
// notional. 5000 = 50% (Reg-T-style).
const InitialMarginBps int64 = 5000

// MaintenanceMarginBps is the equity floor (as bps of short notional at
// mark) below which a margin call fires. Held here for the future
// margin-call reactor; not used by the ordersaga reactor.
const MaintenanceMarginBps int64 = 3000

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
