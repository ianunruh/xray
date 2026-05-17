package portfolio

import (
	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

// IsExposureAdding reports whether an order would grow the account's
// market exposure (and therefore its risk + margin requirement).
//
//	BUY  + LONG  -> true   (open or add to a long)
//	SELL + SHORT -> true   (open or add to a short)
//	SELL + LONG  -> false  (sells existing long, reduces exposure)
//	BUY  + SHORT -> false  (covers a short, reduces exposure)
//
// Used by the margin-call gating logic — accounts under an active
// call are blocked from adding exposure but free to reduce it.
func IsExposureAdding(side orderbookv1.Side, ps orderbookv1.PositionSide) bool {
	isShort := ps == orderbookv1.PositionSide_POSITION_SIDE_SHORT
	switch {
	case side == orderbookv1.Side_SIDE_BUY && !isShort:
		return true
	case side == orderbookv1.Side_SIDE_SELL && isShort:
		return true
	}
	return false
}
