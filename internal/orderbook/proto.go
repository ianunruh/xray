package orderbook

import orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"

func SideFromProto(s orderbookv1.Side) Side {
	switch s {
	case orderbookv1.Side_SIDE_BUY:
		return Buy
	case orderbookv1.Side_SIDE_SELL:
		return Sell
	default:
		return 0
	}
}

func SideToProto(s Side) orderbookv1.Side {
	switch s {
	case Buy:
		return orderbookv1.Side_SIDE_BUY
	case Sell:
		return orderbookv1.Side_SIDE_SELL
	default:
		return orderbookv1.Side_SIDE_UNSPECIFIED
	}
}

func OrderTypeFromProto(ot orderbookv1.OrderType) OrderType {
	switch ot {
	case orderbookv1.OrderType_ORDER_TYPE_MARKET:
		return Market
	case orderbookv1.OrderType_ORDER_TYPE_STOP_MARKET:
		return StopMarket
	case orderbookv1.OrderType_ORDER_TYPE_STOP_LIMIT:
		return StopLimit
	default:
		return Limit
	}
}

func OrderTypeToProto(ot OrderType) orderbookv1.OrderType {
	switch ot {
	case Market:
		return orderbookv1.OrderType_ORDER_TYPE_MARKET
	case StopMarket:
		return orderbookv1.OrderType_ORDER_TYPE_STOP_MARKET
	case StopLimit:
		return orderbookv1.OrderType_ORDER_TYPE_STOP_LIMIT
	case Limit:
		return orderbookv1.OrderType_ORDER_TYPE_LIMIT
	default:
		return orderbookv1.OrderType_ORDER_TYPE_UNSPECIFIED
	}
}

func TimeInForceFromProto(tif orderbookv1.TimeInForce) TimeInForce {
	switch tif {
	case orderbookv1.TimeInForce_TIME_IN_FORCE_IOC:
		return IOC
	case orderbookv1.TimeInForce_TIME_IN_FORCE_FOK:
		return FOK
	case orderbookv1.TimeInForce_TIME_IN_FORCE_DAY:
		return Day
	default:
		return GTC
	}
}

func TimeInForceToProto(tif TimeInForce) orderbookv1.TimeInForce {
	switch tif {
	case IOC:
		return orderbookv1.TimeInForce_TIME_IN_FORCE_IOC
	case FOK:
		return orderbookv1.TimeInForce_TIME_IN_FORCE_FOK
	case GTC:
		return orderbookv1.TimeInForce_TIME_IN_FORCE_GTC
	case Day:
		return orderbookv1.TimeInForce_TIME_IN_FORCE_DAY
	default:
		return orderbookv1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED
	}
}

// MarketPhaseFromProto maps the proto enum to internal phase. UNSPECIFIED
// (the zero value seen on snapshots written before phase tracking
// existed) maps to PhaseContinuous so historical data restores cleanly.
func MarketPhaseFromProto(p orderbookv1.MarketPhase) MarketPhase {
	switch p {
	case orderbookv1.MarketPhase_MARKET_PHASE_AUCTION:
		return PhaseAuction
	case orderbookv1.MarketPhase_MARKET_PHASE_CLOSING_AUCTION:
		return PhaseClosingAuction
	case orderbookv1.MarketPhase_MARKET_PHASE_CLOSED:
		return PhaseClosed
	default:
		return PhaseContinuous
	}
}

func MarketPhaseToProto(p MarketPhase) orderbookv1.MarketPhase {
	switch p {
	case PhaseAuction:
		return orderbookv1.MarketPhase_MARKET_PHASE_AUCTION
	case PhaseClosingAuction:
		return orderbookv1.MarketPhase_MARKET_PHASE_CLOSING_AUCTION
	case PhaseClosed:
		return orderbookv1.MarketPhase_MARKET_PHASE_CLOSED
	default:
		return orderbookv1.MarketPhase_MARKET_PHASE_CONTINUOUS
	}
}

func CrossTypeFromProto(c orderbookv1.CrossType) CrossType {
	switch c {
	case orderbookv1.CrossType_CROSS_TYPE_OPENING:
		return CrossOpening
	case orderbookv1.CrossType_CROSS_TYPE_CLOSING:
		return CrossClosing
	case orderbookv1.CrossType_CROSS_TYPE_HALT_REOPEN:
		return CrossHaltReopen
	default:
		return CrossNone
	}
}

func CrossTypeToProto(c CrossType) orderbookv1.CrossType {
	switch c {
	case CrossOpening:
		return orderbookv1.CrossType_CROSS_TYPE_OPENING
	case CrossClosing:
		return orderbookv1.CrossType_CROSS_TYPE_CLOSING
	case CrossHaltReopen:
		return orderbookv1.CrossType_CROSS_TYPE_HALT_REOPEN
	default:
		return orderbookv1.CrossType_CROSS_TYPE_NONE
	}
}
