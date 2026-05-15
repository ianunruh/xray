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
	default:
		return orderbookv1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED
	}
}
