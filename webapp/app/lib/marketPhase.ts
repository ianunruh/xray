import { MarketPhase } from "../../src/gen/orderbook/v1/events_pb";

export function phaseLabel(phase: MarketPhase): string {
  switch (phase) {
    case MarketPhase.AUCTION:
      return "AUCTION";
    case MarketPhase.CLOSING_AUCTION:
      return "CLOSING AUCTION";
    case MarketPhase.CLOSED:
      return "CLOSED";
    case MarketPhase.CONTINUOUS:
    case MarketPhase.UNSPECIFIED:
    default:
      return "CONTINUOUS";
  }
}

export function phaseColor(phase: MarketPhase): string {
  switch (phase) {
    case MarketPhase.AUCTION:
      return "yellow";
    case MarketPhase.CLOSING_AUCTION:
      return "orange";
    case MarketPhase.CLOSED:
      return "red";
    case MarketPhase.CONTINUOUS:
    case MarketPhase.UNSPECIFIED:
    default:
      return "green";
  }
}
