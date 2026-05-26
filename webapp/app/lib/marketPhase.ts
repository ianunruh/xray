import { MarketPhase } from "../../src/gen/orderbook/v1/events_pb";

export function phaseLabel(phase: MarketPhase): string {
  switch (phase) {
    case MarketPhase.AUCTION:
      return "AUCTION";
    case MarketPhase.CLOSING_AUCTION:
      return "CLOSING AUCTION";
    case MarketPhase.CLOSED:
      return "CLOSED";
    case MarketPhase.LIMIT_STATE:
      return "LIMIT STATE";
    case MarketPhase.HALTED:
      return "HALTED";
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
    case MarketPhase.LIMIT_STATE:
      return "yellow";
    case MarketPhase.HALTED:
      return "red";
    case MarketPhase.CONTINUOUS:
    case MarketPhase.UNSPECIFIED:
    default:
      return "green";
  }
}

// phaseDescription returns a one-liner explaining the phase for tooltips
// and the halt banner. Empty for the trivial CONTINUOUS case.
export function phaseDescription(phase: MarketPhase): string {
  switch (phase) {
    case MarketPhase.AUCTION:
      return "Auction in progress — orders rest without crossing until the uncross.";
    case MarketPhase.CLOSING_AUCTION:
      return "Closing auction — only AT_CLOSE orders accepted until uncross.";
    case MarketPhase.CLOSED:
      return "Market is closed for this symbol.";
    case MarketPhase.LIMIT_STATE:
      return "LULD limit state — trades through the band are paused. The symbol will halt if not resolved soon.";
    case MarketPhase.HALTED:
      return "Trading is halted under LULD. The symbol will reopen via a single-price auction.";
    default:
      return "";
  }
}
