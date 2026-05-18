import type { MarketPhase } from "../../src/gen/orderbook/v1/events_pb";

// ReplayBounds is the version/timestamp envelope of an aggregate's
// event stream — what the replay scrubber slides over. Returned by the
// trading route loader when ?symbol is set; null when the symbol has
// no events yet.
export type ReplayBounds = {
  firstVersion: number;
  lastVersion: number;
  firstDate: Date;
  lastDate: Date;
  currentPhase: MarketPhase;
};
