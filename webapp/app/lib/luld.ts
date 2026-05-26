import type { Timestamp } from "@bufbuild/protobuf/wkt";

// LULDState is the shape the trading view and replay view pass to
// MarketPanel / HaltBanner / CandleChart. Mirrors the LULD fields on
// GetMarketStatusResponse and ReplayOrderBookResponse.
export type LULDState = {
  referencePrice: bigint;
  upperBand: bigint;
  lowerBand: bigint;
  bandBps: number;
  haltDeadline: Date | null;
  reopenAt: Date | null;
};

export function emptyLULDState(): LULDState {
  return {
    referencePrice: 0n,
    upperBand: 0n,
    lowerBand: 0n,
    bandBps: 0,
    haltDeadline: null,
    reopenAt: null,
  };
}

// luldFromProto extracts LULDState from any message that has the
// canonical luld_* field set (GetMarketStatusResponse or
// ReplayOrderBookResponse). Both share the same field names.
export function luldFromProto(msg: {
  luldReferencePrice: bigint;
  luldUpperBand: bigint;
  luldLowerBand: bigint;
  luldBandBps: number;
  luldHaltDeadline?: Timestamp;
  luldReopenAt?: Timestamp;
}): LULDState {
  return {
    referencePrice: msg.luldReferencePrice,
    upperBand: msg.luldUpperBand,
    lowerBand: msg.luldLowerBand,
    bandBps: msg.luldBandBps,
    haltDeadline: tsToDate(msg.luldHaltDeadline),
    reopenAt: tsToDate(msg.luldReopenAt),
  };
}

function tsToDate(ts: Timestamp | undefined): Date | null {
  if (!ts || (ts.seconds === 0n && ts.nanos === 0)) {
    return null;
  }
  return new Date(Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000));
}
