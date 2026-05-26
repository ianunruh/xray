import { useEffect, useState } from "react";
import { Alert, Group, Text } from "@mantine/core";
import { MarketPhase } from "../../src/gen/orderbook/v1/events_pb";
import type { LULDState } from "~/lib/luld";
import { formatPrice } from "~/lib/format";

// HaltBanner surfaces LULD limit-state and trading-halt phases at the
// top of the trading view. Renders nothing when the symbol is in
// CONTINUOUS or any other phase.
export function HaltBanner({
  symbol,
  phase,
  luld,
}: {
  symbol: string;
  phase: MarketPhase;
  luld: LULDState;
}) {
  const target =
    phase === MarketPhase.LIMIT_STATE
      ? luld.haltDeadline
      : phase === MarketPhase.HALTED
        ? luld.reopenAt
        : null;
  const countdown = useCountdownString(target);

  if (phase === MarketPhase.LIMIT_STATE) {
    return (
      <Alert color="yellow" variant="filled" radius="md" withCloseButton={false}>
        <Group justify="space-between" wrap="nowrap">
          <Text fw={700}>
            {symbol} — LULD limit state.{" "}
            {luld.upperBand > 0n && (
              <>
                Bands {formatPrice(luld.lowerBand)} – {formatPrice(luld.upperBand)}.
              </>
            )}{" "}
            Through-band orders are rejected.
          </Text>
          <Text ff="monospace">
            {countdown ? `halts in ${countdown}` : "resolving…"}
          </Text>
        </Group>
      </Alert>
    );
  }
  if (phase === MarketPhase.HALTED) {
    return (
      <Alert color="red" variant="filled" radius="md" withCloseButton={false}>
        <Group justify="space-between" wrap="nowrap">
          <Text fw={700}>
            {symbol} — TRADING HALTED. New orders are rejected until the
            reopening auction completes.
          </Text>
          <Text ff="monospace">
            {countdown ? `reopens in ${countdown}` : "reopening…"}
          </Text>
        </Group>
      </Alert>
    );
  }
  return null;
}

// useCountdownString returns a "MM:SS" string counting down to target,
// or "now" once the deadline has passed. Re-renders once per second
// while target is non-null.
function useCountdownString(target: Date | null): string {
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    if (!target) return;
    const id = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [target]);
  if (!target) return "";
  const ms = target.getTime() - nowMs;
  if (ms <= 0) return "now";
  const totalSec = Math.ceil(ms / 1000);
  const mins = Math.floor(totalSec / 60);
  const secs = totalSec % 60;
  if (mins === 0) return `${secs}s`;
  return `${mins}:${secs.toString().padStart(2, "0")}`;
}
