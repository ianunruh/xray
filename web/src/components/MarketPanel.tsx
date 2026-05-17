import { useState } from "react";
import {
  Badge,
  Card,
  Grid,
  Group,
  SegmentedControl,
  Stack,
  Text,
  Title,
} from "@mantine/core";
import { useMarketDepth } from "../hooks/useMarketDepth";
import {
  phaseColor,
  phaseLabel,
  useOrderBookPhase,
} from "../hooks/useOrderBookPhase";
import { useOfficialClose } from "../hooks/useOfficialClose";
import { useReplayBounds } from "../hooks/useReplayBounds";
import {
  useReplayOrderBook,
  type ReplayTarget,
} from "../hooks/useReplayOrderBook";
import { formatPrice, formatQuantity } from "../format";
import { DepthSide } from "./MarketDepth";
import { TradeList } from "./TradeList";
import { TradeTable } from "./TradeTable";
import { CandleChart } from "./CandleChart";
import { ReplayControls } from "./ReplayControls";

type Mode = "live" | "replay";

export function MarketPanel({ symbol }: { symbol: string }) {
  const [mode, setMode] = useState<Mode>("live");
  const close = useOfficialClose(symbol);

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Group justify="space-between" align="center">
          <Group gap="sm" align="center">
            <Title order={5}>Market: {symbol}</Title>
            <SegmentedControl
              size="xs"
              value={mode}
              onChange={(v) => setMode(v as Mode)}
              data={[
                { label: "Live", value: "live" },
                { label: "Replay", value: "replay" },
              ]}
            />
          </Group>
          {mode === "live" ? (
            <LivePhaseBadge symbol={symbol} />
          ) : (
            <Badge color="grape" variant="filled">
              REPLAY
            </Badge>
          )}
        </Group>

        {mode === "live" ? (
          <LiveBody symbol={symbol} />
        ) : (
          <ReplayBody symbol={symbol} />
        )}

        {close && (
          <Text size="xs" c="dimmed" ta="center">
            Official close {close.sessionDate}:{" "}
            <Text component="span" ff="monospace" c="bright">
              {formatPrice(close.closePrice)}
            </Text>{" "}
            on {formatQuantity(close.closeVolume)} shares
          </Text>
        )}
      </Stack>
    </Card>
  );
}

function LivePhaseBadge({ symbol }: { symbol: string }) {
  const phase = useOrderBookPhase(symbol);
  return (
    <Badge color={phaseColor(phase)} variant="filled">
      {phaseLabel(phase)}
    </Badge>
  );
}

function LiveBody({ symbol }: { symbol: string }) {
  const { bids, asks, maxQuantity } = useMarketDepth(symbol);
  return (
    <>
      <CandleChart symbol={symbol} />
      <Grid>
        <Grid.Col span={4}>
          <DepthSide
            title="Bids"
            levels={bids}
            side="bid"
            maxQuantity={maxQuantity}
          />
        </Grid.Col>
        <Grid.Col span={4}>
          <DepthSide
            title="Asks"
            levels={asks}
            side="ask"
            maxQuantity={maxQuantity}
          />
        </Grid.Col>
        <Grid.Col span={4}>
          <TradeList symbol={symbol} />
        </Grid.Col>
      </Grid>
    </>
  );
}

function ReplayBody({ symbol }: { symbol: string }) {
  const { bounds, refresh } = useReplayBounds(symbol);
  const [target, setTarget] = useState<ReplayTarget | null>(null);
  const effectiveTarget: ReplayTarget | null =
    target ?? (bounds ? { kind: "version", version: bounds.lastVersion } : null);
  const { snapshot, loading } = useReplayOrderBook(symbol, effectiveTarget);

  if (!bounds) {
    return (
      <Text size="sm" c="dimmed">
        No events for this symbol yet — nothing to replay.
      </Text>
    );
  }

  const sliderDate =
    target?.kind === "date" ? target.date : snapshot?.atDate ?? bounds.lastDate;

  const allLevels = [...(snapshot?.bids ?? []), ...(snapshot?.asks ?? [])];
  const maxQuantity =
    allLevels.length > 0
      ? allLevels.reduce(
          (max, l) => (l.quantity > max ? l.quantity : max),
          0n,
        )
      : 1n;

  // recent_trades comes oldest-first from the server; TradeTable expects
  // newest-first.
  const tradesNewestFirst = [...(snapshot?.recentTrades ?? [])].reverse();

  return (
    <>
      <ReplayControls
        bounds={bounds}
        atDate={sliderDate}
        onScrub={(d) => setTarget({ kind: "date", date: d })}
        onJumpStart={() =>
          setTarget({ kind: "version", version: bounds.firstVersion })
        }
        onJumpEnd={() =>
          setTarget({ kind: "version", version: bounds.lastVersion })
        }
        onRefresh={refresh}
      />
      <Group gap="xs">
        <Badge
          color={phaseColor(snapshot?.phase ?? bounds.currentPhase)}
          variant="light"
        >
          phase: {phaseLabel(snapshot?.phase ?? bounds.currentPhase)}
        </Badge>
        {snapshot && (
          <Text size="xs" c="dimmed">
            version {snapshot.atVersion} of {bounds.lastVersion}
            {loading ? " · loading…" : ""}
          </Text>
        )}
      </Group>
      <Grid>
        <Grid.Col span={4}>
          <DepthSide
            title="Bids"
            levels={snapshot?.bids ?? []}
            side="bid"
            maxQuantity={maxQuantity}
          />
        </Grid.Col>
        <Grid.Col span={4}>
          <DepthSide
            title="Asks"
            levels={snapshot?.asks ?? []}
            side="ask"
            maxQuantity={maxQuantity}
          />
        </Grid.Col>
        <Grid.Col span={4}>
          <TradeTable trades={tradesNewestFirst} />
        </Grid.Col>
      </Grid>
    </>
  );
}
