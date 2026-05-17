import { useCallback, useState } from "react";
import {
  Badge,
  Card,
  Divider,
  Grid,
  Group,
  SegmentedControl,
  Stack,
  Text,
  Title,
} from "@mantine/core";
import { orderBookClient } from "../client";
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
import { useStream } from "../hooks/useStream";
import { formatPrice, formatQuantity } from "../format";
import { DepthSide } from "./MarketDepth";
import { TradeTable } from "./TradeTable";
import { CandleChart } from "./CandleChart";
import { ReplayControls } from "./ReplayControls";
import type { PriceLevel, Trade } from "../gen/orderbook/v1/service_pb";

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

// MarketTicker renders the compact best-bid / best-ask / last-trade
// summary bar that sits above the chart and depth tables.
function MarketTicker({
  bid,
  ask,
  lastTrade,
}: {
  bid: PriceLevel | undefined;
  ask: PriceLevel | undefined;
  lastTrade: Trade | undefined;
}) {
  const spread =
    bid && ask && ask.price > bid.price ? ask.price - bid.price : null;
  return (
    <Group gap="xl" wrap="wrap">
      <TickerCell
        label="Best Bid"
        color="green"
        price={bid?.price}
        quantity={bid?.quantity}
      />
      <TickerCell
        label="Best Ask"
        color="red"
        price={ask?.price}
        quantity={ask?.quantity}
      />
      <div>
        <Text size="xs" c="dimmed">
          Spread
        </Text>
        <Text fw={700} ff="monospace">
          {spread === null ? "—" : formatPrice(spread)}
        </Text>
      </div>
      <Divider orientation="vertical" />
      <TickerCell
        label="Last Trade"
        color="bright"
        price={lastTrade?.price}
        quantity={lastTrade?.quantity}
      />
    </Group>
  );
}

function TickerCell({
  label,
  color,
  price,
  quantity,
}: {
  label: string;
  color: string;
  price: bigint | undefined;
  quantity: bigint | undefined;
}) {
  return (
    <div>
      <Text size="xs" c="dimmed">
        {label}
      </Text>
      <Group gap={6} align="baseline">
        <Text fw={700} c={color} ff="monospace">
          {price === undefined ? "—" : formatPrice(price)}
        </Text>
        <Text size="xs" c="dimmed" ff="monospace">
          {quantity === undefined ? "" : `× ${formatQuantity(quantity)}`}
        </Text>
      </Group>
    </div>
  );
}

function useLastTrade(symbol: string): Trade | undefined {
  const [trade, setTrade] = useState<Trade | undefined>(undefined);
  const onTrade = useCallback((t: Trade) => setTrade(t), []);
  useStream(
    (signal) => orderBookClient.streamTrades({ symbol }, { signal }),
    onTrade,
    [symbol],
  );
  return trade;
}

function LiveBody({ symbol }: { symbol: string }) {
  const { bids, asks, maxQuantity } = useMarketDepth(symbol);
  const lastTrade = useLastTrade(symbol);
  return (
    <>
      <MarketTicker bid={bids[0]} ask={asks[0]} lastTrade={lastTrade} />
      <CandleChart symbol={symbol} />
      <Grid>
        <Grid.Col span={6}>
          <DepthSide
            title="Bids"
            levels={bids}
            side="bid"
            maxQuantity={maxQuantity}
          />
        </Grid.Col>
        <Grid.Col span={6}>
          <DepthSide
            title="Asks"
            levels={asks}
            side="ask"
            maxQuantity={maxQuantity}
          />
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
  const lastTrade = tradesNewestFirst[0];

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
      <MarketTicker
        bid={snapshot?.bids?.[0]}
        ask={snapshot?.asks?.[0]}
        lastTrade={lastTrade}
      />
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
