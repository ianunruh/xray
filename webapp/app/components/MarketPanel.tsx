import { useCallback, useEffect, useState } from "react";
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
import { orderBookClient } from "~/lib/client";
import { useSharedMarketDepth } from "../hooks/marketDepth";
import { phaseColor, phaseLabel } from "~/lib/marketPhase";
import {
  useReplayOrderBook,
  type ReplayTarget,
} from "../hooks/useReplayOrderBook";
import { useStream } from "../hooks/useStream";
import { formatPrice, formatQuantity } from "~/lib/format";
import { DepthSide } from "./MarketDepth";
import { TradeTable } from "./TradeTable";
import { CandleChart } from "./CandleChart";
import { ReplayControls } from "./ReplayControls";
import { MarketPhase, Side } from "../../src/gen/orderbook/v1/events_pb";
import type {
  GetOfficialCloseResponse,
  IndicativeAuctionState,
  PriceLevel,
  Trade,
} from "../../src/gen/orderbook/v1/service_pb";
import { useIndicativeAuctionState } from "../hooks/useIndicativeAuctionState";
import type { ReplayBounds } from "~/lib/replay";

type Mode = "live" | "replay";

export function MarketPanel({
  symbol,
  phase,
  sessionVolume,
  officialClose,
  replayBounds,
  onRefreshReplay,
}: {
  symbol: string;
  phase: MarketPhase;
  sessionVolume: bigint;
  officialClose: GetOfficialCloseResponse | null;
  replayBounds: ReplayBounds | null;
  onRefreshReplay: () => void;
}) {
  const [mode, setMode] = useState<Mode>("live");

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
            <Badge color={phaseColor(phase)} variant="filled">
              {phaseLabel(phase)}
            </Badge>
          ) : (
            <Badge color="grape" variant="filled">
              REPLAY
            </Badge>
          )}
        </Group>

        {mode === "live" ? (
          <LiveBody
            symbol={symbol}
            sessionVolume={sessionVolume}
            officialClose={officialClose}
          />
        ) : (
          <ReplayBody
            symbol={symbol}
            bounds={replayBounds}
            onRefresh={onRefreshReplay}
            officialClose={officialClose}
          />
        )}

        {officialClose && (
          <Text size="xs" c="dimmed" ta="center">
            Official close {officialClose.sessionDate}:{" "}
            <Text component="span" ff="monospace" c="bright">
              {formatPrice(officialClose.closePrice)}
            </Text>{" "}
            on {formatQuantity(officialClose.closeVolume)} shares
          </Text>
        )}
      </Stack>
    </Card>
  );
}

// MarketTicker renders the compact best-bid / best-ask / last-trade
// summary bar that sits above the chart and depth tables.
function MarketTicker({
  bid,
  ask,
  lastTrade,
  sessionVolume,
  officialClose,
}: {
  bid: PriceLevel | undefined;
  ask: PriceLevel | undefined;
  lastTrade: Trade | undefined;
  sessionVolume?: bigint;
  officialClose: GetOfficialCloseResponse | null;
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
      <div style={{ minWidth: 80 }}>
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
      <ChangeCell lastPrice={lastTrade?.price} officialClose={officialClose} />
      {sessionVolume !== undefined && (
        <div style={{ minWidth: 120 }} title="Cumulative shares traded this session; resets on close.">
          <Text size="xs" c="dimmed">
            Session Volume
          </Text>
          <Text fw={700} ff="monospace">
            {sessionVolume > 0n ? formatQuantity(sessionVolume) : "—"}
          </Text>
        </div>
      )}
    </Group>
  );
}

// ChangeCell renders signed price/percent delta of the most recent trade
// against the official close. Bigint pct math is scaled by 1e6 (delta is
// already in 1e-4 dollars; another 1e2 keeps two pct decimals).
function ChangeCell({
  lastPrice,
  officialClose,
}: {
  lastPrice: bigint | undefined;
  officialClose: GetOfficialCloseResponse | null;
}) {
  const closePrice = officialClose?.closePrice ?? 0n;
  const cellStyle = { minWidth: 170 };
  if (closePrice <= 0n) {
    return (
      <div style={cellStyle}>
        <Text size="xs" c="dimmed">
          Change vs Close
        </Text>
        <Text fw={700} c="dimmed" ff="monospace">
          no close yet
        </Text>
      </div>
    );
  }
  if (lastPrice === undefined || lastPrice <= 0n) {
    return (
      <div style={cellStyle}>
        <Text size="xs" c="dimmed">
          Change vs Close
        </Text>
        <Text fw={700} c="dimmed" ff="monospace">
          —
        </Text>
      </div>
    );
  }
  const delta = lastPrice - closePrice;
  const pctBp = (delta * 1000000n) / closePrice;
  const pct = Number(pctBp) / 10000;
  const sign = delta > 0n ? "+" : delta < 0n ? "−" : "";
  const absDelta = delta < 0n ? -delta : delta;
  const color = delta > 0n ? "teal" : delta < 0n ? "red" : "bright";
  return (
    <div
      style={cellStyle}
      title={`vs official close ${officialClose?.sessionDate}: ${formatPrice(closePrice)}`}
    >
      <Text size="xs" c="dimmed">
        Change vs Close
      </Text>
      <Text fw={700} c={color} ff="monospace">
        {sign}
        {formatPrice(absDelta)} ({sign}
        {Math.abs(pct).toFixed(2)}%)
      </Text>
    </div>
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
    <div style={{ minWidth: 170 }}>
      <Text size="xs" c="dimmed">
        {label}
      </Text>
      <Group gap={6} align="baseline" wrap="nowrap">
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

const LIVE_TRADE_HISTORY = 100;

// useLiveTrades opens a single trades stream and retains the most
// recent LIVE_TRADE_HISTORY trades. Used to drive both the ticker's
// last-trade cell and the trades table below the depth grid.
function useLiveTrades(symbol: string): Trade[] {
  const [trades, setTrades] = useState<Trade[]>([]);
  const onTrade = useCallback((t: Trade) => {
    setTrades((prev) => [t, ...prev].slice(0, LIVE_TRADE_HISTORY));
  }, []);
  useStream(
    (signal) => orderBookClient.streamTrades({ symbol }, { signal }),
    onTrade,
    [symbol],
  );
  // Reset history when symbol changes so the table doesn't briefly
  // show prior-symbol rows before the new stream catches up.
  useEffect(() => {
    setTrades([]);
  }, [symbol]);
  return trades;
}

// IndicativeAuctionBanner renders the live "what would uncross do right
// now" snapshot while the symbol is in an auction phase. Subscribed in
// LiveBody via useIndicativeAuctionState; rendered only when phase is
// AUCTION or CLOSING_AUCTION. The server pushes on every order
// arrival + a 1Hz heartbeat, so the freshness label ticks locally to
// avoid going stale-looking between server updates.
function IndicativeAuctionBanner({ state }: { state: IndicativeAuctionState }) {
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 500);
    return () => window.clearInterval(id);
  }, []);
  const computedMs = state.computedAt
    ? Number(state.computedAt.seconds) * 1000 +
      Math.floor(state.computedAt.nanos / 1_000_000)
    : nowMs;
  const ageSec = Math.max(0, Math.round((nowMs - computedMs) / 1000));
  const isClosing = state.phase === MarketPhase.CLOSING_AUCTION;
  const label = isClosing ? "Indicative closing cross" : "Indicative opening cross";
  const imbColor =
    state.imbalanceQty === 0n
      ? "dimmed"
      : state.imbalanceSide === Side.BUY
        ? "green"
        : "red";
  const imbText =
    state.imbalanceQty === 0n
      ? "balanced"
      : `${formatQuantity(state.imbalanceQty)} ${state.imbalanceSide === Side.BUY ? "buy" : "sell"}`;
  return (
    <Card withBorder bg="dark.6" p="xs">
      <Group justify="space-between" align="center" wrap="nowrap">
        <Group gap="xl" wrap="nowrap">
          <div>
            <Text size="xs" c="dimmed">
              {label}
            </Text>
            <Text fw={700} ff="monospace" size="lg">
              {state.indicativePrice > 0n ? formatPrice(state.indicativePrice) : "—"}
            </Text>
          </div>
          <div>
            <Text size="xs" c="dimmed">
              Match Qty
            </Text>
            <Text fw={600} ff="monospace">
              {state.matchedQty > 0n ? formatQuantity(state.matchedQty) : "—"}
            </Text>
          </div>
          <div>
            <Text size="xs" c="dimmed">
              Imbalance
            </Text>
            <Text fw={600} ff="monospace" c={imbColor}>
              {imbText}
            </Text>
          </div>
        </Group>
        <Text size="xs" c="dimmed">
          {ageSec}s ago
        </Text>
      </Group>
    </Card>
  );
}

function LiveBody({
  symbol,
  sessionVolume,
  officialClose,
}: {
  symbol: string;
  sessionVolume: bigint;
  officialClose: GetOfficialCloseResponse | null;
}) {
  const { bids, asks, maxQuantity } = useSharedMarketDepth();
  const trades = useLiveTrades(symbol);
  const indicative = useIndicativeAuctionState(symbol);
  const inAuction =
    indicative?.phase === MarketPhase.AUCTION ||
    indicative?.phase === MarketPhase.CLOSING_AUCTION;
  return (
    <>
      <MarketTicker
        bid={bids[0]}
        ask={asks[0]}
        lastTrade={trades[0]}
        sessionVolume={sessionVolume}
        officialClose={officialClose}
      />
      {inAuction && indicative && (
        <IndicativeAuctionBanner state={indicative} />
      )}
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
      <TradeTable trades={trades} />
    </>
  );
}

function ReplayBody({
  symbol,
  bounds,
  onRefresh,
  officialClose,
}: {
  symbol: string;
  bounds: ReplayBounds | null;
  onRefresh: () => void;
  officialClose: GetOfficialCloseResponse | null;
}) {
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
        onRefresh={onRefresh}
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
        officialClose={officialClose}
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
