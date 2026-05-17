import { Badge, Card, Grid, Group, Stack, Text, Title } from "@mantine/core";
import { useMarketDepth } from "../hooks/useMarketDepth";
import {
  phaseColor,
  phaseLabel,
  useOrderBookPhase,
} from "../hooks/useOrderBookPhase";
import { useOfficialClose } from "../hooks/useOfficialClose";
import { formatPrice, formatQuantity } from "../format";
import { DepthSide } from "./MarketDepth";
import { TradeList } from "./TradeList";
import { CandleChart } from "./CandleChart";

export function MarketPanel({ symbol }: { symbol: string }) {
  const { bids, asks, maxQuantity } = useMarketDepth(symbol);
  const phase = useOrderBookPhase(symbol);
  const close = useOfficialClose(symbol);

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Group justify="space-between" align="center">
          <Title order={5}>Market: {symbol}</Title>
          <Badge color={phaseColor(phase)} variant="filled">
            {phaseLabel(phase)}
          </Badge>
        </Group>
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
