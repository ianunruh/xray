import { Card, Grid, Stack, Title } from "@mantine/core";
import { useMarketDepth } from "../hooks/useMarketDepth";
import { DepthSide } from "./MarketDepth";
import { TradeList } from "./TradeList";
import { CandleChart } from "./CandleChart";

export function MarketPanel({ symbol }: { symbol: string }) {
  const { bids, asks, maxQuantity } = useMarketDepth(symbol);

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Title order={5}>Market: {symbol}</Title>
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
      </Stack>
    </Card>
  );
}
