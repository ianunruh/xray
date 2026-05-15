import { useEffect, useRef } from "react";
import {
  createChart,
  type CandlestickData,
  type IChartApi,
  type ISeriesApi,
  ColorType,
  type UTCTimestamp,
} from "lightweight-charts";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { Box, Text } from "@mantine/core";
import { orderBookClient } from "../client";
import { priceToNumber } from "../format";
import { CandleInterval, type Candle } from "../gen/orderbook/v1/service_pb";

function candleToBar(c: Candle): CandlestickData<UTCTimestamp> {
  return {
    time: (c.openTime
      ? Math.floor(timestampDate(c.openTime).getTime() / 1000)
      : 0) as UTCTimestamp,
    open: priceToNumber(c.open),
    high: priceToNumber(c.high),
    low: priceToNumber(c.low),
    close: priceToNumber(c.close),
  };
}

export function CandleChart({ symbol }: { symbol: string }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const errorRef = useRef<string | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const chart = createChart(container, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#c9d1d9",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.05)" },
        horzLines: { color: "rgba(255,255,255,0.05)" },
      },
      width: container.clientWidth,
      height: 300,
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
      },
    });

    const series = chart.addCandlestickSeries({
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
    });

    chartRef.current = chart;
    seriesRef.current = series;

    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        chart.applyOptions({ width: entry.contentRect.width });
      }
    });
    resizeObserver.observe(container);

    const abort = new AbortController();

    (async () => {
      try {
        const resp = await orderBookClient.getCandles({
          symbol,
          interval: CandleInterval.CANDLE_INTERVAL_1M,
        });
        const bars = resp.candles.map(candleToBar);
        series.setData(bars);
        chart.timeScale().fitContent();
      } catch (err) {
        errorRef.current = (err as Error).message;
      }

      try {
        for await (const candle of orderBookClient.streamCandles(
          { symbol, interval: CandleInterval.CANDLE_INTERVAL_1M },
          { signal: abort.signal },
        )) {
          series.update(candleToBar(candle));
        }
      } catch (err) {
        if (!abort.signal.aborted) {
          console.error("Candle stream error:", err);
        }
      }
    })();

    return () => {
      abort.abort();
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, [symbol]);

  return (
    <Box>
      {errorRef.current && (
        <Text c="red" size="xs">
          {errorRef.current}
        </Text>
      )}
      <div ref={containerRef} />
    </Box>
  );
}
