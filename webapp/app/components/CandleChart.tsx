import { useEffect, useRef } from "react";
import {
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  ColorType,
  type UTCTimestamp,
} from "lightweight-charts";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { Box, Text } from "@mantine/core";
import { orderBookClient } from "~/lib/client";
import { priceToNumber } from "~/lib/format";
import { CandleInterval, type Candle } from "../../src/gen/orderbook/v1/service_pb";

const VOLUME_UP_COLOR = "rgba(38, 166, 154, 0.55)";
const VOLUME_DOWN_COLOR = "rgba(239, 83, 80, 0.55)";

function candleTime(c: Candle): UTCTimestamp {
  return (c.openTime
    ? Math.floor(timestampDate(c.openTime).getTime() / 1000)
    : 0) as UTCTimestamp;
}

function candleToBar(c: Candle): CandlestickData<UTCTimestamp> {
  return {
    time: candleTime(c),
    open: priceToNumber(c.open),
    high: priceToNumber(c.high),
    low: priceToNumber(c.low),
    close: priceToNumber(c.close),
  };
}

function candleToVolume(c: Candle): HistogramData<UTCTimestamp> {
  return {
    time: candleTime(c),
    value: Number(c.volume),
    color: c.close >= c.open ? VOLUME_UP_COLOR : VOLUME_DOWN_COLOR,
  };
}

export function CandleChart({ symbol }: { symbol: string }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);
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

    // Volume on an overlay scale pinned to the bottom 25% of the pane so
    // the bars don't compete with the price grid.
    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });
    chart.priceScale("volume").applyOptions({
      scaleMargins: { top: 0.75, bottom: 0 },
    });

    chartRef.current = chart;
    seriesRef.current = series;
    volumeSeriesRef.current = volumeSeries;

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
        series.setData(resp.candles.map(candleToBar));
        volumeSeries.setData(resp.candles.map(candleToVolume));
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
          volumeSeries.update(candleToVolume(candle));
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
      volumeSeriesRef.current = null;
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
