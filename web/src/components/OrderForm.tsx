import { useState } from "react";
import {
  Button,
  Card,
  Group,
  NumberInput,
  SegmentedControl,
  Select,
  Stack,
  Text,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Side, OrderType, PositionSide, TimeInForce } from "../gen/orderbook/v1/events_pb";
import { sagaClient } from "../client";
import { formatMoney, moneyToPrice } from "../format";
import { useMarginSnapshot } from "../hooks/useMarginSnapshot";
import { usePreviewOrderImpact } from "../hooks/usePreviewOrderImpact";

const ORDER_TYPES: Record<string, OrderType> = {
  LIMIT: OrderType.LIMIT,
  MARKET: OrderType.MARKET,
};

const TIME_IN_FORCE: Record<string, TimeInForce> = {
  GTC: TimeInForce.GTC,
  IOC: TimeInForce.IOC,
  FOK: TimeInForce.FOK,
  DAY: TimeInForce.DAY,
  AT_OPEN: TimeInForce.AT_OPEN,
  AT_CLOSE: TimeInForce.AT_CLOSE,
};

const LIMIT_TIF_OPTIONS = [
  { label: "GTC", value: "GTC" },
  { label: "IOC", value: "IOC" },
  { label: "FOK", value: "FOK" },
  { label: "DAY", value: "DAY" },
  { label: "LOO (At Open)", value: "AT_OPEN" },
  { label: "LOC (At Close)", value: "AT_CLOSE" },
];

const MARKET_TIF_OPTIONS = [
  { label: "IOC", value: "IOC" },
  { label: "FOK", value: "FOK" },
  { label: "MOO (At Open)", value: "AT_OPEN" },
  { label: "MOC (At Close)", value: "AT_CLOSE" },
];

type Mode = "SINGLE" | "BRACKET" | "OCO";

export function OrderForm({
  accountId,
  symbol,
}: {
  accountId: string;
  symbol: string;
}) {
  const [mode, setMode] = useState<Mode>("SINGLE");
  const [side, setSide] = useState<string>("BUY");
  const [positionSide, setPositionSide] = useState<string>("LONG");
  const [orderType, setOrderType] = useState<string>("MARKET");
  const [price, setPrice] = useState<number | string>("");
  const [quantity, setQuantity] = useState<number | string>(100);
  const [tif, setTif] = useState<string>("IOC");
  const [takeProfit, setTakeProfit] = useState<number | string>("");
  const [stopLoss, setStopLoss] = useState<number | string>("");
  const [loading, setLoading] = useState(false);

  const margin = useMarginSnapshot(accountId);

  const isSingle = mode === "SINGLE";
  const isBracket = mode === "BRACKET";
  const isOCO = mode === "OCO";
  const isMarket = orderType === "MARKET";
  const ps = positionSide === "SHORT"
    ? PositionSide.SHORT
    : PositionSide.LONG;

  // Server-side preview of the order's hold + margin impact. OCO
  // mode doesn't request a preview — exits don't take cash, and the
  // preview's side/position_side don't map cleanly onto an OCO plan.
  const qtyNum = Number(quantity);
  const prcNum = Number(price);
  const previewParams =
    !isOCO && qtyNum > 0 && (isMarket || prcNum > 0)
      ? {
          accountId,
          symbol,
          side: side === "BUY" ? Side.BUY : Side.SELL,
          positionSide: ps,
          orderType: ORDER_TYPES[orderType] ?? OrderType.MARKET,
          price: isMarket ? 0n : moneyToPrice(prcNum),
          quantity: BigInt(qtyNum),
        }
      : null;
  const preview = usePreviewOrderImpact(previewParams);
  const insufficient = preview ? !preview.sufficientBuyingPower : false;
  const wouldCauseCall = preview ? preview.projectedInCall : false;

  function handleOrderTypeChange(v: string | null) {
    const next = v ?? "MARKET";
    setOrderType(next);
    if (next === "MARKET" && (tif === "GTC" || tif === "DAY")) {
      setTif("IOC");
    }
    if (next === "LIMIT" && tif === "IOC") {
      setTif("DAY");
    }
  }

  async function handleSubmit() {
    const qty = typeof quantity === "number" ? quantity : parseInt(quantity, 10);
    if (!qty || qty <= 0) {
      notifications.show({ message: "Enter a valid quantity", color: "red" });
      return;
    }

    if (isSingle) {
      const prc = typeof price === "number" ? price : parseFloat(price);
      if (!isMarket && (!prc || prc <= 0)) {
        notifications.show({ message: "Enter a valid price", color: "red" });
        return;
      }
      setLoading(true);
      try {
        await sagaClient.place({
          accountId,
          plan: {
            case: "singleOrder",
            value: {
              symbol,
              side: side === "BUY" ? Side.BUY : Side.SELL,
              orderType: ORDER_TYPES[orderType] ?? OrderType.MARKET,
              price: isMarket ? 0n : moneyToPrice(prc),
              quantity: BigInt(qty),
              timeInForce: TIME_IN_FORCE[tif] ?? TimeInForce.IOC,
              positionSide: ps,
            },
          },
        });
        notifications.show({
          title: "Order placed",
          message: `${side} ${qty} ${symbol}`,
          color: "green",
        });
      } catch (e: unknown) {
        notifications.show({
          title: "Order failed",
          message: e instanceof Error ? e.message : String(e),
          color: "red",
        });
      } finally {
        setLoading(false);
      }
      return;
    }

    const tp = typeof takeProfit === "number" ? takeProfit : parseFloat(takeProfit);
    const sl = typeof stopLoss === "number" ? stopLoss : parseFloat(stopLoss);
    if (!tp || tp <= 0) {
      notifications.show({ message: "Enter a valid take-profit price", color: "red" });
      return;
    }
    if (!sl || sl <= 0) {
      notifications.show({ message: "Enter a valid stop-loss price", color: "red" });
      return;
    }

    if (isOCO) {
      // OCO has no entry — TP and SL just need to bracket the current position
      // on opposite sides. SELL exit: TP > SL (profit above, stop below).
      // BUY exit (covering a short): TP < SL.
      if (side === "SELL" && tp <= sl) {
        notifications.show({
          message: "For a SELL OCO, TP must be above SL",
          color: "red",
        });
        return;
      }
      if (side === "BUY" && tp >= sl) {
        notifications.show({
          message: "For a BUY OCO, TP must be below SL",
          color: "red",
        });
        return;
      }
      setLoading(true);
      try {
        await sagaClient.place({
          accountId,
          plan: {
            case: "oco",
            value: {
              symbol,
              exitSide: side === "BUY" ? Side.BUY : Side.SELL,
              quantity: BigInt(qty),
              takeProfitPrice: moneyToPrice(tp),
              stopLossPrice: moneyToPrice(sl),
              positionSide: ps,
            },
          },
        });
        notifications.show({
          title: "OCO placed",
          message: `${side} ${qty} ${symbol} (TP ${tp} / SL ${sl})`,
          color: "green",
        });
      } catch (e: unknown) {
        notifications.show({
          title: "OCO failed",
          message: e instanceof Error ? e.message : String(e),
          color: "red",
        });
      } finally {
        setLoading(false);
      }
      return;
    }

    // Bracket mode: entry is always a LIMIT order today.
    const entry = typeof price === "number" ? price : parseFloat(price);
    if (!entry || entry <= 0) {
      notifications.show({ message: "Enter a valid entry price", color: "red" });
      return;
    }
    // Sanity check: TP and SL should be on opposite sides of entry.
    if (side === "BUY" && (tp <= entry || sl >= entry)) {
      notifications.show({
        message: "For a BUY bracket, TP must be above entry and SL below",
        color: "red",
      });
      return;
    }
    if (side === "SELL" && (tp >= entry || sl <= entry)) {
      notifications.show({
        message: "For a SELL bracket, TP must be below entry and SL above",
        color: "red",
      });
      return;
    }

    setLoading(true);
    try {
      await sagaClient.place({
        accountId,
        plan: {
          case: "bracket",
          value: {
            symbol,
            entrySide: side === "BUY" ? Side.BUY : Side.SELL,
            entryPrice: moneyToPrice(entry),
            entryQuantity: BigInt(qty),
            takeProfitPrice: moneyToPrice(tp),
            stopLossPrice: moneyToPrice(sl),
            positionSide: ps,
          },
        },
      });
      notifications.show({
        title: "Bracket placed",
        message: `${side} ${qty} ${symbol} @ ${entry} (TP ${tp} / SL ${sl})`,
        color: "green",
      });
    } catch (e: unknown) {
      notifications.show({
        title: "Bracket failed",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setLoading(false);
    }
  }

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Title order={5}>Place Order</Title>
        <SegmentedControl
          fullWidth
          size="xs"
          value={mode}
          onChange={(v) => setMode((v as Mode) ?? "SINGLE")}
          data={[
            { label: "Single", value: "SINGLE" },
            { label: "Bracket", value: "BRACKET" },
            { label: "OCO", value: "OCO" },
          ]}
        />
        <SegmentedControl
          fullWidth
          value={side}
          onChange={setSide}
          data={[
            { label: "Buy", value: "BUY" },
            { label: "Sell", value: "SELL" },
          ]}
          color={side === "BUY" ? "green" : "red"}
        />
        <SegmentedControl
          fullWidth
          size="xs"
          value={positionSide}
          onChange={setPositionSide}
          data={[
            { label: "Long", value: "LONG" },
            { label: "Short", value: "SHORT" },
          ]}
          color={positionSide === "SHORT" ? "orange" : "blue"}
        />
        {isSingle && (
          <Group grow>
            <Select
              label="Type"
              size="xs"
              value={orderType}
              onChange={handleOrderTypeChange}
              data={[
                { label: "Limit", value: "LIMIT" },
                { label: "Market", value: "MARKET" },
              ]}
              checkIconPosition="right"
            />
            <Select
              label="TIF"
              size="xs"
              value={tif}
              onChange={(v) => setTif(v ?? "IOC")}
              data={isMarket ? MARKET_TIF_OPTIONS : LIMIT_TIF_OPTIONS}
              checkIconPosition="right"
            />
          </Group>
        )}
        {((isSingle && !isMarket) || isBracket) && (
          <NumberInput
            label={isBracket ? "Entry Price" : "Price"}
            size="xs"
            placeholder="0.00"
            min={0}
            decimalScale={4}
            value={price}
            onChange={setPrice}
          />
        )}
        <NumberInput
          label="Quantity"
          size="xs"
          placeholder="0"
          min={1}
          allowDecimal={false}
          value={quantity}
          onChange={setQuantity}
        />
        {!isSingle && (
          <Group grow>
            <NumberInput
              label="Take Profit"
              size="xs"
              placeholder="0.00"
              min={0}
              decimalScale={4}
              value={takeProfit}
              onChange={setTakeProfit}
            />
            <NumberInput
              label="Stop Loss"
              size="xs"
              placeholder="0.00"
              min={0}
              decimalScale={4}
              value={stopLoss}
              onChange={setStopLoss}
            />
          </Group>
        )}
        {preview && (
          <Stack gap={4}>
            <Group justify="space-between" gap="xs">
              <Text size="xs" c="dimmed">
                Buying power impact
              </Text>
              <Text size="xs" c={insufficient ? "red" : undefined} fw={600}>
                {preview.buyingPowerImpact === 0n
                  ? "—"
                  : `-${formatMoney(preview.buyingPowerImpact)}`}
                {margin && (
                  <Text component="span" size="xs" c="dimmed" ml={6}>
                    (avail {formatMoney(margin.buyingPower)})
                  </Text>
                )}
              </Text>
            </Group>
            {margin && (
              <Group justify="space-between" gap="xs">
                <Text size="xs" c="dimmed">
                  Equity after fill
                </Text>
                <Text size="xs" fw={600}>
                  {formatMoney(preview.projectedEquity)}
                  <Text component="span" size="xs" c="dimmed" ml={6}>
                    ({margin.equity === preview.projectedEquity
                      ? "no change"
                      : `${preview.projectedEquity > margin.equity ? "+" : ""}${formatMoney(preview.projectedEquity - margin.equity)}`})
                  </Text>
                </Text>
              </Group>
            )}
            {margin &&
              preview.projectedMaintenanceRequirement !==
                margin.maintenanceRequirement && (
              <Group justify="space-between" gap="xs">
                <Text size="xs" c="dimmed">
                  Maint. req. after fill
                </Text>
                <Text size="xs" fw={600} c={wouldCauseCall ? "red" : undefined}>
                  {formatMoney(preview.projectedMaintenanceRequirement)}
                  <Text component="span" size="xs" c="dimmed" ml={6}>
                    ({preview.projectedMaintenanceRequirement >
                    margin.maintenanceRequirement
                      ? "+"
                      : ""}
                    {formatMoney(
                      preview.projectedMaintenanceRequirement -
                        margin.maintenanceRequirement,
                    )}
                    )
                  </Text>
                </Text>
              </Group>
            )}
            {preview.warnings.map((w) => (
              <Text key={w} size="xs" c="red">
                ⚠ {w}
              </Text>
            ))}
          </Stack>
        )}
        <Button
          fullWidth
          color={side === "BUY" ? "green" : "red"}
          loading={loading}
          onClick={handleSubmit}
          disabled={insufficient}
        >
          {isSingle && (side === "BUY" ? "Buy" : "Sell")}
          {isBracket && `${side} Bracket`}
          {isOCO && `${side} OCO`} {symbol}
        </Button>
      </Stack>
    </Card>
  );
}
