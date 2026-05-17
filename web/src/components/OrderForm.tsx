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

// estimateBuyingPowerReduction mirrors the server-side hold the
// matching saga would create. Returns the bigint reduction, 0n when
// the order doesn't touch cash (long sell, short cover, OCO), or
// null when the inputs aren't ready to estimate (no price for a
// limit order, missing margin rate). Market orders are estimated
// from the typed price as a placeholder; the server walks the book
// for the real hold, so the displayed number will be conservative
// for market BUYs that sweep up.
function estimateBuyingPowerReduction(args: {
  mode: Mode;
  isMarket: boolean;
  side: string;
  positionSide: string;
  price: number | string;
  quantity: number | string;
  marginBps: bigint | null;
}): bigint | null {
  const { mode, isMarket, side, positionSide, price, quantity, marginBps } =
    args;
  if (mode === "OCO") return 0n; // exits an existing position
  const qty = typeof quantity === "number" ? quantity : parseInt(quantity, 10);
  if (!qty || qty <= 0) return null;
  const prc = typeof price === "number" ? price : parseFloat(price);
  if (isMarket && mode === "SINGLE" && side === "BUY" && positionSide === "LONG") {
    // Without book depth, fall back to typed price (often blank for
    // market) — return null so we don't lie about the hold.
    if (!prc || prc <= 0) return null;
  }
  if (!prc || prc <= 0) {
    // Bracket entry and limit orders always require a price.
    if (mode === "BRACKET" || !isMarket) return null;
  }
  const notional = moneyToPrice(prc) * BigInt(qty);

  // Long sell and short cover hold shares/capacity, not cash.
  if (side === "SELL" && positionSide === "LONG") return 0n;
  if (side === "BUY" && positionSide === "SHORT") return 0n;

  // Long buy: cash hold = notional (limit) or estimated cost (market).
  if (side === "BUY" && positionSide === "LONG") return notional;

  // Short open: collateral = initial_margin_bps * notional / 10000.
  if (side === "SELL" && positionSide === "SHORT") {
    if (marginBps === null) return null;
    return (notional * marginBps) / 10_000n;
  }
  return null;
}

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

  // Estimated buying-power impact for the order being typed.
  // Returns null if it can't be computed (missing price for a limit,
  // unknown margin rate, etc.). Bracket entry uses the same hold as
  // a single order; OCO doesn't take cash (exits an existing position).
  const estImpact = estimateBuyingPowerReduction({
    mode,
    isMarket,
    side,
    positionSide,
    price,
    quantity,
    marginBps: margin?.initialMarginBps ?? null,
  });
  const insufficient =
    estImpact !== null &&
    margin !== null &&
    estImpact > margin.buyingPower;

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
        {estImpact !== null && (
          <Group justify="space-between" gap="xs">
            <Text size="xs" c="dimmed">
              Buying power impact
            </Text>
            <Text size="xs" c={insufficient ? "red" : undefined} fw={600}>
              {estImpact === 0n
                ? "—"
                : `-${formatMoney(estImpact)}`}
              {margin && (
                <Text component="span" size="xs" c="dimmed" ml={6}>
                  (avail {formatMoney(margin.buyingPower)})
                </Text>
              )}
            </Text>
          </Group>
        )}
        {insufficient && (
          <Text size="xs" c="red">
            Insufficient buying power.
          </Text>
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
