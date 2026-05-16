import { useState } from "react";
import {
  Button,
  Card,
  Group,
  NumberInput,
  SegmentedControl,
  Select,
  Stack,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Side, OrderType, TimeInForce } from "../gen/orderbook/v1/events_pb";
import { sagaClient } from "../client";
import { moneyToPrice } from "../format";

const ORDER_TYPES: Record<string, OrderType> = {
  LIMIT: OrderType.LIMIT,
  MARKET: OrderType.MARKET,
};

const TIME_IN_FORCE: Record<string, TimeInForce> = {
  GTC: TimeInForce.GTC,
  IOC: TimeInForce.IOC,
  FOK: TimeInForce.FOK,
  DAY: TimeInForce.DAY,
};

const LIMIT_TIF_OPTIONS = [
  { label: "GTC", value: "GTC" },
  { label: "IOC", value: "IOC" },
  { label: "FOK", value: "FOK" },
  { label: "DAY", value: "DAY" },
];

const MARKET_TIF_OPTIONS = [
  { label: "IOC", value: "IOC" },
  { label: "FOK", value: "FOK" },
];

export function OrderForm({
  accountId,
  symbol,
}: {
  accountId: string;
  symbol: string;
}) {
  const [side, setSide] = useState<string>("BUY");
  const [orderType, setOrderType] = useState<string>("MARKET");
  const [price, setPrice] = useState<number | string>("");
  const [quantity, setQuantity] = useState<number | string>(100);
  const [tif, setTif] = useState<string>("IOC");
  const [loading, setLoading] = useState(false);

  const isMarket = orderType === "MARKET";

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
    const prc = typeof price === "number" ? price : parseFloat(price);
    if (!qty || qty <= 0) {
      notifications.show({ message: "Enter a valid quantity", color: "red" });
      return;
    }
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
  }

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Title order={5}>Place Order</Title>
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
        {!isMarket && (
          <NumberInput
            label="Price"
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
        <Button
          fullWidth
          color={side === "BUY" ? "green" : "red"}
          loading={loading}
          onClick={handleSubmit}
        >
          {side === "BUY" ? "Buy" : "Sell"} {symbol}
        </Button>
      </Stack>
    </Card>
  );
}
