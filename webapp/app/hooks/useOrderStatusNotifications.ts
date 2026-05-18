import { useEffect, useRef } from "react";
import { notifications } from "@mantine/notifications";
import { playFillDing } from "~/lib/audio";
import { formatPrice, formatQuantity } from "~/lib/format";
import { Side } from "../../src/gen/orderbook/v1/events_pb";
import {
  OrderStatus,
  type PendingOrder,
} from "../../src/gen/portfolio/v1/service_pb";
import { useAccountData } from "./accountData";

function sideVerb(side: Side): string {
  return side === Side.BUY ? "Buy" : side === Side.SELL ? "Sell" : "Order";
}

function showFilled(o: PendingOrder) {
  playFillDing();
  // Prefer the projection's last_fill_price (actual execution price);
  // fall back to the order's limit for the rare case where it hasn't
  // populated yet.
  const fillPrice = o.lastFillPrice > 0n ? o.lastFillPrice : o.price;
  const qty = formatQuantity(o.filledQuantity);
  const message =
    fillPrice > 0n ? `${qty} @ ${formatPrice(fillPrice)}` : qty;
  notifications.show({
    title: `${sideVerb(o.side)} ${o.symbol} filled`,
    message,
    color: "green",
  });
}

function showFailed(o: PendingOrder) {
  // failReason "cancelled" comes from a user/system cancel; treat it
  // as informational rather than an error so the colour isn't alarming.
  const reason = o.failReason ?? "";
  const isCancel = /cancel/i.test(reason);
  notifications.show({
    title: `${sideVerb(o.side)} ${o.symbol} ${isCancel ? "cancelled" : "failed"}`,
    message: reason || (isCancel ? "Order cancelled" : "Order failed"),
    color: isCancel ? "gray" : "red",
  });
}

function showPartialFill(o: PendingOrder) {
  const fillPrice = o.lastFillPrice > 0n ? o.lastFillPrice : o.price;
  const progress = `${formatQuantity(o.filledQuantity)} of ${formatQuantity(o.quantity)}`;
  const message =
    fillPrice > 0n ? `${progress} @ ${formatPrice(fillPrice)}` : progress;
  notifications.show({
    title: `${sideVerb(o.side)} ${o.symbol} partial fill`,
    message,
    color: "blue",
  });
}

// useOrderStatusNotifications watches the viewed portfolio's order
// list and fires toast notifications when an order transitions into a
// terminal state (filled / failed / cancelled) or accrues a partial
// fill. Skips the first snapshot for a given account so historical
// orders don't spam on initial mount or account switch.
export function useOrderStatusNotifications() {
  const { accountId, portfolio } = useAccountData();
  const prevRef = useRef<Map<string, PendingOrder>>(new Map());
  const initializedRef = useRef(false);

  useEffect(() => {
    prevRef.current = new Map();
    initializedRef.current = false;
  }, [accountId]);

  useEffect(() => {
    if (!portfolio) return;
    const prev = prevRef.current;
    const next = new Map<string, PendingOrder>();

    for (const o of portfolio.pendingOrders) {
      next.set(o.sagaId, o);
      if (!initializedRef.current) continue;

      const before = prev.get(o.sagaId);
      if (!before) {
        // Order first appears already in a terminal state — most likely
        // an IOC/market order that filled or got rejected between portfolio
        // ticks. Fire so the user still sees the outcome.
        if (o.status === OrderStatus.COMPLETED) showFilled(o);
        else if (o.status === OrderStatus.FAILED) showFailed(o);
        continue;
      }

      if (before.status !== o.status) {
        if (o.status === OrderStatus.COMPLETED) showFilled(o);
        else if (o.status === OrderStatus.FAILED) showFailed(o);
      } else if (
        o.filledQuantity > before.filledQuantity &&
        o.status !== OrderStatus.COMPLETED
      ) {
        // Fire even when this tick fills the remainder — the partial
        // toast carries the trade fill price, which the COMPLETED
        // toast doesn't have for market orders.
        showPartialFill(o);
      }
    }

    prevRef.current = next;
    initializedRef.current = true;
  }, [portfolio]);
}
