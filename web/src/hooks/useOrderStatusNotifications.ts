import { useEffect, useRef } from "react";
import { notifications } from "@mantine/notifications";
import { formatPrice, formatQuantity } from "../format";
import { Side } from "../gen/orderbook/v1/events_pb";
import {
  OrderStatus,
  type PendingOrder,
} from "../gen/portfolio/v1/service_pb";
import { useAccountData } from "./accountData";

function sideVerb(side: Side): string {
  return side === Side.BUY ? "Buy" : side === Side.SELL ? "Sell" : "Order";
}

function showFilled(o: PendingOrder) {
  notifications.show({
    title: `${sideVerb(o.side)} ${o.symbol} filled`,
    message: `${formatQuantity(o.filledQuantity)} @ ${formatPrice(o.price)}`,
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
  notifications.show({
    title: `${sideVerb(o.side)} ${o.symbol} partial fill`,
    message: `${formatQuantity(o.filledQuantity)} of ${formatQuantity(o.quantity)} @ ${formatPrice(o.price)}`,
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
        o.filledQuantity < o.quantity &&
        o.status !== OrderStatus.COMPLETED
      ) {
        // Skip when this tick filled the rest — the COMPLETED transition
        // on the next tick (or this one, if the saga runs them together)
        // already covers it.
        showPartialFill(o);
      }
    }

    prevRef.current = next;
    initializedRef.current = true;
  }, [portfolio]);
}
