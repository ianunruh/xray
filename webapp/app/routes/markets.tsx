import { useEffect, useState } from "react";
import {
  Badge,
  Box,
  Button,
  Group,
  LoadingOverlay,
  Modal,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import { useFetcher, useRevalidator } from "react-router";
import { Code, ConnectError } from "@connectrpc/connect";
import type { Route } from "./+types/markets";
import { orderBookClient } from "~/lib/client.server";
import { formatPrice, formatQuantity } from "~/lib/format";
import { phaseColor, phaseLabel } from "~/lib/marketPhase";
import { MarketPhase, Side } from "../../src/gen/orderbook/v1/events_pb";

type ActionKind = "open" | "closing" | "uncross" | "close";

type PendingAction = {
  symbol: string;
  phase: MarketPhase;
  kind: ActionKind;
};

type UncrossSummary = {
  clearingPrice: bigint;
  matchedQty: bigint;
  imbalanceQty: bigint;
  imbalanceSide: Side;
  tradeCount: number;
};

type MarketRow = {
  symbol: string;
  phase: MarketPhase;
  lastTradePrice: bigint;
  // Most recent official close price; 0 means no close on record yet.
  lastClosePrice: bigint;
  lastCloseDate: string;
};

export async function loader() {
  const { symbols } = await orderBookClient.listSymbols({});
  const rows: MarketRow[] = await Promise.all(
    symbols.map(async (s) => {
      const [bookR, closeR] = await Promise.allSettled([
        orderBookClient.getOrderBook({ symbol: s }),
        orderBookClient.getOfficialClose({ symbol: s, sessionDate: "" }),
      ]);

      const phase =
        bookR.status === "fulfilled" &&
        bookR.value.phase !== MarketPhase.UNSPECIFIED
          ? bookR.value.phase
          : MarketPhase.CONTINUOUS;
      const lastTradePrice =
        bookR.status === "fulfilled" ? bookR.value.lastTradePrice : 0n;

      let lastClosePrice = 0n;
      let lastCloseDate = "";
      if (closeR.status === "fulfilled") {
        lastClosePrice = closeR.value.closePrice;
        lastCloseDate = closeR.value.sessionDate;
      } else if (
        !(
          closeR.reason instanceof ConnectError &&
          (closeR.reason.code === Code.NotFound ||
            closeR.reason.code === Code.Unimplemented)
        )
      ) {
        // Unexpected error — surface in the server log but degrade to "no close".
        console.error(`getOfficialClose(${s}) failed`, closeR.reason);
      }

      return { symbol: s, phase, lastTradePrice, lastClosePrice, lastCloseDate };
    }),
  );
  return { rows };
}

type ActionResult =
  | {
      ok: true;
      intent: ActionKind;
      symbol: string;
      uncross?: UncrossSummary;
      cancelledOrders?: number;
    }
  | { ok: false; intent: ActionKind; symbol: string; error: string };

export async function action({
  request,
}: Route.ActionArgs): Promise<ActionResult> {
  const form = await request.formData();
  const intent = String(form.get("intent") ?? "") as ActionKind;
  const symbol = String(form.get("symbol") ?? "");
  const reason = String(form.get("reason") ?? "");

  if (!symbol) {
    return { ok: false, intent, symbol, error: "missing symbol" };
  }

  try {
    switch (intent) {
      case "open":
        await orderBookClient.openAuction({ symbol, reason });
        return { ok: true, intent, symbol };
      case "closing":
        await orderBookClient.beginClosingAuction({ symbol, reason });
        return { ok: true, intent, symbol };
      case "uncross": {
        const r = await orderBookClient.uncross({ symbol });
        return {
          ok: true,
          intent,
          symbol,
          uncross: {
            clearingPrice: r.clearingPrice,
            matchedQty: r.matchedQty,
            imbalanceQty: r.imbalanceQty,
            imbalanceSide: r.imbalanceSide,
            tradeCount: r.tradeCount,
          },
        };
      }
      case "close": {
        const r = await orderBookClient.closeMarket({ symbol });
        return {
          ok: true,
          intent,
          symbol,
          cancelledOrders: r.cancelledOrders,
        };
      }
      default:
        return {
          ok: false,
          intent,
          symbol,
          error: `unknown intent: ${intent}`,
        };
    }
  } catch (e: unknown) {
    return {
      ok: false,
      intent,
      symbol,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

export default function Markets({ loaderData }: Route.ComponentProps) {
  const { rows } = loaderData;
  const revalidator = useRevalidator();
  const fetcher = useFetcher<typeof action>();

  const [pending, setPending] = useState<PendingAction | null>(null);
  const [reason, setReason] = useState("");
  const [confirmOpened, confirmHandlers] = useDisclosure(false);
  const [lastUncross, setLastUncross] = useState<
    Record<string, UncrossSummary>
  >({});

  // Refresh phases every 3s — there's no server-pushed phase stream yet,
  // so we poll GetOrderBook through the loader.
  useEffect(() => {
    const id = window.setInterval(() => {
      if (revalidator.state === "idle") revalidator.revalidate();
    }, 3000);
    return () => window.clearInterval(id);
  }, [revalidator]);

  // React to action completion: surface notifications and capture uncross
  // response into the per-symbol summary map.
  useEffect(() => {
    if (fetcher.state !== "idle" || !fetcher.data) return;
    const data = fetcher.data;
    if (!data.ok) {
      notifications.show({
        title: `${data.symbol}: action failed`,
        message: data.error,
        color: "red",
      });
      return;
    }
    switch (data.intent) {
      case "open":
        notifications.show({
          title: `${data.symbol}: opening auction`,
          message: "Phase → AUCTION",
          color: "yellow",
        });
        confirmHandlers.close();
        setPending(null);
        setReason("");
        break;
      case "closing":
        notifications.show({
          title: `${data.symbol}: closing auction`,
          message: "Phase → CLOSING_AUCTION",
          color: "orange",
        });
        confirmHandlers.close();
        setPending(null);
        setReason("");
        break;
      case "uncross":
        if (data.uncross) {
          setLastUncross((prev) => ({ ...prev, [data.symbol]: data.uncross! }));
          notifications.show({
            title: `${data.symbol}: uncrossed`,
            message:
              data.uncross.matchedQty > 0n
                ? `Cleared ${formatQuantity(data.uncross.matchedQty)} @ ${formatPrice(data.uncross.clearingPrice)} (${data.uncross.tradeCount} trades)`
                : "No match — book was one-sided or non-crossing",
            color: "green",
          });
        }
        confirmHandlers.close();
        setPending(null);
        setReason("");
        break;
      case "close":
        notifications.show({
          title: `${data.symbol}: close-market processed`,
          message: `${data.cancelledOrders ?? 0} DAY order${data.cancelledOrders === 1 ? "" : "s"} cancelled`,
          color: "blue",
        });
        confirmHandlers.close();
        setPending(null);
        setReason("");
        break;
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fetcher.state, fetcher.data]);

  function startAction(symbol: string, phase: MarketPhase, kind: ActionKind) {
    setPending({ symbol, phase, kind });
    setReason("");
    confirmHandlers.open();
  }

  function closeModal() {
    confirmHandlers.close();
    setPending(null);
    setReason("");
  }

  function execute() {
    if (!pending) return;
    const fd = new FormData();
    fd.set("intent", pending.kind);
    fd.set("symbol", pending.symbol);
    if (needsReason(pending.kind)) fd.set("reason", reason);
    fetcher.submit(fd, { method: "post" });
  }

  const loading = revalidator.state === "loading";
  const submitting = fetcher.state !== "idle";

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Title order={4}>Markets</Title>
        <Button
          size="xs"
          variant="default"
          onClick={() => revalidator.revalidate()}
          loading={loading && rows.length === 0}
        >
          Refresh
        </Button>
      </Group>

      <Text size="sm" c="dimmed">
        Drive each symbol's session lifecycle. Open an auction to halt
        continuous matching and accumulate orders; uncross to clear at a
        single equilibrium price. Closing-auction → uncross marks the
        official close and locks the book.
      </Text>

      <Box pos="relative">
        <LoadingOverlay
          visible={loading && rows.length === 0}
          zIndex={2}
          overlayProps={{ blur: 1 }}
        />
        <Table highlightOnHover striped>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Symbol</Table.Th>
              <Table.Th>Phase</Table.Th>
              <Table.Th ta="right">Last</Table.Th>
              <Table.Th ta="right">Change vs Close</Table.Th>
              <Table.Th>Last Uncross</Table.Th>
              <Table.Th ta="right">Actions</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.length === 0 && !loading && (
              <Table.Tr>
                <Table.Td colSpan={6}>
                  <Text size="sm" c="dimmed">
                    No symbols yet — place an order to bootstrap one.
                  </Text>
                </Table.Td>
              </Table.Tr>
            )}
            {rows.map((r) => (
              <Row
                key={r.symbol}
                symbol={r.symbol}
                phase={r.phase}
                lastTradePrice={r.lastTradePrice}
                lastClosePrice={r.lastClosePrice}
                lastCloseDate={r.lastCloseDate}
                lastUncross={lastUncross[r.symbol]}
                onAction={(k) => startAction(r.symbol, r.phase, k)}
              />
            ))}
          </Table.Tbody>
        </Table>
      </Box>

      <Modal
        opened={confirmOpened}
        onClose={closeModal}
        title={pending ? modalTitle(pending) : ""}
      >
        {pending && (
          <Stack gap="sm">
            <ActionDescription action={pending} />
            {needsReason(pending.kind) && (
              <TextInput
                label="Reason (optional)"
                placeholder={defaultReason(pending.kind)}
                value={reason}
                onChange={(e) => setReason(e.currentTarget.value)}
                autoFocus
              />
            )}
            <Group justify="flex-end" gap="sm">
              <Button variant="default" onClick={closeModal}>
                Cancel
              </Button>
              <Button
                color={confirmColor(pending.kind)}
                onClick={execute}
                loading={submitting}
              >
                {confirmLabel(pending.kind)}
              </Button>
            </Group>
          </Stack>
        )}
      </Modal>
    </Stack>
  );
}

function Row({
  symbol,
  phase,
  lastTradePrice,
  lastClosePrice,
  lastCloseDate,
  lastUncross,
  onAction,
}: {
  symbol: string;
  phase: MarketPhase;
  lastTradePrice: bigint;
  lastClosePrice: bigint;
  lastCloseDate: string;
  lastUncross: UncrossSummary | undefined;
  onAction: (kind: ActionKind) => void;
}) {
  return (
    <Table.Tr>
      <Table.Td>
        <Text size="sm" ff="monospace" fw={600}>
          {symbol}
        </Text>
      </Table.Td>
      <Table.Td>
        <Badge size="sm" variant="filled" color={phaseColor(phase)}>
          {phaseLabel(phase)}
        </Badge>
      </Table.Td>
      <Table.Td ta="right">
        {lastTradePrice > 0n ? (
          <Text size="sm" ff="monospace">
            {formatPrice(lastTradePrice)}
          </Text>
        ) : (
          <Text size="xs" c="dimmed">
            —
          </Text>
        )}
      </Table.Td>
      <Table.Td ta="right">
        <ChangeCell
          lastTradePrice={lastTradePrice}
          lastClosePrice={lastClosePrice}
          lastCloseDate={lastCloseDate}
        />
      </Table.Td>
      <Table.Td>
        {lastUncross ? (
          <Stack gap={0}>
            <Text size="xs" ff="monospace">
              {lastUncross.matchedQty > 0n
                ? `${formatQuantity(lastUncross.matchedQty)} @ ${formatPrice(lastUncross.clearingPrice)}`
                : "no match"}
            </Text>
            {lastUncross.imbalanceQty > 0n && (
              <Text size="xs" c="dimmed">
                imbalance {formatQuantity(lastUncross.imbalanceQty)}{" "}
                {lastUncross.imbalanceSide === Side.BUY ? "buy" : "sell"}
              </Text>
            )}
          </Stack>
        ) : (
          <Text size="xs" c="dimmed">
            —
          </Text>
        )}
      </Table.Td>
      <Table.Td ta="right">
        <Group gap="xs" justify="flex-end">
          {phase === MarketPhase.CONTINUOUS && (
            <>
              <Button
                size="xs"
                variant="light"
                color="yellow"
                onClick={() => onAction("open")}
              >
                Open Auction
              </Button>
              <Button
                size="xs"
                variant="light"
                color="orange"
                onClick={() => onAction("closing")}
              >
                Begin Closing
              </Button>
              <Button
                size="xs"
                variant="light"
                color="blue"
                onClick={() => onAction("close")}
              >
                Close Market
              </Button>
            </>
          )}
          {phase === MarketPhase.AUCTION && (
            <Button
              size="xs"
              variant="light"
              color="green"
              onClick={() => onAction("uncross")}
            >
              Uncross → Continuous
            </Button>
          )}
          {phase === MarketPhase.CLOSING_AUCTION && (
            <Button
              size="xs"
              variant="light"
              color="red"
              onClick={() => onAction("uncross")}
            >
              Uncross → Closed
            </Button>
          )}
          {phase === MarketPhase.CLOSED && (
            <Button
              size="xs"
              variant="light"
              color="yellow"
              onClick={() => onAction("open")}
            >
              Open Auction
            </Button>
          )}
        </Group>
      </Table.Td>
    </Table.Tr>
  );
}

function ChangeCell({
  lastTradePrice,
  lastClosePrice,
  lastCloseDate,
}: {
  lastTradePrice: bigint;
  lastClosePrice: bigint;
  lastCloseDate: string;
}) {
  if (lastClosePrice <= 0n) {
    return (
      <Text size="xs" c="dimmed">
        no close yet
      </Text>
    );
  }
  if (lastTradePrice <= 0n) {
    return (
      <Text size="xs" c="dimmed" title={`close ${lastCloseDate}`}>
        no trade since close
      </Text>
    );
  }
  const delta = lastTradePrice - lastClosePrice;
  // Percent in basis points to keep bigint arithmetic exact, then format.
  const pctBp = (delta * 1000000n) / lastClosePrice;
  const pct = Number(pctBp) / 10000;
  const sign = delta > 0n ? "+" : delta < 0n ? "−" : "";
  const absDelta = delta < 0n ? -delta : delta;
  const color = delta > 0n ? "teal" : delta < 0n ? "red" : "dimmed";
  return (
    <Stack gap={0} align="flex-end">
      <Text
        size="sm"
        ff="monospace"
        c={color}
        title={`vs official close ${lastCloseDate}: ${formatPrice(lastClosePrice)}`}
      >
        {sign}
        {formatPrice(absDelta)} ({sign}
        {Math.abs(pct).toFixed(2)}%)
      </Text>
    </Stack>
  );
}

function ActionDescription({ action }: { action: PendingAction }) {
  switch (action.kind) {
    case "open":
      return (
        <Text size="sm">
          Halts continuous matching on{" "}
          <Text component="span" ff="monospace" fw={600}>
            {action.symbol}
          </Text>{" "}
          and enters the opening-auction phase. New orders accumulate
          without crossing until Uncross fires.
        </Text>
      );
    case "closing":
      return (
        <Text size="sm">
          Freezes continuous matching on{" "}
          <Text component="span" ff="monospace" fw={600}>
            {action.symbol}
          </Text>
          . Only AT_CLOSE (MOC/LOC) orders and cancellations are accepted
          from here until Uncross runs the closing print.
        </Text>
      );
    case "uncross":
      return (
        <Text size="sm">
          {action.phase === MarketPhase.CLOSING_AUCTION
            ? "Runs the closing print — emits trades at the clearing price, locks the book to CLOSED, and records the official close."
            : "Runs the opening print — emits trades at the clearing price and resumes continuous matching."}{" "}
          Cannot be undone.
        </Text>
      );
    case "close":
      return (
        <Text size="sm">
          Cancels all open DAY orders on{" "}
          <Text component="span" ff="monospace" fw={600}>
            {action.symbol}
          </Text>
          . Does not change the market phase (use Begin Closing →
          Uncross for that). Cannot be undone.
        </Text>
      );
  }
}

function modalTitle(action: PendingAction): string {
  const verb = {
    open: "Open auction for",
    closing: "Begin closing auction for",
    uncross: "Uncross",
    close: "Close market for",
  }[action.kind];
  return `${verb} ${action.symbol}?`;
}

function needsReason(kind: ActionKind): boolean {
  return kind === "open" || kind === "closing";
}

function defaultReason(kind: ActionKind): string {
  return kind === "open" ? "session_open" : "session_close";
}

function confirmLabel(kind: ActionKind): string {
  switch (kind) {
    case "open":
      return "Open Auction";
    case "closing":
      return "Begin Closing";
    case "uncross":
      return "Uncross";
    case "close":
      return "Close Market";
  }
}

function confirmColor(kind: ActionKind): string {
  switch (kind) {
    case "open":
      return "yellow";
    case "closing":
      return "orange";
    case "uncross":
      return "red";
    case "close":
      return "blue";
  }
}
