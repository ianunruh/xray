import { useEffect, useRef, useState } from "react";
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
import { orderBookClient } from "../client";
import { phaseColor, phaseLabel } from "../hooks/useOrderBookPhase";
import { MarketPhase, Side } from "../gen/orderbook/v1/events_pb";
import type { UncrossResponse } from "../gen/orderbook/v1/service_pb";
import { formatPrice, formatQuantity } from "../format";

type ActionKind = "open" | "closing" | "uncross" | "close";

type PendingAction = {
  symbol: string;
  phase: MarketPhase;
  kind: ActionKind;
};

const POLL_INTERVAL_MS = 3000;

// MarketStatePanel lists every known symbol with its current market
// phase and exposes per-symbol controls for transitioning between
// CONTINUOUS / AUCTION / CLOSING_AUCTION / CLOSED. Phase polling is
// best-effort — a per-symbol GetOrderBook call every few seconds.
export function MarketStatePanel() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [phases, setPhases] = useState<Record<string, MarketPhase>>({});
  const [loading, setLoading] = useState(false);
  const [pending, setPending] = useState<PendingAction | null>(null);
  const [reason, setReason] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [confirmOpened, confirmHandlers] = useDisclosure(false);
  // The most recent uncross result, keyed by symbol — surfaced in a
  // small inline summary on the row so traders can see clearing price
  // without digging through events.
  const [lastUncross, setLastUncross] = useState<
    Record<string, UncrossResponse>
  >({});
  // Guard against overlapping refresh loops on slow networks.
  const inflightRef = useRef(false);

  async function refresh() {
    if (inflightRef.current) return;
    inflightRef.current = true;
    setLoading(true);
    try {
      const { symbols: list } = await orderBookClient.listSymbols({});
      setSymbols(list);
      const results = await Promise.all(
        list.map(async (s) => {
          try {
            const r = await orderBookClient.getOrderBook({ symbol: s });
            return [s, r.phase] as const;
          } catch {
            return [s, MarketPhase.UNSPECIFIED] as const;
          }
        }),
      );
      setPhases((prev) => {
        const next = { ...prev };
        for (const [s, p] of results) {
          next[s] = p === MarketPhase.UNSPECIFIED ? MarketPhase.CONTINUOUS : p;
        }
        return next;
      });
    } catch (e: unknown) {
      notifications.show({
        title: "Failed to load markets",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setLoading(false);
      inflightRef.current = false;
    }
  }

  useEffect(() => {
    refresh();
    const id = window.setInterval(refresh, POLL_INTERVAL_MS);
    return () => window.clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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

  async function execute() {
    if (!pending) return;
    setSubmitting(true);
    const { symbol, kind } = pending;
    try {
      switch (kind) {
        case "open": {
          await orderBookClient.openAuction({ symbol, reason });
          notifications.show({
            title: `${symbol}: opening auction`,
            message: "Phase → AUCTION",
            color: "yellow",
          });
          break;
        }
        case "closing": {
          await orderBookClient.beginClosingAuction({ symbol, reason });
          notifications.show({
            title: `${symbol}: closing auction`,
            message: "Phase → CLOSING_AUCTION",
            color: "orange",
          });
          break;
        }
        case "uncross": {
          const resp = await orderBookClient.uncross({ symbol });
          setLastUncross((prev) => ({ ...prev, [symbol]: resp }));
          notifications.show({
            title: `${symbol}: uncrossed`,
            message:
              resp.matchedQty > 0n
                ? `Cleared ${formatQuantity(resp.matchedQty)} @ ${formatPrice(resp.clearingPrice)} (${resp.tradeCount} trades)`
                : "No match — book was one-sided or non-crossing",
            color: "green",
          });
          break;
        }
        case "close": {
          const resp = await orderBookClient.closeMarket({ symbol });
          notifications.show({
            title: `${symbol}: close-market processed`,
            message: `${resp.cancelledOrders} DAY order${resp.cancelledOrders === 1 ? "" : "s"} cancelled`,
            color: "blue",
          });
          break;
        }
      }
      closeModal();
      refresh();
    } catch (e: unknown) {
      notifications.show({
        title: `${symbol}: action failed`,
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Title order={4}>Markets</Title>
        <Button
          size="xs"
          variant="default"
          onClick={refresh}
          loading={loading && symbols.length === 0}
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
          visible={loading && symbols.length === 0}
          zIndex={2}
          overlayProps={{ blur: 1 }}
        />
        <Table highlightOnHover striped>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Symbol</Table.Th>
              <Table.Th>Phase</Table.Th>
              <Table.Th>Last Uncross</Table.Th>
              <Table.Th ta="right">Actions</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {symbols.length === 0 && !loading && (
              <Table.Tr>
                <Table.Td colSpan={4}>
                  <Text size="sm" c="dimmed">
                    No symbols yet — place an order to bootstrap one.
                  </Text>
                </Table.Td>
              </Table.Tr>
            )}
            {symbols.map((s) => {
              const phase = phases[s] ?? MarketPhase.CONTINUOUS;
              return (
                <MarketRow
                  key={s}
                  symbol={s}
                  phase={phase}
                  lastUncross={lastUncross[s]}
                  onAction={(k) => startAction(s, phase, k)}
                />
              );
            })}
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

function MarketRow({
  symbol,
  phase,
  lastUncross,
  onAction,
}: {
  symbol: string;
  phase: MarketPhase;
  lastUncross: UncrossResponse | undefined;
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
