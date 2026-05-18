import { Badge, Card, Group, Stack, Table, Text, Title } from "@mantine/core";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import { PositionSide, Side } from "../../src/gen/orderbook/v1/events_pb";
import { usePnL } from "../hooks/usePnL";
import { formatMoney, formatQuantity } from "~/lib/format";

const HISTORY_LIMIT = 25;

function sideName(s: Side): string {
  return s === Side.BUY ? "BUY" : s === Side.SELL ? "SELL" : "—";
}

function positionBadge(ps: PositionSide) {
  if (ps === PositionSide.SHORT) {
    return (
      <Badge size="xs" color="red" variant="light">
        SHORT
      </Badge>
    );
  }
  return (
    <Badge size="xs" color="blue" variant="light">
      LONG
    </Badge>
  );
}

function formatTime(ts: Timestamp | undefined): string {
  if (!ts) return "—";
  const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000);
  return new Date(ms).toLocaleTimeString();
}

// RecentPnLPanel renders the per-fill realized-P&L history from
// PortfolioService.GetPnL. Only entries with non-zero realized P&L are
// shown — these are the closing fills (SELL+LONG or BUY+SHORT) that
// actually crystallize a gain or loss. Opening fills appear in the
// portfolio orders table instead.
export function RecentPnLPanel({ accountId }: { accountId: string }) {
  const pnl = usePnL(accountId);

  if (!pnl) {
    return null;
  }

  // Newest first; cap to HISTORY_LIMIT rows. The server already orders
  // by settled_at ascending, so reverse for newest-first display.
  const closing = pnl.history
    .filter((h) => h.realizedPnl !== 0n)
    .slice(-HISTORY_LIMIT)
    .reverse();

  if (closing.length === 0) {
    return null;
  }

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Group justify="space-between" align="baseline">
          <Title order={5}>Recent Realized P&L</Title>
          <Text size="xs" c="dimmed">
            Last {closing.length} closing fill{closing.length === 1 ? "" : "s"}
          </Text>
        </Group>
        <Table striped highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Time</Table.Th>
              <Table.Th>Symbol</Table.Th>
              <Table.Th>Side</Table.Th>
              <Table.Th ta="right">Qty</Table.Th>
              <Table.Th ta="right">Price</Table.Th>
              <Table.Th ta="right">Realized P&L</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {closing.map((h, i) => (
              <Table.Tr key={`${h.symbol}-${h.side}-${i}`}>
                <Table.Td>
                  <Text size="xs" ff="monospace">
                    {formatTime(h.settledAt)}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Group gap={4} wrap="nowrap">
                    {h.symbol}
                    {positionBadge(h.positionSide)}
                  </Group>
                </Table.Td>
                <Table.Td c={h.side === Side.BUY ? "green" : "red"}>
                  {sideName(h.side)}
                </Table.Td>
                <Table.Td ta="right">{formatQuantity(h.quantity)}</Table.Td>
                <Table.Td ta="right">{formatMoney(h.price)}</Table.Td>
                <Table.Td
                  ta="right"
                  c={h.realizedPnl >= 0n ? "green" : "red"}
                  fw={600}
                >
                  {formatMoney(h.realizedPnl)}
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Stack>
    </Card>
  );
}
