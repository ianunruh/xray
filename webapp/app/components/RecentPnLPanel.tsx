import { Badge, Card, Group, Stack, Table, Text, Title } from "@mantine/core";
import { PositionSide, Side } from "../../src/gen/orderbook/v1/events_pb";
import { formatMoney, formatQuantity } from "~/lib/format";

export type RealizedPnlRow = {
  symbol: string;
  side: Side;
  positionSide: PositionSide;
  quantity: bigint;
  price: bigint;
  realizedPnl: bigint;
  settledAtMs: number;
};

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

function formatTime(ms: number): string {
  if (!ms) return "—";
  return new Date(ms).toLocaleTimeString();
}

// RecentPnLPanel renders the per-fill realized-P&L history. Loader
// pre-filters to closing fills (SELL+LONG or BUY+SHORT — fills that
// actually crystallize a gain or loss) and caps the list.
export function RecentPnLPanel({ rows }: { rows: RealizedPnlRow[] }) {
  if (rows.length === 0) {
    return null;
  }

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Group justify="space-between" align="baseline">
          <Title order={5}>Recent Realized P&L</Title>
          <Text size="xs" c="dimmed">
            Last {rows.length} closing fill{rows.length === 1 ? "" : "s"}
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
            {rows.map((h, i) => (
              <Table.Tr key={`${h.symbol}-${h.side}-${i}`}>
                <Table.Td>
                  <Text size="xs" ff="monospace">
                    {formatTime(h.settledAtMs)}
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
