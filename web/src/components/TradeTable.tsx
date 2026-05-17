import { Badge, Group, Table, Text, Title } from "@mantine/core";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { formatPrice, formatQuantity } from "../format";
import { CrossType } from "../gen/orderbook/v1/events_pb";
import type { Trade } from "../gen/orderbook/v1/service_pb";

function crossBadge(ct: CrossType) {
  switch (ct) {
    case CrossType.OPENING:
      return (
        <Badge size="xs" color="yellow" variant="light">
          OPEN
        </Badge>
      );
    case CrossType.CLOSING:
      return (
        <Badge size="xs" color="orange" variant="filled">
          CLOSE
        </Badge>
      );
    case CrossType.HALT_REOPEN:
      return (
        <Badge size="xs" color="red" variant="light">
          REOPEN
        </Badge>
      );
    default:
      return null;
  }
}

// TradeTable renders a "Recent Trades" panel from a pre-sorted (newest-first)
// array of trades. The live TradeList and the replay view both feed it.
export function TradeTable({
  trades,
  title = "Recent Trades",
}: {
  trades: Trade[];
  title?: string;
}) {
  return (
    <>
      <Title order={6}>{title}</Title>
      <Table>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Time</Table.Th>
            <Table.Th ta="right">Price</Table.Th>
            <Table.Th ta="right">Qty</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {trades.map((t) => (
            <Table.Tr key={t.tradeId}>
              <Table.Td>
                <Group gap={4} wrap="nowrap">
                  <Text size="xs" ff="monospace">
                    {t.executedAt
                      ? timestampDate(t.executedAt).toLocaleTimeString()
                      : ""}
                  </Text>
                  {crossBadge(t.crossType)}
                </Group>
              </Table.Td>
              <Table.Td ta="right">
                <Text
                  size="xs"
                  ff="monospace"
                  fw={t.crossType !== CrossType.NONE ? 700 : 400}
                >
                  {formatPrice(t.price)}
                </Text>
              </Table.Td>
              <Table.Td ta="right">
                <Text size="xs" ff="monospace">
                  {formatQuantity(t.quantity)}
                </Text>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </>
  );
}
