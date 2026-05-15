import { Table, Text, Title } from "@mantine/core";
import { formatPrice, formatQuantity } from "../format";
import type { PriceLevel } from "../gen/orderbook/v1/service_pb";

function depthBackground(
  quantity: bigint,
  maxQuantity: bigint,
  side: "bid" | "ask",
): string {
  const pct = maxQuantity > 0n ? Number((quantity * 100n) / maxQuantity) : 0;
  const color =
    side === "bid" ? "rgba(0,200,0,0.1)" : "rgba(200,0,0,0.1)";
  const dir = side === "bid" ? "to left" : "to right";
  return `linear-gradient(${dir}, ${color} ${pct}%, transparent ${pct}%)`;
}

export function DepthSide({
  title,
  levels,
  side,
  maxQuantity,
}: {
  title: string;
  levels: PriceLevel[];
  side: "bid" | "ask";
  maxQuantity: bigint;
}) {
  return (
    <>
      <Title order={6} c={side === "bid" ? "green" : "red"}>
        {title}
      </Title>
      <Table>
        <Table.Thead>
          <Table.Tr>
            <Table.Th ta="right">Price</Table.Th>
            <Table.Th ta="right">Qty</Table.Th>
            <Table.Th ta="right">#</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {levels.map((level, i) => (
            <Table.Tr
              key={i}
              style={{
                background: depthBackground(
                  level.quantity,
                  maxQuantity,
                  side,
                ),
              }}
            >
              <Table.Td ta="right">
                <Text size="xs" c={side === "bid" ? "green" : "red"} ff="monospace">
                  {formatPrice(level.price)}
                </Text>
              </Table.Td>
              <Table.Td ta="right">
                <Text size="xs" ff="monospace">
                  {formatQuantity(level.quantity)}
                </Text>
              </Table.Td>
              <Table.Td ta="right">
                <Text size="xs" ff="monospace">
                  {level.orderCount}
                </Text>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </>
  );
}
