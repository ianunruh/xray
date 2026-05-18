import { useState } from "react";
import { ActionIcon, Card, Group, Stack, Table, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Side } from "../../src/gen/orderbook/v1/events_pb";
import { OCOPhase } from "../../src/gen/saga/v1/saga_pb";
import { sagaClient } from "~/lib/client";
import { formatPrice, formatQuantity } from "~/lib/format";

export type OcoRow = {
  sagaId: string;
  symbol: string;
  exitSide: Side;
  quantity: bigint;
  takeProfitPrice: bigint;
  stopLossPrice: bigint;
  settledQuantity: bigint;
  phase: OCOPhase;
};

function phaseName(p: OCOPhase): string {
  switch (p) {
    case OCOPhase.OCO_PHASE_STARTED:
      return "Holding";
    case OCOPhase.OCO_PHASE_SHARES_HELD:
      return "Placing";
    case OCOPhase.OCO_PHASE_EXIT_PLACED:
      return "Active";
    default:
      return "—";
  }
}

function sideName(s: Side): string {
  return s === Side.BUY ? "BUY" : s === Side.SELL ? "SELL" : "—";
}

export function OcosPanel({
  rows,
  onJumpToAggregate,
}: {
  rows: OcoRow[];
  onJumpToAggregate?: (aggregateId: string) => void;
}) {
  const [cancellingId, setCancellingId] = useState<string | null>(null);

  if (rows.length === 0) {
    return null;
  }

  async function handleCancel(sagaId: string, symbol: string) {
    setCancellingId(sagaId);
    try {
      await sagaClient.cancel({ sagaId });
      notifications.show({
        title: "OCO cancelled",
        message: `Cancelled OCO for ${symbol}`,
        color: "green",
      });
    } catch (e: unknown) {
      notifications.show({
        title: "Cancel failed",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setCancellingId(null);
    }
  }

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Title order={5}>Active OCOs</Title>
        <Table striped highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Symbol</Table.Th>
              <Table.Th>Exit</Table.Th>
              <Table.Th ta="right">Qty</Table.Th>
              <Table.Th ta="right">TP</Table.Th>
              <Table.Th ta="right">SL</Table.Th>
              <Table.Th ta="right">Settled</Table.Th>
              <Table.Th>Phase</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.map((o) => (
              <Table.Tr key={o.sagaId}>
                <Table.Td>{o.symbol}</Table.Td>
                <Table.Td c={o.exitSide === Side.BUY ? "green" : "red"}>
                  {sideName(o.exitSide)}
                </Table.Td>
                <Table.Td ta="right">{formatQuantity(o.quantity)}</Table.Td>
                <Table.Td ta="right">{formatPrice(o.takeProfitPrice)}</Table.Td>
                <Table.Td ta="right">{formatPrice(o.stopLossPrice)}</Table.Td>
                <Table.Td ta="right">{formatQuantity(o.settledQuantity)}</Table.Td>
                <Table.Td>{phaseName(o.phase)}</Table.Td>
                <Table.Td>
                  <Group gap={4} wrap="nowrap" justify="flex-end">
                    {onJumpToAggregate && (
                      <ActionIcon
                        size="xs"
                        variant="subtle"
                        color="grape"
                        onClick={() => onJumpToAggregate(`oco-saga:${o.sagaId}`)}
                        title="View saga in Events"
                      >
                        ⇢
                      </ActionIcon>
                    )}
                    <ActionIcon
                      size="xs"
                      variant="subtle"
                      color="red"
                      loading={cancellingId === o.sagaId}
                      onClick={() => handleCancel(o.sagaId, o.symbol)}
                      title="Cancel OCO"
                    >
                      X
                    </ActionIcon>
                  </Group>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Stack>
    </Card>
  );
}
