import { useState } from "react";
import { ActionIcon, Card, Group, Stack, Table, Text, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Side } from "../gen/orderbook/v1/events_pb";
import { OCOPhase } from "../gen/saga/v1/saga_pb";
import { sagaClient } from "../client";
import { useOcos } from "../hooks/useOcos";
import { formatPrice, formatQuantity } from "../format";

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
  accountId,
  onJumpToAggregate,
}: {
  accountId: string;
  onJumpToAggregate?: (aggregateId: string) => void;
}) {
  const ocos = useOcos(accountId);
  const [cancellingId, setCancellingId] = useState<string | null>(null);

  if (ocos.length === 0) {
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
            {ocos.map((o) => {
              const d = o.details.case === "oco" ? o.details.value : null;
              if (!d) {
                return (
                  <Table.Tr key={o.sagaId}>
                    <Table.Td colSpan={8}>
                      <Text c="dimmed" size="xs">
                        Saga {o.sagaId}: missing OCO details
                      </Text>
                    </Table.Td>
                  </Table.Tr>
                );
              }
              return (
                <Table.Tr key={o.sagaId}>
                  <Table.Td>{o.symbol}</Table.Td>
                  <Table.Td c={d.exitSide === Side.BUY ? "green" : "red"}>
                    {sideName(d.exitSide)}
                  </Table.Td>
                  <Table.Td ta="right">{formatQuantity(d.quantity)}</Table.Td>
                  <Table.Td ta="right">{formatPrice(d.takeProfitPrice)}</Table.Td>
                  <Table.Td ta="right">{formatPrice(d.stopLossPrice)}</Table.Td>
                  <Table.Td ta="right">{formatQuantity(d.settledQuantity)}</Table.Td>
                  <Table.Td>{phaseName(d.phase)}</Table.Td>
                  <Table.Td>
                    <Group gap={4} wrap="nowrap" justify="flex-end">
                      {onJumpToAggregate && (
                        <ActionIcon
                          size="xs"
                          variant="subtle"
                          color="grape"
                          onClick={() => onJumpToAggregate(`oco-saga:${o.sagaId}`)}
                          title="View saga in Diagnostics"
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
              );
            })}
          </Table.Tbody>
        </Table>
      </Stack>
    </Card>
  );
}
