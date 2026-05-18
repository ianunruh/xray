import { useState } from "react";
import { ActionIcon, Card, Group, Stack, Table, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Side } from "../../src/gen/orderbook/v1/events_pb";
import { BracketPhase } from "../../src/gen/saga/v1/saga_pb";
import { sagaClient } from "~/lib/client";
import { formatPrice, formatQuantity } from "~/lib/format";

export type BracketRow = {
  sagaId: string;
  symbol: string;
  entrySide: Side;
  entryPrice: bigint;
  entryQuantity: bigint;
  takeProfitPrice: bigint;
  stopLossPrice: bigint;
  phase: BracketPhase;
};

function phaseName(p: BracketPhase): string {
  switch (p) {
    case BracketPhase.PENDING_ENTRY:
      return "Entry";
    case BracketPhase.PENDING_EXIT:
      return "Exit";
    default:
      return "—";
  }
}

function sideName(s: Side): string {
  return s === Side.BUY ? "BUY" : s === Side.SELL ? "SELL" : "—";
}

export function BracketsPanel({
  rows,
  onJumpToAggregate,
}: {
  rows: BracketRow[];
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
        title: "Bracket cancelled",
        message: `Cancelled bracket for ${symbol}`,
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
        <Title order={5}>Active Brackets</Title>
        <Table striped highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Symbol</Table.Th>
              <Table.Th>Side</Table.Th>
              <Table.Th ta="right">Entry</Table.Th>
              <Table.Th ta="right">Qty</Table.Th>
              <Table.Th ta="right">TP</Table.Th>
              <Table.Th ta="right">SL</Table.Th>
              <Table.Th>Phase</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.map((b) => (
              <Table.Tr key={b.sagaId}>
                <Table.Td>{b.symbol}</Table.Td>
                <Table.Td c={b.entrySide === Side.BUY ? "green" : "red"}>
                  {sideName(b.entrySide)}
                </Table.Td>
                <Table.Td ta="right">{formatPrice(b.entryPrice)}</Table.Td>
                <Table.Td ta="right">{formatQuantity(b.entryQuantity)}</Table.Td>
                <Table.Td ta="right">{formatPrice(b.takeProfitPrice)}</Table.Td>
                <Table.Td ta="right">{formatPrice(b.stopLossPrice)}</Table.Td>
                <Table.Td>{phaseName(b.phase)}</Table.Td>
                <Table.Td>
                  <Group gap={4} wrap="nowrap" justify="flex-end">
                    {onJumpToAggregate && (
                      <ActionIcon
                        size="xs"
                        variant="subtle"
                        color="grape"
                        onClick={() => onJumpToAggregate(`bracket-saga:${b.sagaId}`)}
                        title="View saga in Events"
                      >
                        ⇢
                      </ActionIcon>
                    )}
                    <ActionIcon
                      size="xs"
                      variant="subtle"
                      color="red"
                      loading={cancellingId === b.sagaId}
                      onClick={() => handleCancel(b.sagaId, b.symbol)}
                      title="Cancel bracket"
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
