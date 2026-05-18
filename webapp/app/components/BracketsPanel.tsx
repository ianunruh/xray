import { useState } from "react";
import { ActionIcon, Card, Group, Stack, Table, Text, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Side } from "../../src/gen/orderbook/v1/events_pb";
import { BracketPhase } from "../../src/gen/saga/v1/saga_pb";
import { sagaClient } from "~/lib/client";
import { useBrackets } from "../hooks/useBrackets";
import { formatPrice, formatQuantity } from "~/lib/format";

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
  accountId,
  onJumpToAggregate,
}: {
  accountId: string;
  onJumpToAggregate?: (aggregateId: string) => void;
}) {
  const brackets = useBrackets(accountId);
  const [cancellingId, setCancellingId] = useState<string | null>(null);

  if (brackets.length === 0) {
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
            {brackets.map((b) => {
              const d = b.details.case === "bracket" ? b.details.value : null;
              if (!d) {
                return (
                  <Table.Tr key={b.sagaId}>
                    <Table.Td colSpan={8}>
                      <Text c="dimmed" size="xs">
                        Saga {b.sagaId}: missing bracket details
                      </Text>
                    </Table.Td>
                  </Table.Tr>
                );
              }
              return (
                <Table.Tr key={b.sagaId}>
                  <Table.Td>{b.symbol}</Table.Td>
                  <Table.Td c={d.entrySide === Side.BUY ? "green" : "red"}>
                    {sideName(d.entrySide)}
                  </Table.Td>
                  <Table.Td ta="right">{formatPrice(d.entryPrice)}</Table.Td>
                  <Table.Td ta="right">{formatQuantity(d.entryQuantity)}</Table.Td>
                  <Table.Td ta="right">{formatPrice(d.takeProfitPrice)}</Table.Td>
                  <Table.Td ta="right">{formatPrice(d.stopLossPrice)}</Table.Td>
                  <Table.Td>{phaseName(d.phase)}</Table.Td>
                  <Table.Td>
                    <Group gap={4} wrap="nowrap" justify="flex-end">
                      {onJumpToAggregate && (
                        <ActionIcon
                          size="xs"
                          variant="subtle"
                          color="grape"
                          onClick={() => onJumpToAggregate(`bracket-saga:${b.sagaId}`)}
                          title="View saga in Diagnostics"
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
              );
            })}
          </Table.Tbody>
        </Table>
      </Stack>
    </Card>
  );
}
