import { useEffect } from "react";
import { ActionIcon, Card, Group, Stack, Table, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { useFetcher } from "react-router";
import { Side } from "../../src/gen/orderbook/v1/events_pb";
import { BracketPhase } from "../../src/gen/saga/v1/saga_pb";
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

type CancelResult = { ok: boolean; intent: string; error?: string };

export function BracketsPanel({
  rows,
  onJumpToAggregate,
}: {
  rows: BracketRow[];
  onJumpToAggregate?: (aggregateId: string) => void;
}) {
  const fetcher = useFetcher<CancelResult>();
  // Track which row is in flight by reading the submission's sagaId off
  // formData — avoids a parallel useState.
  const cancellingId =
    fetcher.state !== "idle" &&
    fetcher.formData?.get("intent") === "cancel-saga"
      ? String(fetcher.formData.get("sagaId") ?? "")
      : null;

  useEffect(() => {
    if (fetcher.state !== "idle" || !fetcher.data) return;
    const data = fetcher.data;
    if (data.ok) {
      notifications.show({
        title: "Bracket cancelled",
        message: "",
        color: "green",
      });
    } else if (data.error) {
      notifications.show({
        title: "Cancel failed",
        message: data.error,
        color: "red",
      });
    }
  }, [fetcher.state, fetcher.data]);

  if (rows.length === 0) {
    return null;
  }

  function handleCancel(sagaId: string) {
    const fd = new FormData();
    fd.set("intent", "cancel-saga");
    fd.set("sagaId", sagaId);
    fetcher.submit(fd, { method: "post" });
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
                      onClick={() => handleCancel(b.sagaId)}
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
