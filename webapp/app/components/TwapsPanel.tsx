import { useState } from "react";
import {
  ActionIcon,
  Card,
  Group,
  Progress,
  Stack,
  Table,
  Tooltip,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Side } from "../../src/gen/orderbook/v1/events_pb";
import { sagaClient } from "~/lib/client";
import { formatPrice, formatQuantity } from "~/lib/format";

export type TwapRow = {
  sagaId: string;
  symbol: string;
  side: Side;
  totalQuantity: bigint;
  limitPrice: bigint;
  totalFilledQuantity: bigint;
  totalCashSettled: bigint;
  sliceCount: number;
  slicesLaunched: number;
  completedSlices: number;
  sliceIntervalMs: bigint;
};

function sideName(s: Side): string {
  return s === Side.BUY ? "BUY" : s === Side.SELL ? "SELL" : "—";
}

function avgFillPrice(filled: bigint, cash: bigint): bigint | null {
  if (filled === 0n) return null;
  return cash / filled;
}

export function TwapsPanel({
  rows,
  onJumpToAggregate,
}: {
  rows: TwapRow[];
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
        title: "TWAP cancelled",
        message: `Cancelled TWAP for ${symbol}`,
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
        <Title order={5}>Active TWAPs</Title>
        <Table striped highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Symbol</Table.Th>
              <Table.Th>Side</Table.Th>
              <Table.Th ta="right">Total</Table.Th>
              <Table.Th ta="right">Limit</Table.Th>
              <Table.Th>Progress</Table.Th>
              <Table.Th ta="right">Filled</Table.Th>
              <Table.Th ta="right">Avg Px</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.map((t) => {
              // Two-segment progress: filled completed (green) + in-flight
              // launched-but-not-completed (yellow). Remaining is grey.
              const completedPct =
                t.sliceCount > 0 ? (t.completedSlices / t.sliceCount) * 100 : 0;
              const inFlightPct =
                t.sliceCount > 0
                  ? ((t.slicesLaunched - t.completedSlices) / t.sliceCount) * 100
                  : 0;
              const avg = avgFillPrice(t.totalFilledQuantity, t.totalCashSettled);
              const intervalSec = Number(t.sliceIntervalMs) / 1000;
              return (
                <Table.Tr key={t.sagaId}>
                  <Table.Td>{t.symbol}</Table.Td>
                  <Table.Td c={t.side === Side.BUY ? "green" : "red"}>
                    {sideName(t.side)}
                  </Table.Td>
                  <Table.Td ta="right">{formatQuantity(t.totalQuantity)}</Table.Td>
                  <Table.Td ta="right">{formatPrice(t.limitPrice)}</Table.Td>
                  <Table.Td>
                    <Tooltip
                      label={`${t.completedSlices}/${t.sliceCount} slices filled · every ${intervalSec}s`}
                    >
                      <Progress.Root size="md" w={120}>
                        <Progress.Section value={completedPct} color="green" />
                        <Progress.Section value={inFlightPct} color="yellow" />
                      </Progress.Root>
                    </Tooltip>
                  </Table.Td>
                  <Table.Td ta="right">
                    {formatQuantity(t.totalFilledQuantity)}
                  </Table.Td>
                  <Table.Td ta="right">
                    {avg === null ? "—" : formatPrice(avg)}
                  </Table.Td>
                  <Table.Td>
                    <Group gap={4} wrap="nowrap" justify="flex-end">
                      {onJumpToAggregate && (
                        <ActionIcon
                          size="xs"
                          variant="subtle"
                          color="grape"
                          onClick={() => onJumpToAggregate(`twap-saga:${t.sagaId}`)}
                          title="View saga in Events"
                        >
                          ⇢
                        </ActionIcon>
                      )}
                      <ActionIcon
                        size="xs"
                        variant="subtle"
                        color="red"
                        loading={cancellingId === t.sagaId}
                        onClick={() => handleCancel(t.sagaId, t.symbol)}
                        title="Cancel TWAP"
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
