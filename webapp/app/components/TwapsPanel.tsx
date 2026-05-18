import { useState } from "react";
import {
  ActionIcon,
  Card,
  Group,
  Progress,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Side } from "../../src/gen/orderbook/v1/events_pb";
import type { TWAPDetails } from "../../src/gen/saga/v1/saga_pb";
import { sagaClient } from "~/lib/client";
import { useTwaps } from "../hooks/useTwaps";
import { formatPrice, formatQuantity } from "~/lib/format";

function sideName(s: Side): string {
  return s === Side.BUY ? "BUY" : s === Side.SELL ? "SELL" : "—";
}

// avgFillPrice derives the weighted-avg fill price across all completed
// slices as cash_settled / filled_qty. Returns null when nothing has
// filled yet, so the UI can render a dash.
function avgFillPrice(d: TWAPDetails): bigint | null {
  if (d.totalFilledQuantity === 0n) return null;
  return d.totalCashSettled / d.totalFilledQuantity;
}

export function TwapsPanel({
  accountId,
  onJumpToAggregate,
}: {
  accountId: string;
  onJumpToAggregate?: (aggregateId: string) => void;
}) {
  const twaps = useTwaps(accountId);
  const [cancellingId, setCancellingId] = useState<string | null>(null);

  if (twaps.length === 0) {
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
            {twaps.map((t) => {
              const d = t.details.case === "twap" ? t.details.value : null;
              if (!d) {
                return (
                  <Table.Tr key={t.sagaId}>
                    <Table.Td colSpan={8}>
                      <Text c="dimmed" size="xs">
                        Saga {t.sagaId}: missing TWAP details
                      </Text>
                    </Table.Td>
                  </Table.Tr>
                );
              }
              const sliceCount = d.sliceCount;
              const launched = d.slicesLaunched;
              const completed = d.slices.filter((s) => s.completed).length;
              // Two-segment progress: filled completed (green) + in-flight
              // launched-but-not-completed (yellow). Remaining is grey.
              const completedPct = sliceCount > 0 ? (completed / sliceCount) * 100 : 0;
              const inFlightPct =
                sliceCount > 0 ? ((launched - completed) / sliceCount) * 100 : 0;
              const avg = avgFillPrice(d);
              const intervalSec = Number(d.sliceIntervalMs) / 1000;
              return (
                <Table.Tr key={t.sagaId}>
                  <Table.Td>{t.symbol}</Table.Td>
                  <Table.Td c={d.side === Side.BUY ? "green" : "red"}>
                    {sideName(d.side)}
                  </Table.Td>
                  <Table.Td ta="right">{formatQuantity(d.totalQuantity)}</Table.Td>
                  <Table.Td ta="right">{formatPrice(d.limitPrice)}</Table.Td>
                  <Table.Td>
                    <Tooltip
                      label={`${completed}/${sliceCount} slices filled · every ${intervalSec}s`}
                    >
                      <Progress.Root size="md" w={120}>
                        <Progress.Section value={completedPct} color="green" />
                        <Progress.Section value={inFlightPct} color="yellow" />
                      </Progress.Root>
                    </Tooltip>
                  </Table.Td>
                  <Table.Td ta="right">{formatQuantity(d.totalFilledQuantity)}</Table.Td>
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
                          title="View saga in Diagnostics"
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
