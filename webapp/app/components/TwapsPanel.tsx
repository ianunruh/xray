import { useEffect } from "react";
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
import { useFetcher } from "react-router";
import { Side } from "../../src/gen/orderbook/v1/events_pb";
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

type CancelResult = { ok: boolean; intent: string; error?: string };

export function TwapsPanel({
  rows,
  onJumpToAggregate,
}: {
  rows: TwapRow[];
  onJumpToAggregate?: (aggregateId: string) => void;
}) {
  const fetcher = useFetcher<CancelResult>();
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
        title: "TWAP cancelled",
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
                        onClick={() => handleCancel(t.sagaId)}
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
