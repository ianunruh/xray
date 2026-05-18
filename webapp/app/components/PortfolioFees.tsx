import { Badge, Card, Group, Stack, Table, Text, Title } from "@mantine/core";
import type { Timestamp } from "@bufbuild/protobuf/wkt";
import {
  FeeKind,
  type FeeRecord,
} from "../../src/gen/portfolio/v1/service_pb";
import { formatMoney } from "~/lib/format";

// kindBadge maps the proto enum to a label + color so the table can
// scan-by-kind at a glance: blue for routine transaction fees, orange
// for margin interest (financing cost), red for short borrow (risk
// premium).
function kindBadge(kind: FeeKind): { label: string; color: string } {
  switch (kind) {
    case FeeKind.TRANSACTION:
      return { label: "Transaction", color: "blue" };
    case FeeKind.MARGIN_INTEREST:
      return { label: "Margin Int.", color: "orange" };
    case FeeKind.SHORT_BORROW:
      return { label: "Short Borrow", color: "red" };
    default:
      return { label: "Unknown", color: "gray" };
  }
}

function formatTs(ts: Timestamp | undefined): { short: string; full: string } {
  if (!ts) return { short: "—", full: "" };
  const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000);
  const d = new Date(ms);
  return {
    short: d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
    full: d.toISOString(),
  };
}

// formatBpsAsPct turns the stored bps value into a percent display:
// 600 bps → "6.00%", 12 bps → "0.12%".
function formatBpsAsPct(bps: bigint): string {
  const pct = Number(bps) / 100;
  return `${pct.toFixed(2)}%`;
}

// detailText composes the per-kind "Detail" cell. Centralised so all
// three kinds keep a consistent voice and column width budget.
function detailText(r: FeeRecord): string {
  switch (r.kind) {
    case FeeKind.TRANSACTION: {
      const shortId = r.relatedId ? r.relatedId.slice(0, 8) + "…" : "—";
      const notional = r.notional > 0n ? ` · ${formatMoney(r.notional)} notional` : "";
      return `${shortId}${notional}`;
    }
    case FeeKind.MARGIN_INTEREST: {
      // For interest, "amount" already reflects the principal at the
      // accrual moment via the engine, but the projection doesn't
      // store principal — the rate alone is the most useful detail.
      return r.rateBps > 0n ? `${formatBpsAsPct(r.rateBps)} annual` : "—";
    }
    case FeeKind.SHORT_BORROW: {
      // ShortBorrowFeeAccrued's wire fields don't carry qty/mark
      // separately in the projection (only rate). Could expand the
      // schema later; rate alone is the most useful summary today.
      const rate = r.rateBps > 0n ? `${formatBpsAsPct(r.rateBps)} annual` : "—";
      return rate;
    }
    default:
      return "—";
  }
}

export function PortfolioFees({ rows }: { rows: FeeRecord[] }) {
  if (rows.length === 0) {
    return (
      <Card withBorder>
        <Text c="dimmed">No fees charged yet.</Text>
      </Card>
    );
  }

  // Summary chips over the visible window — keeps the eye-test trivial
  // for "where is the cash going" without scrolling the whole table.
  let totalTxn = 0n;
  let totalInterest = 0n;
  let totalBorrow = 0n;
  for (const r of rows) {
    switch (r.kind) {
      case FeeKind.TRANSACTION:
        totalTxn += r.amount;
        break;
      case FeeKind.MARGIN_INTEREST:
        totalInterest += r.amount;
        break;
      case FeeKind.SHORT_BORROW:
        totalBorrow += r.amount;
        break;
    }
  }

  return (
    <Card withBorder>
      <Stack gap="sm">
        <Group justify="space-between" align="center">
          <Title order={6}>Fee &amp; Interest History</Title>
          <Group gap="md">
            <SummaryChip
              label="Transactions"
              color="blue"
              value={formatMoney(totalTxn)}
            />
            <SummaryChip
              label="Margin Interest"
              color="orange"
              value={formatMoney(totalInterest)}
            />
            <SummaryChip
              label="Short Borrow"
              color="red"
              value={formatMoney(totalBorrow)}
            />
          </Group>
        </Group>
        <Table striped highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Time</Table.Th>
              <Table.Th>Kind</Table.Th>
              <Table.Th>Symbol</Table.Th>
              <Table.Th ta="right">Amount</Table.Th>
              <Table.Th>Detail</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.map((r, i) => {
              const ts = formatTs(r.chargedAt);
              const badge = kindBadge(r.kind);
              return (
                <Table.Tr key={i}>
                  <Table.Td>
                    <Text size="xs" ff="monospace" title={ts.full}>
                      {ts.short}
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    <Badge color={badge.color} variant="light" size="sm">
                      {badge.label}
                    </Badge>
                  </Table.Td>
                  <Table.Td>{r.symbol || "—"}</Table.Td>
                  <Table.Td ta="right" ff="monospace">
                    {r.amount === 0n ? (
                      <Text component="span" c="dimmed" ff="monospace">
                        $0.00
                      </Text>
                    ) : (
                      formatMoney(r.amount)
                    )}
                  </Table.Td>
                  <Table.Td>
                    <Text size="xs" c="dimmed" ff="monospace">
                      {detailText(r)}
                    </Text>
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

function SummaryChip({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color: string;
}) {
  return (
    <Group gap={4} align="baseline">
      <Text size="xs" c={color}>
        {label}
      </Text>
      <Text size="xs" fw={700} ff="monospace">
        {value}
      </Text>
    </Group>
  );
}

