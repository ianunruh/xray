import { useState } from "react";
import {
  Alert,
  Badge,
  Button,
  Card,
  Group,
  NumberInput,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Form, useNavigation, useRevalidator } from "react-router";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { ConnectError } from "@connectrpc/connect";
import type { Route } from "./+types/corporate-actions";
import { corpactionClient } from "~/lib/client.server";
import { formatMoney } from "~/lib/format";
import {
  ActionStatus,
  ActionType,
} from "../../src/gen/corpaction/v1/events_pb";

export async function loader() {
  const r = await corpactionClient.list({});
  return { actions: r.actions };
}

type ActionResult =
  | { ok: true; actionId: string }
  | { ok: false; error: string };

export async function action({
  request,
}: Route.ActionArgs): Promise<ActionResult> {
  const form = await request.formData();
  const type = Number(form.get("type") ?? 0) as ActionType;
  const symbol = String(form.get("symbol") ?? "").trim().toUpperCase();
  if (!symbol) return { ok: false, error: "symbol required" };

  const req: Parameters<typeof corpactionClient.declare>[0] = {
    symbol,
    type,
  };
  try {
    switch (type) {
      case ActionType.SPLIT: {
        const numerator = Number(form.get("splitNumerator") ?? 0);
        const denominator = Number(form.get("splitDenominator") ?? 0);
        if (numerator <= 0 || denominator <= 0) {
          throw new Error("split numerator and denominator must be positive");
        }
        req.splitNumerator = numerator;
        req.splitDenominator = denominator;
        const eff = String(form.get("effectiveDate") ?? "");
        if (!eff) throw new Error("effective date required");
        req.effectiveDate = timestampFromDate(new Date(eff));
        break;
      }
      case ActionType.CASH_DIVIDEND: {
        const perShareDollars = Number(form.get("perShareDollars") ?? 0);
        if (perShareDollars <= 0) {
          throw new Error("per-share dividend must be positive");
        }
        // Price units are 4 decimal places, so $0.24 = 2400.
        req.dividendPerShare = BigInt(Math.round(perShareDollars * 10000));
        const rec = String(form.get("recordDate") ?? "");
        const pay = String(form.get("payDate") ?? "");
        if (!rec || !pay) throw new Error("record and pay dates required");
        req.recordDate = timestampFromDate(new Date(rec));
        req.payDate = timestampFromDate(new Date(pay));
        break;
      }
      case ActionType.SYMBOL_CHANGE: {
        const newSymbol = String(form.get("newSymbol") ?? "").trim().toUpperCase();
        if (!newSymbol) throw new Error("new symbol required");
        if (newSymbol === symbol) throw new Error("new symbol must differ");
        req.newSymbol = newSymbol;
        const eff = String(form.get("effectiveDate") ?? "");
        if (!eff) throw new Error("effective date required");
        req.effectiveDate = timestampFromDate(new Date(eff));
        break;
      }
      default:
        throw new Error("select an action type");
    }
    const resp = await corpactionClient.declare(req);
    return { ok: true, actionId: resp.actionId };
  } catch (err) {
    const msg =
      err instanceof ConnectError
        ? err.rawMessage
        : err instanceof Error
          ? err.message
          : String(err);
    return { ok: false, error: msg };
  }
}

const TYPE_OPTIONS = [
  { value: String(ActionType.SPLIT), label: "Stock split" },
  { value: String(ActionType.CASH_DIVIDEND), label: "Cash dividend" },
  { value: String(ActionType.SYMBOL_CHANGE), label: "Symbol change (rename)" },
];

function actionTypeLabel(t: ActionType): string {
  switch (t) {
    case ActionType.SPLIT:
      return "Split";
    case ActionType.CASH_DIVIDEND:
      return "Dividend";
    case ActionType.SYMBOL_CHANGE:
      return "Rename";
    default:
      return "—";
  }
}

function statusBadge(s: ActionStatus) {
  switch (s) {
    case ActionStatus.DECLARED:
      return <Badge color="blue">Declared</Badge>;
    case ActionStatus.APPLIED:
      return <Badge color="green">Applied</Badge>;
    case ActionStatus.FAILED:
      return <Badge color="red">Failed</Badge>;
    default:
      return <Badge color="gray">—</Badge>;
  }
}

function formatLocalDateTime(ts: { seconds: bigint; nanos: number } | undefined): string {
  if (!ts) return "—";
  const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000);
  return new Date(ms).toLocaleString();
}

function describeAction(a: {
  type: ActionType;
  splitNumerator: number;
  splitDenominator: number;
  dividendPerShare: bigint;
  newSymbol: string;
}): string {
  switch (a.type) {
    case ActionType.SPLIT:
      return `${a.splitNumerator}-for-${a.splitDenominator}`;
    case ActionType.CASH_DIVIDEND:
      return `${formatMoney(a.dividendPerShare)} / share`;
    case ActionType.SYMBOL_CHANGE:
      return `→ ${a.newSymbol}`;
    default:
      return "—";
  }
}

export default function CorporateActions({
  loaderData,
  actionData,
}: Route.ComponentProps) {
  const { actions } = loaderData;
  const navigation = useNavigation();
  const revalidator = useRevalidator();
  const submitting = navigation.state === "submitting";

  // Re-fetch when an apply just happened so the ledger updates.
  // (The reactor cycle is async — actions go from Declared to Applied
  // on the next tick. A manual refresh button is below the form too.)
  const [type, setType] = useState<ActionType>(ActionType.SPLIT);

  return (
    <Stack gap="md" p="md">
      <Title order={3}>Corporate Actions</Title>
      <Text size="sm" c="dimmed">
        Declare a split, cash dividend, or symbol change. The reactor
        applies actions whose effective_date (splits/renames) or
        pay_date (dividends) has passed — runs on a 5m cadence by
        default. See the operations card on{" "}
        <a href="/projections">Projections</a> for tick status.
      </Text>

      <Card withBorder padding="md">
        <Stack gap="sm">
          <Title order={5}>Declare</Title>
          {actionData?.ok === false && (
            <Alert color="red">{actionData.error}</Alert>
          )}
          {actionData?.ok === true && (
            <Alert color="green">
              Declared action {actionData.actionId}. It will apply on its
              next reactor tick after the effective/pay date.
            </Alert>
          )}
          <Form method="post" replace>
            <Stack gap="sm">
              <Group grow>
                <Select
                  label="Type"
                  name="type"
                  data={TYPE_OPTIONS}
                  value={String(type)}
                  onChange={(v) => v && setType(Number(v) as ActionType)}
                  allowDeselect={false}
                />
                <TextInput
                  label="Symbol"
                  name="symbol"
                  placeholder="AAPL"
                  required
                />
              </Group>
              {type === ActionType.SPLIT && (
                <Group grow align="end">
                  <NumberInput
                    label="Numerator"
                    name="splitNumerator"
                    placeholder="2"
                    min={1}
                    required
                  />
                  <NumberInput
                    label="Denominator"
                    name="splitDenominator"
                    placeholder="1"
                    min={1}
                    required
                  />
                  <TextInput
                    label="Effective date"
                    name="effectiveDate"
                    type="datetime-local"
                    required
                  />
                </Group>
              )}
              {type === ActionType.CASH_DIVIDEND && (
                <Group grow align="end">
                  <NumberInput
                    label="Per-share ($)"
                    name="perShareDollars"
                    placeholder="0.24"
                    min={0.0001}
                    step={0.01}
                    decimalScale={4}
                    required
                  />
                  <TextInput
                    label="Record date"
                    name="recordDate"
                    type="datetime-local"
                    required
                  />
                  <TextInput
                    label="Pay date"
                    name="payDate"
                    type="datetime-local"
                    required
                  />
                </Group>
              )}
              {type === ActionType.SYMBOL_CHANGE && (
                <Group grow align="end">
                  <TextInput
                    label="New symbol"
                    name="newSymbol"
                    placeholder="META"
                    required
                  />
                  <TextInput
                    label="Effective date"
                    name="effectiveDate"
                    type="datetime-local"
                    required
                  />
                </Group>
              )}
              <Group justify="flex-end">
                <Button
                  variant="subtle"
                  color="gray"
                  type="button"
                  onClick={() => revalidator.revalidate()}
                >
                  Refresh ledger
                </Button>
                <Button type="submit" loading={submitting}>
                  Declare
                </Button>
              </Group>
            </Stack>
          </Form>
        </Stack>
      </Card>

      <Card withBorder padding="md">
        <Stack gap="sm">
          <Title order={5}>Ledger</Title>
          {actions.length === 0 ? (
            <Text size="sm" c="dimmed">
              No actions declared yet.
            </Text>
          ) : (
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Action ID</Table.Th>
                  <Table.Th>Symbol</Table.Th>
                  <Table.Th>Type</Table.Th>
                  <Table.Th>Details</Table.Th>
                  <Table.Th>When</Table.Th>
                  <Table.Th>Status</Table.Th>
                  <Table.Th>Touched</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {actions.map((a) => {
                  const trigger =
                    a.type === ActionType.CASH_DIVIDEND
                      ? a.payDate
                      : a.effectiveDate;
                  const when = formatLocalDateTime(trigger);
                  return (
                    <Table.Tr key={a.actionId}>
                      <Table.Td>
                        <Text size="xs" ff="monospace" c="dimmed">
                          {a.actionId.slice(0, 8)}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Text fw={500}>{a.symbol}</Text>
                      </Table.Td>
                      <Table.Td>{actionTypeLabel(a.type)}</Table.Td>
                      <Table.Td>{describeAction(a)}</Table.Td>
                      <Table.Td>
                        <Text size="xs">{when}</Text>
                      </Table.Td>
                      <Table.Td>{statusBadge(a.status)}</Table.Td>
                      <Table.Td>
                        <Text size="xs" c="dimmed">
                          {a.status === ActionStatus.APPLIED
                            ? `${a.holdersCount}h · ${a.ordersCount}o · ${a.sagasCount}s`
                            : a.failedReason || "—"}
                        </Text>
                      </Table.Td>
                    </Table.Tr>
                  );
                })}
              </Table.Tbody>
            </Table>
          )}
        </Stack>
      </Card>
    </Stack>
  );
}
