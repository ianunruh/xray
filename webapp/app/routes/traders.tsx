import { useEffect, useState } from "react";
import {
  Badge,
  Box,
  Button,
  Group,
  LoadingOverlay,
  Modal,
  NumberInput,
  Select,
  Stack,
  Switch,
  Table,
  Text,
  TextInput,
  Title,
  Tooltip,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import { useFetcher, useRevalidator } from "react-router";
import type { Route } from "./+types/traders";
import { traderClient } from "~/lib/client.server";
import { formatMoney } from "~/lib/format";
import {
  type FormState,
  type MMFormFields,
  type NoiseFormFields,
  accountOf,
  buildConfig,
  depositOf,
  duplicateForm,
  emptyForm,
  formFromTrader,
  symbolOf,
  typeLabel,
} from "~/lib/traderForm";
import {
  TraderStatus,
  TraderType,
} from "../../src/gen/trader/v1/service_pb";

type TraderRow = {
  id: string;
  name: string;
  type: TraderType;
  status: TraderStatus;
  lastError: string;
  symbol: string;
  accountId: string;
  initialDeposit: bigint;
  form: FormState;
};

export async function loader() {
  const r = await traderClient.listTraders({});
  const traders: TraderRow[] = r.traders.map((t) => ({
    id: t.id,
    name: t.name,
    type: t.type,
    status: t.status,
    lastError: t.lastError,
    symbol: symbolOf(t),
    accountId: accountOf(t),
    initialDeposit: depositOf(t),
    form: formFromTrader(t),
  }));
  return { traders };
}

type ActionResult =
  | { ok: true; intent: string }
  | { ok: false; intent: string; error: string };

export async function action({
  request,
}: Route.ActionArgs): Promise<ActionResult> {
  const form = await request.formData();
  const intent = String(form.get("intent") ?? "");
  try {
    switch (intent) {
      case "create": {
        const formJson = String(form.get("form") ?? "");
        const startNow = form.get("startNow") === "true";
        const fs = JSON.parse(formJson) as FormState;
        if (!fs.name.trim()) throw new Error("name is required");
        await traderClient.createTrader({
          name: fs.name.trim(),
          type: fs.type,
          config: buildConfig(fs),
          start: startNow,
        });
        return { ok: true, intent };
      }
      case "update": {
        const formJson = String(form.get("form") ?? "");
        const fs = JSON.parse(formJson) as FormState;
        if (!fs.id) throw new Error("missing id");
        if (!fs.name.trim()) throw new Error("name is required");
        await traderClient.updateTrader({
          id: fs.id,
          name: fs.name.trim(),
          config: buildConfig(fs),
        });
        return { ok: true, intent };
      }
      case "start": {
        const id = String(form.get("id") ?? "");
        if (!id) throw new Error("missing id");
        await traderClient.startTrader({ id });
        return { ok: true, intent };
      }
      case "stop": {
        const id = String(form.get("id") ?? "");
        if (!id) throw new Error("missing id");
        await traderClient.stopTrader({ id });
        return { ok: true, intent };
      }
      case "delete": {
        const id = String(form.get("id") ?? "");
        if (!id) throw new Error("missing id");
        await traderClient.deleteTrader({ id });
        return { ok: true, intent };
      }
      case "startAll": {
        const r = await traderClient.startAllTraders({});
        if (r.failed > 0) {
          return {
            ok: false,
            intent,
            error: `${r.started} started, ${r.failed} failed — see status column`,
          };
        }
        return { ok: true, intent };
      }
      case "stopAll": {
        await traderClient.stopAllTraders({});
        return { ok: true, intent };
      }
      default:
        return { ok: false, intent, error: `unknown intent: ${intent}` };
    }
  } catch (e: unknown) {
    return {
      ok: false,
      intent,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

function statusBadge(t: TraderRow) {
  switch (t.status) {
    case TraderStatus.RUNNING:
      return <Badge color="green">running</Badge>;
    case TraderStatus.STOPPED:
      return <Badge color="gray">stopped</Badge>;
    case TraderStatus.FAILED:
      return (
        <Tooltip label={t.lastError || "unknown error"} multiline w={320}>
          <Badge color="red" style={{ cursor: "help" }}>
            failed
          </Badge>
        </Tooltip>
      );
    default:
      return <Badge color="gray">—</Badge>;
  }
}

export default function Traders({ loaderData }: Route.ComponentProps) {
  const { traders } = loaderData;
  const revalidator = useRevalidator();
  const mutationFetcher = useFetcher<typeof action>();
  const rowFetcher = useFetcher<typeof action>();
  const bulkFetcher = useFetcher<typeof action>();

  const [form, setForm] = useState<FormState>(emptyForm());
  const [editing, setEditing] = useState(false);
  const [modalOpened, modalHandlers] = useDisclosure(false);
  const [confirmDelete, setConfirmDelete] = useState<TraderRow | null>(null);
  const [deleteOpened, deleteHandlers] = useDisclosure(false);
  const [busyId, setBusyId] = useState<string | null>(null);

  // 3s polling refresh so RUNNING/FAILED transitions surface without
  // a manual click. Skip while a revalidation is already in flight.
  useEffect(() => {
    const id = window.setInterval(() => {
      if (revalidator.state === "idle") revalidator.revalidate();
    }, 3000);
    return () => window.clearInterval(id);
  }, [revalidator]);

  // React to mutation fetcher completion (save / delete from a modal).
  useEffect(() => {
    if (mutationFetcher.state !== "idle" || !mutationFetcher.data) return;
    const data = mutationFetcher.data;
    if (data.ok) {
      if (data.intent === "create" || data.intent === "update") {
        modalHandlers.close();
      } else if (data.intent === "delete") {
        deleteHandlers.close();
        setConfirmDelete(null);
      }
    } else if (data.error) {
      notifications.show({
        title: `${data.intent} failed`,
        message: data.error,
        color: "red",
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mutationFetcher.state, mutationFetcher.data]);

  // React to bulk start/stop completion — surface failures as notifications.
  useEffect(() => {
    if (bulkFetcher.state !== "idle" || !bulkFetcher.data) return;
    if (!bulkFetcher.data.ok) {
      notifications.show({
        title: `${bulkFetcher.data.intent} failed`,
        message: bulkFetcher.data.error,
        color: "red",
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bulkFetcher.state, bulkFetcher.data]);

  // React to per-row fetcher completion (start/stop button); clear busyId.
  useEffect(() => {
    if (rowFetcher.state !== "idle") return;
    if (rowFetcher.data && !rowFetcher.data.ok) {
      notifications.show({
        title: `${rowFetcher.data.intent} failed`,
        message: rowFetcher.data.error,
        color: "red",
      });
    }
    setBusyId(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rowFetcher.state, rowFetcher.data]);

  function openCreate() {
    setForm(emptyForm());
    setEditing(false);
    modalHandlers.open();
  }

  function openEdit(t: TraderRow) {
    setForm(t.form);
    setEditing(true);
    modalHandlers.open();
  }

  function openDuplicate(t: TraderRow) {
    setForm(
      duplicateForm(
        t.form,
        traders.map((x) => x.name),
        traders.map((x) => x.accountId).filter(Boolean),
      ),
    );
    setEditing(false);
    modalHandlers.open();
  }

  function bulkStart() {
    const fd = new FormData();
    fd.set("intent", "startAll");
    bulkFetcher.submit(fd, { method: "post" });
  }

  function bulkStop() {
    const fd = new FormData();
    fd.set("intent", "stopAll");
    bulkFetcher.submit(fd, { method: "post" });
  }

  function save(startNow: boolean) {
    if (!form.name.trim()) {
      notifications.show({
        title: "Name is required",
        message: "",
        color: "red",
      });
      return;
    }
    const fd = new FormData();
    fd.set("intent", editing ? "update" : "create");
    fd.set("form", JSON.stringify(form));
    if (!editing) fd.set("startNow", String(startNow));
    mutationFetcher.submit(fd, { method: "post" });
  }

  function toggleRun(t: TraderRow) {
    setBusyId(t.id);
    const fd = new FormData();
    fd.set("intent", t.status === TraderStatus.RUNNING ? "stop" : "start");
    fd.set("id", t.id);
    rowFetcher.submit(fd, { method: "post" });
  }

  function askDelete(t: TraderRow) {
    setConfirmDelete(t);
    deleteHandlers.open();
  }

  function doDelete() {
    if (!confirmDelete) return;
    const fd = new FormData();
    fd.set("intent", "delete");
    fd.set("id", confirmDelete.id);
    mutationFetcher.submit(fd, { method: "post" });
  }

  const loading = revalidator.state === "loading";
  const saving =
    mutationFetcher.state !== "idle" &&
    (mutationFetcher.formData?.get("intent") === "create" ||
      mutationFetcher.formData?.get("intent") === "update");
  const deleting =
    mutationFetcher.state !== "idle" &&
    mutationFetcher.formData?.get("intent") === "delete";
  const starting =
    bulkFetcher.state !== "idle" &&
    bulkFetcher.formData?.get("intent") === "startAll";
  const stopping =
    bulkFetcher.state !== "idle" &&
    bulkFetcher.formData?.get("intent") === "stopAll";
  const runningCount = traders.filter(
    (t) => t.status === TraderStatus.RUNNING,
  ).length;
  const startableCount = traders.length - runningCount;

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Title order={3}>Traders</Title>
        <Group gap="xs">
          <Button
            size="xs"
            variant="default"
            color="green"
            onClick={bulkStart}
            loading={starting}
            disabled={startableCount === 0}
          >
            Start all
          </Button>
          <Button
            size="xs"
            variant="default"
            color="red"
            onClick={bulkStop}
            loading={stopping}
            disabled={runningCount === 0}
          >
            Stop all
          </Button>
          <Button
            size="xs"
            variant="default"
            onClick={() => revalidator.revalidate()}
            loading={loading}
          >
            Refresh
          </Button>
          <Button size="xs" onClick={openCreate}>
            + New trader
          </Button>
        </Group>
      </Group>

      <Box pos="relative">
        <LoadingOverlay visible={loading && traders.length === 0} zIndex={1} />
        {traders.length === 0 && !loading ? (
          <Text c="dimmed">
            No traders yet. Create one to start posting orders against the
            book.
          </Text>
        ) : (
          <Table withTableBorder withColumnBorders striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Name</Table.Th>
                <Table.Th>Type</Table.Th>
                <Table.Th>Symbol</Table.Th>
                <Table.Th>Account</Table.Th>
                <Table.Th>Initial deposit</Table.Th>
                <Table.Th>Status</Table.Th>
                <Table.Th style={{ width: 340 }}>Actions</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {traders.map((t) => (
                <Table.Tr key={t.id}>
                  <Table.Td>
                    <Text fw={500}>{t.name}</Text>
                  </Table.Td>
                  <Table.Td>
                    <Badge variant="light">{typeLabel(t.type)}</Badge>
                  </Table.Td>
                  <Table.Td>{t.symbol}</Table.Td>
                  <Table.Td>
                    <Text size="xs" ff="monospace">
                      {t.accountId}
                    </Text>
                  </Table.Td>
                  <Table.Td>{formatMoney(t.initialDeposit)}</Table.Td>
                  <Table.Td>{statusBadge(t)}</Table.Td>
                  <Table.Td>
                    <Group gap="xs" wrap="nowrap" justify="flex-end">
                      <Button
                        size="xs"
                        variant={
                          t.status === TraderStatus.RUNNING
                            ? "default"
                            : "filled"
                        }
                        color={
                          t.status === TraderStatus.RUNNING ? "gray" : "green"
                        }
                        onClick={() => toggleRun(t)}
                        loading={busyId === t.id}
                      >
                        {t.status === TraderStatus.RUNNING ? "Stop" : "Start"}
                      </Button>
                      <Button
                        size="xs"
                        variant="default"
                        onClick={() => openEdit(t)}
                      >
                        Edit
                      </Button>
                      <Button
                        size="xs"
                        variant="default"
                        onClick={() => openDuplicate(t)}
                      >
                        Duplicate
                      </Button>
                      <Button
                        size="xs"
                        variant="subtle"
                        color="red"
                        onClick={() => askDelete(t)}
                      >
                        Delete
                      </Button>
                    </Group>
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        )}
      </Box>

      <Modal
        opened={modalOpened}
        onClose={modalHandlers.close}
        title={editing ? "Edit Trader" : "New Trader"}
        size="lg"
      >
        <Stack gap="sm">
          <Group grow>
            <TextInput
              label="Name"
              placeholder="e.g. mm-AAPL"
              value={form.name}
              onChange={(e) =>
                setForm({ ...form, name: e.currentTarget.value })
              }
              required
            />
            <Select
              label="Type"
              data={[
                { value: String(TraderType.MM), label: "mm (market maker)" },
                { value: String(TraderType.NOISE), label: "noise" },
              ]}
              value={String(form.type)}
              onChange={(v) =>
                setForm({ ...form, type: Number(v) as TraderType })
              }
              disabled={editing}
              checkIconPosition="right"
              allowDeselect={false}
            />
          </Group>

          {form.type === TraderType.MM ? (
            <MMFormSection
              value={form.mm}
              onChange={(mm) => setForm({ ...form, mm })}
            />
          ) : (
            <NoiseFormSection
              value={form.noise}
              onChange={(noise) => setForm({ ...form, noise })}
            />
          )}

          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={modalHandlers.close}>
              Cancel
            </Button>
            {!editing && (
              <Button
                variant="default"
                onClick={() => save(false)}
                loading={saving}
              >
                Save (stopped)
              </Button>
            )}
            <Button onClick={() => save(!editing)} loading={saving}>
              {editing ? "Save" : "Save & Start"}
            </Button>
          </Group>
        </Stack>
      </Modal>

      <Modal
        opened={deleteOpened}
        onClose={() => {
          deleteHandlers.close();
          setConfirmDelete(null);
        }}
        title="Delete trader?"
      >
        <Stack gap="sm">
          <Text size="sm">
            This will stop and remove <b>{confirmDelete?.name}</b>. The
            portfolio and any resting orders are not deleted.
          </Text>
          <Group justify="flex-end">
            <Button
              variant="default"
              onClick={() => {
                deleteHandlers.close();
                setConfirmDelete(null);
              }}
            >
              Cancel
            </Button>
            <Button color="red" onClick={doDelete} loading={deleting}>
              Delete
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
}

function MMFormSection({
  value,
  onChange,
}: {
  value: MMFormFields;
  onChange: (v: MMFormFields) => void;
}) {
  const set = <K extends keyof MMFormFields>(k: K, v: MMFormFields[K]) =>
    onChange({ ...value, [k]: v });
  return (
    <Stack gap="xs">
      <Group grow>
        <TextInput
          label="Symbol"
          placeholder="AAPL"
          value={value.symbol}
          onChange={(e) => set("symbol", e.currentTarget.value)}
          required
        />
        <TextInput
          label="Account ID"
          placeholder="mm-AAPL"
          value={value.accountId}
          onChange={(e) => set("accountId", e.currentTarget.value)}
          required
        />
      </Group>
      <Group grow>
        <NumberInput
          label="Initial deposit ($)"
          value={value.initialDeposit}
          onChange={(v) => set("initialDeposit", v as number | "")}
          min={0}
          decimalScale={4}
        />
        <NumberInput
          label="Initial shares"
          value={value.initialShares}
          onChange={(v) => set("initialShares", v as number | "")}
          min={0}
        />
      </Group>
      <Group grow>
        <NumberInput
          label="Spread ($)"
          description="Total spread between best bid and best ask"
          value={value.spread}
          onChange={(v) => set("spread", v as number | "")}
          min={0}
          decimalScale={4}
        />
        <NumberInput
          label="Quantity / level"
          value={value.quantity}
          onChange={(v) => set("quantity", v as number | "")}
          min={0}
        />
      </Group>
      <Group grow>
        <NumberInput
          label="Levels"
          value={value.levels}
          onChange={(v) => set("levels", v as number | "")}
          min={1}
        />
        <NumberInput
          label="Level spacing ($)"
          value={value.levelSpacing}
          onChange={(v) => set("levelSpacing", v as number | "")}
          min={0}
          decimalScale={4}
        />
      </Group>
      <Group grow>
        <NumberInput
          label="Max position"
          value={value.maxPosition}
          onChange={(v) => set("maxPosition", v as number | "")}
          min={1}
        />
        <NumberInput
          label="Requote interval (ms)"
          value={value.requoteIntervalMs}
          onChange={(v) => set("requoteIntervalMs", v as number | "")}
          min={100}
          step={1000}
        />
      </Group>
      <Group grow>
        <NumberInput
          label="Price move threshold ($)"
          value={value.priceMoveThreshold}
          onChange={(v) => set("priceMoveThreshold", v as number | "")}
          min={0}
          decimalScale={4}
        />
        <NumberInput
          label="Max skew ($)"
          description="Midprice shift at |position| == max_position"
          value={value.maxSkew}
          onChange={(v) => set("maxSkew", v as number | "")}
          min={0}
          decimalScale={4}
        />
      </Group>
    </Stack>
  );
}

function NoiseFormSection({
  value,
  onChange,
}: {
  value: NoiseFormFields;
  onChange: (v: NoiseFormFields) => void;
}) {
  const set = <K extends keyof NoiseFormFields>(
    k: K,
    v: NoiseFormFields[K],
  ) => onChange({ ...value, [k]: v });
  return (
    <Stack gap="xs">
      <Group grow>
        <TextInput
          label="Symbol"
          placeholder="AAPL"
          value={value.symbol}
          onChange={(e) => set("symbol", e.currentTarget.value)}
          required
        />
        <TextInput
          label="Account ID"
          placeholder="noise-AAPL"
          value={value.accountId}
          onChange={(e) => set("accountId", e.currentTarget.value)}
          required
        />
      </Group>
      <Group grow>
        <NumberInput
          label="Initial deposit ($)"
          value={value.initialDeposit}
          onChange={(v) => set("initialDeposit", v as number | "")}
          min={0}
          decimalScale={4}
        />
        <NumberInput
          label="Initial shares"
          value={value.initialShares}
          onChange={(v) => set("initialShares", v as number | "")}
          min={0}
        />
      </Group>
      <Switch
        label="Random initial shares"
        description="Credit a uniform random count in [0, initial_shares] at first start"
        checked={value.randomInitialShares}
        onChange={(e) => set("randomInitialShares", e.currentTarget.checked)}
      />
      <Group grow>
        <NumberInput
          label="Order interval (ms)"
          value={value.orderIntervalMs}
          onChange={(v) => set("orderIntervalMs", v as number | "")}
          min={100}
          step={1000}
        />
        <NumberInput
          label="Max position"
          value={value.maxPosition}
          onChange={(v) => set("maxPosition", v as number | "")}
          min={1}
        />
      </Group>
      <Group grow>
        <NumberInput
          label="Min quantity"
          value={value.minQuantity}
          onChange={(v) => set("minQuantity", v as number | "")}
          min={1}
        />
        <NumberInput
          label="Max quantity"
          value={value.maxQuantity}
          onChange={(v) => set("maxQuantity", v as number | "")}
          min={1}
        />
      </Group>
      <Group grow>
        <NumberInput
          label="Price jitter ($)"
          description="Limit orders land in [ref-jitter, ref+jitter]"
          value={value.priceJitter}
          onChange={(v) => set("priceJitter", v as number | "")}
          min={0}
          decimalScale={4}
        />
        <NumberInput
          label="Market order pct"
          description="0.0 = all limit, 1.0 = all market"
          value={value.marketOrderPct}
          onChange={(v) => set("marketOrderPct", v as number | "")}
          min={0}
          max={1}
          step={0.05}
          decimalScale={2}
        />
      </Group>
      <Group grow>
        <NumberInput
          label="Buy bias"
          description="0.0 = always sell, 0.5 = neutral, 1.0 = always buy"
          value={value.buyBias}
          onChange={(v) => set("buyBias", v as number | "")}
          min={0}
          max={1}
          step={0.05}
          decimalScale={2}
        />
        <NumberInput
          label="Order timeout (ms)"
          description="Cancel resting limit orders older than this (0 = default 5m)"
          value={value.orderTimeoutMs}
          onChange={(v) => set("orderTimeoutMs", v as number | "")}
          min={0}
          step={30000}
        />
      </Group>
    </Stack>
  );
}
