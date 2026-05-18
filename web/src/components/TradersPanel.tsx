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
import { traderClient } from "../client";
import {
  type MMConfig,
  type NoiseConfig,
  type Trader,
  type TraderConfig,
  TraderStatus,
  TraderType,
} from "../gen/trader/v1/service_pb";
import { formatMoney, moneyToPrice, priceToNumber } from "../format";

type FormState = {
  id?: string;
  name: string;
  type: TraderType;
  mm: MMFormFields;
  noise: NoiseFormFields;
};

type MMFormFields = {
  symbol: string;
  accountId: string;
  initialDeposit: number | "";
  initialShares: number | "";
  spread: number | "";
  quantity: number | "";
  levels: number | "";
  levelSpacing: number | "";
  maxPosition: number | "";
  requoteIntervalMs: number | "";
  priceMoveThreshold: number | "";
  maxSkew: number | "";
};

type NoiseFormFields = {
  symbol: string;
  accountId: string;
  initialDeposit: number | "";
  initialShares: number | "";
  randomInitialShares: boolean;
  orderIntervalMs: number | "";
  minQuantity: number | "";
  maxQuantity: number | "";
  priceJitter: number | "";
  marketOrderPct: number | "";
  maxPosition: number | "";
  buyBias: number | "";
};

function emptyForm(): FormState {
  return {
    name: "",
    type: TraderType.MM,
    mm: {
      symbol: "",
      accountId: "",
      initialDeposit: 10_000_000,
      initialShares: 10_000,
      spread: 3.0,
      quantity: 20,
      levels: 3,
      levelSpacing: 1.0,
      maxPosition: 30_000,
      requoteIntervalMs: 30_000,
      priceMoveThreshold: 2.0,
      maxSkew: 1.0,
    },
    noise: {
      symbol: "",
      accountId: "",
      initialDeposit: 500_000,
      initialShares: 500,
      randomInitialShares: true,
      orderIntervalMs: 3_000,
      minQuantity: 1,
      maxQuantity: 10,
      priceJitter: 20.0,
      marketOrderPct: 0.5,
      maxPosition: 1000,
      buyBias: 0.5,
    },
  };
}

function formFromTrader(t: Trader): FormState {
  const f = emptyForm();
  f.id = t.id;
  f.name = t.name;
  f.type = t.type;
  if (t.config?.config.case === "mm") {
    const c = t.config.config.value;
    f.mm = mmFromProto(c);
  } else if (t.config?.config.case === "noise") {
    const c = t.config.config.value;
    f.noise = noiseFromProto(c);
  }
  return f;
}

function mmFromProto(c: MMConfig): MMFormFields {
  return {
    symbol: c.symbol,
    accountId: c.accountId,
    initialDeposit: priceToNumber(c.initialDeposit),
    initialShares: Number(c.initialShares),
    spread: priceToNumber(c.spread),
    quantity: Number(c.quantity),
    levels: c.levels,
    levelSpacing: priceToNumber(c.levelSpacing),
    maxPosition: Number(c.maxPosition),
    requoteIntervalMs: Number(c.requoteIntervalMs),
    priceMoveThreshold: priceToNumber(c.priceMoveThreshold),
    maxSkew: priceToNumber(c.maxSkew),
  };
}

function noiseFromProto(c: NoiseConfig): NoiseFormFields {
  return {
    symbol: c.symbol,
    accountId: c.accountId,
    initialDeposit: priceToNumber(c.initialDeposit),
    initialShares: Number(c.initialShares),
    randomInitialShares: c.randomInitialShares,
    orderIntervalMs: Number(c.orderIntervalMs),
    minQuantity: Number(c.minQuantity),
    maxQuantity: Number(c.maxQuantity),
    priceJitter: priceToNumber(c.priceJitter),
    marketOrderPct: c.marketOrderPct,
    maxPosition: Number(c.maxPosition),
    buyBias: c.buyBias,
  };
}

// num pulls a NumberInput value into a number, treating "" as 0. The form
// types let users blank-out a field; the engine config validators handle
// the actual "is this positive?" check.
function num(v: number | ""): number {
  return v === "" ? 0 : Number(v);
}

function buildConfig(f: FormState): TraderConfig {
  if (f.type === TraderType.MM) {
    const mm: MMConfig = {
      $typeName: "trader.v1.MMConfig",
      symbol: f.mm.symbol.trim(),
      accountId: f.mm.accountId.trim(),
      initialDeposit: moneyToPrice(num(f.mm.initialDeposit)),
      initialShares: BigInt(num(f.mm.initialShares)),
      spread: moneyToPrice(num(f.mm.spread)),
      quantity: BigInt(num(f.mm.quantity)),
      levels: num(f.mm.levels),
      levelSpacing: moneyToPrice(num(f.mm.levelSpacing)),
      maxPosition: BigInt(num(f.mm.maxPosition)),
      requoteIntervalMs: BigInt(num(f.mm.requoteIntervalMs)),
      priceMoveThreshold: moneyToPrice(num(f.mm.priceMoveThreshold)),
      maxSkew: moneyToPrice(num(f.mm.maxSkew)),
    };
    return {
      $typeName: "trader.v1.TraderConfig",
      config: { case: "mm", value: mm },
    };
  }
  const noise: NoiseConfig = {
    $typeName: "trader.v1.NoiseConfig",
    symbol: f.noise.symbol.trim(),
    accountId: f.noise.accountId.trim(),
    initialDeposit: moneyToPrice(num(f.noise.initialDeposit)),
    initialShares: BigInt(num(f.noise.initialShares)),
    randomInitialShares: f.noise.randomInitialShares,
    orderIntervalMs: BigInt(num(f.noise.orderIntervalMs)),
    minQuantity: BigInt(num(f.noise.minQuantity)),
    maxQuantity: BigInt(num(f.noise.maxQuantity)),
    priceJitter: moneyToPrice(num(f.noise.priceJitter)),
    marketOrderPct: num(f.noise.marketOrderPct),
    maxPosition: BigInt(num(f.noise.maxPosition)),
    buyBias: num(f.noise.buyBias),
  };
  return {
    $typeName: "trader.v1.TraderConfig",
    config: { case: "noise", value: noise },
  };
}

function typeLabel(t: TraderType): string {
  switch (t) {
    case TraderType.MM:
      return "mm";
    case TraderType.NOISE:
      return "noise";
    default:
      return "unknown";
  }
}

function statusBadge(t: Trader) {
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

function symbolOf(t: Trader): string {
  if (t.config?.config.case === "mm") return t.config.config.value.symbol;
  if (t.config?.config.case === "noise") return t.config.config.value.symbol;
  return "";
}

function accountOf(t: Trader): string {
  if (t.config?.config.case === "mm") return t.config.config.value.accountId;
  if (t.config?.config.case === "noise") return t.config.config.value.accountId;
  return "";
}

function depositOf(t: Trader): bigint {
  if (t.config?.config.case === "mm") return t.config.config.value.initialDeposit;
  if (t.config?.config.case === "noise") return t.config.config.value.initialDeposit;
  return 0n;
}

export function TradersPanel() {
  const [traders, setTraders] = useState<Trader[]>([]);
  const [loading, setLoading] = useState(false);
  const [form, setForm] = useState<FormState>(emptyForm());
  const [editing, setEditing] = useState(false);
  const [modalOpened, modalHandlers] = useDisclosure(false);
  const [saving, setSaving] = useState(false);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState<Trader | null>(null);
  const [deleteOpened, deleteHandlers] = useDisclosure(false);

  async function load() {
    setLoading(true);
    try {
      const r = await traderClient.listTraders({});
      setTraders(r.traders);
    } catch (e: unknown) {
      notifications.show({
        title: "Failed to load traders",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // Poll while the panel is open so RUNNING/FAILED transitions surface
    // without a manual refresh.
    const id = window.setInterval(load, 3000);
    return () => window.clearInterval(id);
  }, []);

  function openCreate() {
    setForm(emptyForm());
    setEditing(false);
    modalHandlers.open();
  }

  function openEdit(t: Trader) {
    setForm(formFromTrader(t));
    setEditing(true);
    modalHandlers.open();
  }

  async function save(startNow: boolean) {
    if (!form.name.trim()) {
      notifications.show({ title: "Name is required", message: "", color: "red" });
      return;
    }
    setSaving(true);
    try {
      const config = buildConfig(form);
      if (editing && form.id) {
        await traderClient.updateTrader({
          id: form.id,
          name: form.name.trim(),
          config,
        });
      } else {
        await traderClient.createTrader({
          name: form.name.trim(),
          type: form.type,
          config,
          start: startNow,
        });
      }
      modalHandlers.close();
      await load();
    } catch (e: unknown) {
      notifications.show({
        title: editing ? "Update failed" : "Create failed",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setSaving(false);
    }
  }

  async function toggleRun(t: Trader) {
    setBusyId(t.id);
    try {
      if (t.status === TraderStatus.RUNNING) {
        await traderClient.stopTrader({ id: t.id });
      } else {
        await traderClient.startTrader({ id: t.id });
      }
      await load();
    } catch (e: unknown) {
      notifications.show({
        title: "Action failed",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setBusyId(null);
    }
  }

  function askDelete(t: Trader) {
    setConfirmDelete(t);
    deleteHandlers.open();
  }

  async function doDelete() {
    if (!confirmDelete) return;
    setBusyId(confirmDelete.id);
    try {
      await traderClient.deleteTrader({ id: confirmDelete.id });
      deleteHandlers.close();
      setConfirmDelete(null);
      await load();
    } catch (e: unknown) {
      notifications.show({
        title: "Delete failed",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setBusyId(null);
    }
  }

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Title order={3}>Traders</Title>
        <Group gap="xs">
          <Button size="xs" variant="default" onClick={load}>
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
            No traders yet. Create one to start posting orders against the book.
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
                <Table.Th style={{ width: 240 }}>Actions</Table.Th>
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
                  <Table.Td>{symbolOf(t)}</Table.Td>
                  <Table.Td>
                    <Text size="xs" ff="monospace">
                      {accountOf(t)}
                    </Text>
                  </Table.Td>
                  <Table.Td>{formatMoney(depositOf(t))}</Table.Td>
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
            This will stop and remove <b>{confirmDelete?.name}</b>. The portfolio
            and any resting orders are not deleted.
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
            <Button color="red" onClick={doDelete} loading={!!busyId}>
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
  const set = <K extends keyof NoiseFormFields>(k: K, v: NoiseFormFields[K]) =>
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
    </Stack>
  );
}
