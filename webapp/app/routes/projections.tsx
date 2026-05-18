import { useEffect, useRef, useState } from "react";
import {
  Badge,
  Box,
  Button,
  Group,
  LoadingOverlay,
  Modal,
  Progress,
  Stack,
  Table,
  Text,
  Title,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { notifications } from "@mantine/notifications";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { useFetcher, useRevalidator } from "react-router";
import type { Route } from "./+types/projections";
import { diagnosticsClient as serverClient } from "~/lib/client.server";
import { diagnosticsClient } from "~/lib/client";
import {
  ProjectionPhase,
  type ProjectionProgress,
} from "../../src/gen/diagnostics/v1/service_pb";

type ProjectionRow = {
  name: string;
  phase: ProjectionPhase;
  checkpoint: bigint;
  headSequence: bigint;
  lag: bigint;
  rebuildable: boolean;
  reasonNotRebuildable: string;
  rebuildStartedAtIso: string | null;
  rebuildLastError: string;
  resettableCount: number;
};

type LiveProgress = {
  position: bigint;
  head: bigint;
  eventsPerSec: number;
  etaSeconds: bigint;
};

export async function loader() {
  const r = await serverClient.listProjections({});
  const projections: ProjectionRow[] = r.projections.map((p) => ({
    name: p.name,
    phase: p.phase,
    checkpoint: p.checkpoint,
    headSequence: p.headSequence,
    lag: p.lag,
    rebuildable: p.rebuildable,
    reasonNotRebuildable: p.reasonNotRebuildable,
    rebuildStartedAtIso: p.rebuildStartedAt
      ? timestampDate(p.rebuildStartedAt).toISOString()
      : null,
    rebuildLastError: p.rebuildLastError,
    resettableCount: p.resettableCount,
  }));
  return { projections };
}

export async function action({ request }: Route.ActionArgs) {
  const form = await request.formData();
  const name = String(form.get("name") ?? "");
  if (!name) return { ok: false as const, name, error: "missing name" };
  try {
    await serverClient.rebuildProjection({ name });
    return { ok: true as const, name };
  } catch (e: unknown) {
    return {
      ok: false as const,
      name,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

function phaseColor(phase: ProjectionPhase): string {
  switch (phase) {
    case ProjectionPhase.RUNNING:
      return "green";
    case ProjectionPhase.REBUILDING:
      return "blue";
    case ProjectionPhase.STOPPED:
      return "gray";
    case ProjectionPhase.FAILED:
      return "red";
    default:
      return "gray";
  }
}

function phaseLabel(phase: ProjectionPhase): string {
  switch (phase) {
    case ProjectionPhase.RUNNING:
      return "running";
    case ProjectionPhase.REBUILDING:
      return "rebuilding";
    case ProjectionPhase.STOPPED:
      return "stopped";
    case ProjectionPhase.FAILED:
      return "failed";
    default:
      return "unknown";
  }
}

function formatSeq(n: bigint): string {
  return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function formatEta(s: bigint): string {
  if (s <= 0n) return "—";
  const n = Number(s);
  if (n < 60) return `${n}s`;
  const m = Math.floor(n / 60);
  const rem = n % 60;
  return `${m}m ${rem}s`;
}

export default function Projections({ loaderData }: Route.ComponentProps) {
  const { projections } = loaderData;
  const revalidator = useRevalidator();
  const rebuildFetcher = useFetcher<typeof action>();

  const [live, setLive] = useState<Record<string, LiveProgress>>({});
  const [confirmTarget, setConfirmTarget] = useState<ProjectionRow | null>(
    null,
  );
  const [confirmOpened, confirmHandlers] = useDisclosure(false);
  const streamsRef = useRef<Record<string, AbortController>>({});

  // Periodic refresh of loader data, replacing the panel's polling
  // useEffect. Skips while a revalidation is already in flight.
  useEffect(() => {
    const id = window.setInterval(() => {
      if (revalidator.state === "idle") revalidator.revalidate();
    }, 3000);
    return () => {
      window.clearInterval(id);
      // Cancel any in-flight progress streams on unmount.
      Object.values(streamsRef.current).forEach((c) => c.abort());
      streamsRef.current = {};
    };
  }, [revalidator]);

  // Start a fresh progress stream after a successful rebuild action.
  useEffect(() => {
    if (rebuildFetcher.state !== "idle" || !rebuildFetcher.data) return;
    const data = rebuildFetcher.data;
    if (data.ok) {
      const name = data.name;
      const target =
        confirmTarget?.name === name
          ? confirmTarget
          : projections.find((p) => p.name === name) ?? null;
      setLive((prev) => ({
        ...prev,
        [name]: {
          position: 0n,
          head: target?.headSequence ?? 0n,
          eventsPerSec: 0,
          etaSeconds: 0n,
        },
      }));
      confirmHandlers.close();
      setConfirmTarget(null);
      streamProgress(name);
    } else if (data.error) {
      notifications.show({
        title: `Rebuild request rejected (${data.name})`,
        message: data.error,
        color: "red",
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rebuildFetcher.state, rebuildFetcher.data]);

  async function streamProgress(name: string) {
    streamsRef.current[name]?.abort();
    const controller = new AbortController();
    streamsRef.current[name] = controller;

    try {
      const stream = diagnosticsClient.streamProjectionProgress(
        { name },
        { signal: controller.signal },
      );
      for await (const tick of stream) {
        applyTick(tick);
        if (
          tick.phase === ProjectionPhase.RUNNING ||
          tick.phase === ProjectionPhase.FAILED
        ) {
          break;
        }
      }
    } catch (e: unknown) {
      if (controller.signal.aborted) return;
      notifications.show({
        title: `Progress stream failed (${name})`,
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      delete streamsRef.current[name];
      setLive((prev) => {
        const next = { ...prev };
        delete next[name];
        return next;
      });
      revalidator.revalidate();
    }
  }

  function applyTick(tick: ProjectionProgress) {
    if (tick.phase === ProjectionPhase.REBUILDING) {
      setLive((prev) => ({
        ...prev,
        [tick.name]: {
          position: tick.position,
          head: tick.headSequence,
          eventsPerSec: tick.eventsPerSec,
          etaSeconds: tick.etaSeconds,
        },
      }));
    }
    if (tick.phase === ProjectionPhase.FAILED && tick.error) {
      notifications.show({
        title: `Rebuild failed (${tick.name})`,
        message: tick.error,
        color: "red",
      });
    }
    if (tick.phase === ProjectionPhase.RUNNING) {
      notifications.show({
        title: `Rebuild complete (${tick.name})`,
        message: `Replayed up to sequence ${formatSeq(tick.position)}`,
        color: "green",
      });
    }
  }

  function openConfirm(p: ProjectionRow) {
    setConfirmTarget(p);
    confirmHandlers.open();
  }

  function executeRebuild() {
    if (!confirmTarget) return;
    const fd = new FormData();
    fd.set("name", confirmTarget.name);
    rebuildFetcher.submit(fd, { method: "post" });
  }

  const loading = revalidator.state === "loading";
  const rebuildPending = rebuildFetcher.state !== "idle";

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Title order={4}>Projections</Title>
        <Button
          size="xs"
          variant="default"
          onClick={() => revalidator.revalidate()}
          loading={loading}
        >
          Refresh
        </Button>
      </Group>

      <Text size="sm" c="dimmed">
        Rebuilding a projection truncates its read-side tables and replays
        every event from the start of the stream. Reactor consumers are
        hidden from rebuild — replaying them would re-issue commands.
      </Text>

      <Box pos="relative">
        <LoadingOverlay
          visible={loading && projections.length === 0}
          zIndex={2}
          overlayProps={{ blur: 1 }}
        />
        <Table highlightOnHover striped>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Name</Table.Th>
              <Table.Th>Phase</Table.Th>
              <Table.Th ta="right">Checkpoint</Table.Th>
              <Table.Th ta="right">Head</Table.Th>
              <Table.Th ta="right">Lag</Table.Th>
              <Table.Th>Progress</Table.Th>
              <Table.Th></Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {projections.map((p) => (
              <Row
                key={p.name}
                projection={p}
                live={live[p.name]}
                onRebuild={() => openConfirm(p)}
              />
            ))}
          </Table.Tbody>
        </Table>
      </Box>

      <Modal
        opened={confirmOpened}
        onClose={() => {
          confirmHandlers.close();
          setConfirmTarget(null);
        }}
        title={`Rebuild ${confirmTarget?.name ?? ""}?`}
      >
        {confirmTarget && (
          <Stack gap="sm">
            <Text size="sm">This will:</Text>
            <ul style={{ marginTop: 0, paddingLeft: 20 }}>
              <li>
                <Text size="sm">
                  Truncate {confirmTarget.resettableCount} projection
                  {confirmTarget.resettableCount === 1 ? "" : "s"} owned by
                  this consumer
                </Text>
              </li>
              <li>
                <Text size="sm">
                  Replay {formatSeq(confirmTarget.headSequence)} event
                  {confirmTarget.headSequence === 1n ? "" : "s"} from
                  sequence 1
                </Text>
              </li>
              <li>
                <Text size="sm">
                  Block reads of this consumer's tables briefly while the
                  rebuild runs
                </Text>
              </li>
            </ul>
            <Group justify="flex-end" gap="sm">
              <Button
                variant="default"
                onClick={() => {
                  confirmHandlers.close();
                  setConfirmTarget(null);
                }}
              >
                Cancel
              </Button>
              <Button
                color="red"
                onClick={executeRebuild}
                loading={rebuildPending}
              >
                Rebuild
              </Button>
            </Group>
          </Stack>
        )}
      </Modal>
    </Stack>
  );
}

function Row({
  projection,
  live,
  onRebuild,
}: {
  projection: ProjectionRow;
  live: LiveProgress | undefined;
  onRebuild: () => void;
}) {
  const rebuilding =
    !!live || projection.phase === ProjectionPhase.REBUILDING;
  const head = live?.head ?? projection.headSequence;
  const pos = live?.position ?? projection.checkpoint;
  const pct =
    head > 0n ? Math.min(100, Number((pos * 100n) / head)) : 0;
  const startedAt = projection.rebuildStartedAtIso
    ? new Date(projection.rebuildStartedAtIso).toLocaleString()
    : null;

  return (
    <Table.Tr>
      <Table.Td>
        <Text size="sm" ff="monospace">
          {projection.name}
        </Text>
        {projection.rebuildLastError && (
          <Text size="xs" c="red" title={projection.rebuildLastError}>
            last error: {projection.rebuildLastError}
          </Text>
        )}
      </Table.Td>
      <Table.Td>
        <Badge size="sm" variant="light" color={phaseColor(projection.phase)}>
          {phaseLabel(projection.phase)}
        </Badge>
      </Table.Td>
      <Table.Td ta="right">
        <Text size="xs" ff="monospace">
          {formatSeq(pos)}
        </Text>
      </Table.Td>
      <Table.Td ta="right">
        <Text size="xs" ff="monospace">
          {formatSeq(head)}
        </Text>
      </Table.Td>
      <Table.Td ta="right">
        <Text
          size="xs"
          ff="monospace"
          c={projection.lag > 0n ? "orange" : undefined}
        >
          {formatSeq(projection.lag)}
        </Text>
      </Table.Td>
      <Table.Td style={{ minWidth: 200 }}>
        {rebuilding ? (
          <Stack gap={2}>
            <Progress value={pct} size="sm" striped animated />
            <Group gap="sm" justify="space-between">
              <Text size="xs" c="dimmed">
                {pct}% · {live ? live.eventsPerSec.toFixed(0) : "0"} ev/s
              </Text>
              <Text size="xs" c="dimmed">
                ETA {live ? formatEta(live.etaSeconds) : "—"}
              </Text>
            </Group>
          </Stack>
        ) : startedAt ? (
          <Text size="xs" c="dimmed">
            last rebuild: {startedAt}
          </Text>
        ) : (
          <Text size="xs" c="dimmed">
            —
          </Text>
        )}
      </Table.Td>
      <Table.Td ta="right">
        {projection.rebuildable ? (
          <Button
            size="xs"
            variant="light"
            color="red"
            onClick={onRebuild}
            disabled={rebuilding}
          >
            Rebuild
          </Button>
        ) : (
          <Text
            size="xs"
            c="dimmed"
            title={projection.reasonNotRebuildable}
            style={{ cursor: "help" }}
          >
            n/a
          </Text>
        )}
      </Table.Td>
    </Table.Tr>
  );
}
