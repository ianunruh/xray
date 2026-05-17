import { useEffect, useMemo, useState } from "react";
import {
  ActionIcon,
  Badge,
  Box,
  Button,
  Code,
  Group,
  ScrollArea,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { diagnosticsClient } from "../client";
import type { DiagnosticEvent } from "../gen/diagnostics/v1/service_pb";

function formatTimestamp(ts: DiagnosticEvent["timestamp"] | undefined): string {
  if (!ts) return "";
  const d = timestampDate(ts);
  // Show HH:MM:SS.mmm — chain events are clustered tightly in time, so the
  // sub-second precision matters more than the date.
  return d.toISOString().slice(11, 23);
}

function prettyJson(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
}

function tsKey(e: DiagnosticEvent): number {
  if (!e.timestamp) return 0;
  return Number(e.timestamp.seconds) * 1e9 + e.timestamp.nanos;
}

function aggregateType(aggregateId: string): string {
  const idx = aggregateId.indexOf(":");
  return idx > 0 ? aggregateId.slice(0, idx) : aggregateId;
}

function aggregateName(aggregateId: string): string {
  const idx = aggregateId.indexOf(":");
  return idx > 0 ? aggregateId.slice(idx + 1) : "";
}

const TYPE_COLORS: Record<string, string> = {
  orderbook: "blue",
  portfolio: "teal",
  "ordersaga": "orange",
  "ocosaga": "red",
  "bracket": "violet",
};

function typeColor(t: string): string {
  return TYPE_COLORS[t] ?? "gray";
}

interface ChainTree {
  roots: DiagnosticEvent[];
  childMap: Map<string, DiagnosticEvent[]>;
}

function buildTree(events: DiagnosticEvent[]): ChainTree {
  const allIds = new Set<string>();
  for (const e of events) allIds.add(e.id);

  const childMap = new Map<string, DiagnosticEvent[]>();
  const roots: DiagnosticEvent[] = [];
  for (const e of events) {
    if (!e.causationId || !allIds.has(e.causationId)) {
      roots.push(e);
    } else {
      const arr = childMap.get(e.causationId) ?? [];
      arr.push(e);
      childMap.set(e.causationId, arr);
    }
  }
  roots.sort((a, b) => tsKey(a) - tsKey(b));
  for (const arr of childMap.values()) arr.sort((a, b) => tsKey(a) - tsKey(b));
  return { roots, childMap };
}

export function ChainPanel({
  initialCorrelationId = "",
  onCorrelationChange,
}: {
  initialCorrelationId?: string;
  onCorrelationChange?: (id: string) => void;
}) {
  const [input, setInput] = useState(initialCorrelationId);
  const [correlation, setCorrelation] = useState(initialCorrelationId);
  const [events, setEvents] = useState<DiagnosticEvent[]>([]);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  async function load(id: string) {
    if (!id) {
      setEvents([]);
      return;
    }
    setLoading(true);
    try {
      const r = await diagnosticsClient.getEventChain({ correlationId: id });
      setEvents(r.events);
      setExpanded({});
    } catch (e: unknown) {
      notifications.show({
        title: "Failed to load chain",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
      setEvents([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (initialCorrelationId) {
      load(initialCorrelationId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function go() {
    const id = input.trim();
    setCorrelation(id);
    onCorrelationChange?.(id);
    load(id);
  }

  const tree = useMemo(() => buildTree(events), [events]);

  function toggle(id: string) {
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));
  }

  return (
    <Stack gap="md">
      <Title order={4}>Causal Chain</Title>

      <Group align="end" gap="sm">
        <TextInput
          label="Correlation ID"
          placeholder="paste a correlation_id (or jump from Diagnostics)"
          value={input}
          onChange={(e) => setInput(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") go();
          }}
          style={{ flexGrow: 1 }}
        />
        <Button onClick={go} loading={loading} disabled={!input.trim()}>
          Load Chain
        </Button>
      </Group>

      {correlation && !loading && events.length === 0 && (
        <Text size="sm" c="dimmed">
          No events for correlation_id {correlation}
        </Text>
      )}

      {events.length > 0 && (
        <>
          <Text size="xs" c="dimmed">
            {events.length} events · {tree.roots.length}{" "}
            {tree.roots.length === 1 ? "root" : "roots"}
          </Text>
          <ScrollArea h="calc(100vh - 220px)">
            <Stack gap={2}>
              {tree.roots.map((root) => (
                <ChainNode
                  key={root.id}
                  event={root}
                  depth={0}
                  childMap={tree.childMap}
                  expanded={expanded}
                  onToggle={toggle}
                />
              ))}
            </Stack>
          </ScrollArea>
        </>
      )}
    </Stack>
  );
}

function ChainNode({
  event,
  depth,
  childMap,
  expanded,
  onToggle,
}: {
  event: DiagnosticEvent;
  depth: number;
  childMap: Map<string, DiagnosticEvent[]>;
  expanded: Record<string, boolean>;
  onToggle: (id: string) => void;
}) {
  const children = childMap.get(event.id) ?? [];
  const isExpanded = !!expanded[event.id];
  const aggType = aggregateType(event.aggregateId);
  const aggName = aggregateName(event.aggregateId);

  return (
    <>
      <Box
        style={{
          marginLeft: depth * 20,
          padding: "4px 8px",
          borderLeft: depth > 0 ? "2px solid var(--mantine-color-gray-3)" : undefined,
          cursor: "pointer",
        }}
        onClick={() => onToggle(event.id)}
      >
        <Group gap="xs" wrap="nowrap">
          <Badge size="sm" variant="light" color={typeColor(aggType)} style={{ flexShrink: 0 }}>
            {aggType}
          </Badge>
          <Text size="xs" ff="monospace" c="dimmed" style={{ flexShrink: 0 }}>
            {aggName}
          </Text>
          <Text size="xs" ff="monospace" fw={600}>
            {event.type}
          </Text>
          <Text size="xs" ff="monospace" c="dimmed" style={{ marginLeft: "auto", flexShrink: 0 }}>
            {formatTimestamp(event.timestamp)}
          </Text>
          <ActionIcon
            variant="subtle"
            size="xs"
            onClick={(e) => {
              e.stopPropagation();
              onToggle(event.id);
            }}
            aria-label={isExpanded ? "Collapse" : "Expand"}
          >
            {isExpanded ? "−" : "+"}
          </ActionIcon>
        </Group>
      </Box>
      {isExpanded && (
        <Box
          style={{
            marginLeft: depth * 20 + 16,
            padding: "4px 8px 8px",
            borderLeft: "2px solid var(--mantine-color-gray-3)",
          }}
        >
          <Group gap="xs" mb={4} wrap="wrap">
            <Text size="xs" c="dimmed">id:</Text>
            <Code>{event.id}</Code>
            {event.causationId && (
              <>
                <Text size="xs" c="dimmed">caused by:</Text>
                <Code>{event.causationId}</Code>
              </>
            )}
          </Group>
          <Code block style={{ whiteSpace: "pre", fontSize: 11 }}>
            {prettyJson(event.dataJson)}
          </Code>
        </Box>
      )}
      {children.map((c) => (
        <ChainNode
          key={c.id}
          event={c}
          depth={depth + 1}
          childMap={childMap}
          expanded={expanded}
          onToggle={onToggle}
        />
      ))}
    </>
  );
}
