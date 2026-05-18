import { useMemo, useState } from "react";
import {
  ActionIcon,
  Badge,
  Box,
  Button,
  Code,
  Group,
  LoadingOverlay,
  ScrollArea,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { useNavigation, useSearchParams } from "react-router";
import type { Route } from "./+types/chain";
import { diagnosticsClient } from "~/lib/client.server";

type ChainEvent = {
  id: string;
  aggregateId: string;
  type: string;
  causationId: string;
  dataJson: string;
  timestampMs: number;
  timestampLabel: string;
};

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const correlation = url.searchParams.get("correlation") ?? "";
  if (!correlation) {
    return { correlation: "", events: [] as ChainEvent[] };
  }
  const r = await diagnosticsClient.getEventChain({
    correlationId: correlation,
  });
  const events: ChainEvent[] = r.events.map((e) => {
    const d = e.timestamp ? timestampDate(e.timestamp) : null;
    return {
      id: e.id,
      aggregateId: e.aggregateId,
      type: e.type,
      causationId: e.causationId,
      dataJson: e.dataJson,
      timestampMs: d ? d.getTime() : 0,
      // Chain events cluster tightly in time; sub-second precision matters
      // more than the date.
      timestampLabel: d ? d.toISOString().slice(11, 23) : "",
    };
  });
  return { correlation, events };
}

function prettyJson(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
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
  ordersaga: "orange",
  ocosaga: "red",
  bracket: "violet",
};

function typeColor(t: string): string {
  return TYPE_COLORS[t] ?? "gray";
}

type ChainTree = {
  roots: ChainEvent[];
  childMap: Map<string, ChainEvent[]>;
};

function buildTree(events: ChainEvent[]): ChainTree {
  const allIds = new Set<string>();
  for (const e of events) allIds.add(e.id);

  const childMap = new Map<string, ChainEvent[]>();
  const roots: ChainEvent[] = [];
  for (const e of events) {
    if (!e.causationId || !allIds.has(e.causationId)) {
      roots.push(e);
    } else {
      const arr = childMap.get(e.causationId) ?? [];
      arr.push(e);
      childMap.set(e.causationId, arr);
    }
  }
  roots.sort((a, b) => a.timestampMs - b.timestampMs);
  for (const arr of childMap.values()) {
    arr.sort((a, b) => a.timestampMs - b.timestampMs);
  }
  return { roots, childMap };
}

export default function Chain({ loaderData }: Route.ComponentProps) {
  const { correlation, events } = loaderData;
  const navigation = useNavigation();
  const [, setSearchParams] = useSearchParams();

  const [input, setInput] = useState(correlation);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  const loading =
    navigation.state === "loading" &&
    navigation.location?.pathname === "/chain";

  const tree = useMemo(() => buildTree(events), [events]);

  function go() {
    const id = input.trim();
    setExpanded({});
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (id) next.set("correlation", id);
      else next.delete("correlation");
      return next;
    });
  }

  function toggle(id: string) {
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));
  }

  return (
    <Stack gap="md">
      <Title order={4}>Causal Chain</Title>

      <Group align="end" gap="sm">
        <TextInput
          label="Correlation ID"
          placeholder="paste a correlation_id (or jump from Events)"
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

      {correlation && (
        <>
          <Text size="xs" c="dimmed">
            {events.length > 0
              ? `${events.length} events · ${tree.roots.length} ${tree.roots.length === 1 ? "root" : "roots"}`
              : loading
                ? "Loading…"
                : `No events for correlation_id ${correlation}`}
          </Text>
          <Box pos="relative" h="calc(100vh - 220px)">
            <LoadingOverlay
              visible={loading}
              zIndex={2}
              overlayProps={{ blur: 1 }}
            />
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
          </Box>
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
  event: ChainEvent;
  depth: number;
  childMap: Map<string, ChainEvent[]>;
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
          borderLeft:
            depth > 0 ? "2px solid var(--mantine-color-gray-3)" : undefined,
          cursor: "pointer",
        }}
        onClick={() => onToggle(event.id)}
      >
        <Group gap="xs" wrap="nowrap">
          <Badge
            size="sm"
            variant="light"
            color={typeColor(aggType)}
            style={{ flexShrink: 0 }}
          >
            {aggType}
          </Badge>
          <Text size="xs" ff="monospace" c="dimmed" style={{ flexShrink: 0 }}>
            {aggName}
          </Text>
          <Text size="xs" ff="monospace" fw={600}>
            {event.type}
          </Text>
          <Text
            size="xs"
            ff="monospace"
            c="dimmed"
            style={{ marginLeft: "auto", flexShrink: 0 }}
          >
            {event.timestampLabel}
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
            <Text size="xs" c="dimmed">
              id:
            </Text>
            <Code>{event.id}</Code>
            {event.causationId && (
              <>
                <Text size="xs" c="dimmed">
                  caused by:
                </Text>
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
