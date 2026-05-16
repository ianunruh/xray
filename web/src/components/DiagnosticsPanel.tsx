import { useEffect, useMemo, useState } from "react";
import {
  ActionIcon,
  Badge,
  Box,
  Button,
  Code,
  Group,
  ScrollArea,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { diagnosticsClient } from "../client";
import type {
  AggregateSummary,
  DiagnosticEvent,
} from "../gen/diagnostics/v1/service_pb";

type SortField = "version" | "timestamp" | "type";
type SortDir = "asc" | "desc";

function formatTimestamp(ts: DiagnosticEvent["timestamp"] | undefined): string {
  if (!ts) return "";
  return timestampDate(ts).toISOString();
}

function prettyJson(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
}

export function DiagnosticsPanel({
  initialAggregateId = "",
  onAggregateChange,
}: {
  initialAggregateId?: string;
  onAggregateChange?: (id: string) => void;
}) {
  const [filter, setFilter] = useState("");
  const [aggregates, setAggregates] = useState<AggregateSummary[]>([]);
  const [aggregatesLoading, setAggregatesLoading] = useState(false);
  const [selected, setSelected] = useState<string>(initialAggregateId);
  const [events, setEvents] = useState<DiagnosticEvent[]>([]);
  const [eventsLoading, setEventsLoading] = useState(false);
  const [typeFilter, setTypeFilter] = useState<string | null>(null);
  const [textFilter, setTextFilter] = useState("");
  const [sortField, setSortField] = useState<SortField>("version");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  async function loadAggregates() {
    setAggregatesLoading(true);
    try {
      const r = await diagnosticsClient.listAggregates({ filter });
      setAggregates(r.aggregates);
    } catch (e: unknown) {
      notifications.show({
        title: "Failed to load aggregates",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setAggregatesLoading(false);
    }
  }

  async function loadEvents(aggregateId: string) {
    if (!aggregateId) {
      setEvents([]);
      return;
    }
    setEventsLoading(true);
    try {
      const r = await diagnosticsClient.getAggregateEvents({ aggregateId });
      setEvents(r.events);
      setExpanded({});
    } catch (e: unknown) {
      notifications.show({
        title: "Failed to load events",
        message: e instanceof Error ? e.message : String(e),
        color: "red",
      });
    } finally {
      setEventsLoading(false);
    }
  }

  useEffect(() => {
    loadAggregates();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (selected) {
      loadEvents(selected);
    } else {
      setEvents([]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected]);

  const types = useMemo(() => {
    const set = new Set<string>();
    for (const e of events) set.add(e.type);
    return Array.from(set).sort();
  }, [events]);

  const visibleEvents = useMemo(() => {
    let filtered = events;
    if (typeFilter) {
      filtered = filtered.filter((e) => e.type === typeFilter);
    }
    if (textFilter.trim()) {
      const needle = textFilter.toLowerCase();
      filtered = filtered.filter(
        (e) =>
          e.type.toLowerCase().includes(needle) ||
          e.dataJson.toLowerCase().includes(needle) ||
          e.id.toLowerCase().includes(needle),
      );
    }
    const sorted = [...filtered];
    sorted.sort((a, b) => {
      let cmp = 0;
      switch (sortField) {
        case "version":
          cmp = a.version - b.version;
          break;
        case "timestamp":
          cmp =
            (a.timestamp ? Number(a.timestamp.seconds) : 0) -
            (b.timestamp ? Number(b.timestamp.seconds) : 0);
          if (cmp === 0) {
            cmp =
              (a.timestamp?.nanos ?? 0) - (b.timestamp?.nanos ?? 0);
          }
          break;
        case "type":
          cmp = a.type.localeCompare(b.type);
          break;
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
    return sorted;
  }, [events, typeFilter, textFilter, sortField, sortDir]);

  function toggleSort(field: SortField) {
    if (sortField === field) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDir("asc");
    }
  }

  function sortIndicator(field: SortField): string {
    if (sortField !== field) return "";
    return sortDir === "asc" ? " ▲" : " ▼";
  }

  function selectAggregate(id: string) {
    setSelected(id);
    onAggregateChange?.(id);
  }

  function toggleExpanded(id: string) {
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));
  }

  return (
    <Stack gap="md">
      <Title order={4}>Diagnostics</Title>

      <Group align="end" gap="sm">
        <TextInput
          label="Aggregate filter"
          placeholder="e.g. orderbook or portfolio:my-account"
          value={filter}
          onChange={(e) => setFilter(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") loadAggregates();
          }}
          style={{ flexGrow: 1 }}
        />
        <Button onClick={loadAggregates} loading={aggregatesLoading}>
          Search
        </Button>
        <TextInput
          label="Aggregate ID"
          placeholder="type:id"
          value={selected}
          onChange={(e) => selectAggregate(e.currentTarget.value)}
          style={{ flexGrow: 1 }}
        />
        <Button
          variant="default"
          onClick={() => loadEvents(selected)}
          loading={eventsLoading}
          disabled={!selected}
        >
          Load Events
        </Button>
      </Group>

      <Group align="flex-start" gap="md" wrap="nowrap">
        <Stack gap="xs" w={440}>
          <Text size="sm" c="dimmed">
            Aggregates ({aggregates.length})
          </Text>
          <ScrollArea h={500}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>ID</Table.Th>
                  <Table.Th ta="right">#</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {aggregates.map((a) => (
                  <Table.Tr
                    key={a.aggregateId}
                    onClick={() => selectAggregate(a.aggregateId)}
                    style={{ cursor: "pointer" }}
                    bg={
                      selected === a.aggregateId
                        ? "var(--mantine-color-blue-light)"
                        : undefined
                    }
                  >
                    <Table.Td>
                      <Stack gap={2}>
                        <Group gap={6} wrap="nowrap">
                          <Badge
                            size="sm"
                            variant="light"
                            color="grape"
                            style={{ flexShrink: 0 }}
                          >
                            {a.type}
                          </Badge>
                          <Text size="xs" ff="monospace" truncate>
                            {a.aggregateId.slice(a.type.length + 1)}
                          </Text>
                        </Group>
                        <Text size="xs" c="dimmed">
                          {a.lastEventAt
                            ? timestampDate(a.lastEventAt).toLocaleString()
                            : ""}
                        </Text>
                      </Stack>
                    </Table.Td>
                    <Table.Td ta="right">
                      <Text size="xs" ff="monospace" fw={600}>
                        {a.eventCount.toString()}
                      </Text>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </ScrollArea>
        </Stack>

        <Stack gap="xs" style={{ flexGrow: 1, minWidth: 0 }}>
          <Group gap="sm">
            <Select
              label="Type"
              placeholder="all"
              data={types}
              value={typeFilter}
              onChange={setTypeFilter}
              clearable
              searchable
              size="xs"
            />
            <TextInput
              label="Search"
              placeholder="filter by id, type, or JSON"
              value={textFilter}
              onChange={(e) => setTextFilter(e.currentTarget.value)}
              size="xs"
              style={{ flexGrow: 1 }}
            />
            <Text size="xs" c="dimmed" mt={22}>
              {visibleEvents.length} / {events.length} events
            </Text>
          </Group>

          <ScrollArea h={500}>
            <Table highlightOnHover striped>
              <Table.Thead
                style={{
                  position: "sticky",
                  top: 0,
                  background: "var(--mantine-color-body)",
                  zIndex: 1,
                }}
              >
                <Table.Tr>
                  <Table.Th
                    onClick={() => toggleSort("version")}
                    style={{ cursor: "pointer" }}
                  >
                    Ver{sortIndicator("version")}
                  </Table.Th>
                  <Table.Th
                    onClick={() => toggleSort("type")}
                    style={{ cursor: "pointer" }}
                  >
                    Type{sortIndicator("type")}
                  </Table.Th>
                  <Table.Th
                    onClick={() => toggleSort("timestamp")}
                    style={{ cursor: "pointer" }}
                  >
                    Timestamp{sortIndicator("timestamp")}
                  </Table.Th>
                  <Table.Th>Position</Table.Th>
                  <Table.Th></Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {visibleEvents.map((e) => (
                  <RowGroup
                    key={e.id || `${e.aggregateId}-${e.version}`}
                    event={e}
                    expanded={!!expanded[e.id || `${e.aggregateId}-${e.version}`]}
                    onToggle={() =>
                      toggleExpanded(e.id || `${e.aggregateId}-${e.version}`)
                    }
                  />
                ))}
              </Table.Tbody>
            </Table>
          </ScrollArea>
        </Stack>
      </Group>
    </Stack>
  );
}

function RowGroup({
  event,
  expanded,
  onToggle,
}: {
  event: DiagnosticEvent;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <>
      <Table.Tr style={{ cursor: "pointer" }} onClick={onToggle}>
        <Table.Td>
          <Text size="xs" ff="monospace">
            {event.version}
          </Text>
        </Table.Td>
        <Table.Td>
          <Text size="xs" ff="monospace">
            {event.type}
          </Text>
        </Table.Td>
        <Table.Td>
          <Text size="xs" ff="monospace">
            {formatTimestamp(event.timestamp)}
          </Text>
        </Table.Td>
        <Table.Td>
          <Text size="xs" ff="monospace" c="dimmed">
            {event.position.toString()}
          </Text>
        </Table.Td>
        <Table.Td>
          <ActionIcon
            variant="subtle"
            size="xs"
            onClick={(e) => {
              e.stopPropagation();
              onToggle();
            }}
            aria-label={expanded ? "Collapse" : "Expand"}
          >
            {expanded ? "−" : "+"}
          </ActionIcon>
        </Table.Td>
      </Table.Tr>
      {expanded && (
        <Table.Tr>
          <Table.Td colSpan={5}>
            <Box p="xs">
              <Group gap="xs" mb="xs">
                <Text size="xs" c="dimmed">
                  id:
                </Text>
                <Code>{event.id}</Code>
              </Group>
              <Code block style={{ whiteSpace: "pre", fontSize: 12 }}>
                {prettyJson(event.dataJson)}
              </Code>
            </Box>
          </Table.Td>
        </Table.Tr>
      )}
    </>
  );
}
