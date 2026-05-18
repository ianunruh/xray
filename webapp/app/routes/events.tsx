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
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { Link, useNavigation, useSearchParams } from "react-router";
import type { Route } from "./+types/events";
import { diagnosticsClient } from "~/lib/client.server";

type SortField = "version" | "timestamp" | "type";
type SortDir = "asc" | "desc";

type AggregateRow = {
  aggregateId: string;
  type: string;
  eventCount: number;
  lastEventAtIso: string | null;
};

type EventRow = {
  id: string;
  aggregateId: string;
  version: number;
  type: string;
  position: bigint;
  dataJson: string;
  causationId: string;
  correlationId: string;
  timestampMs: number;
  timestampIso: string;
};

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const filter = url.searchParams.get("filter") ?? "";
  const aggregate = url.searchParams.get("aggregate") ?? "";

  const aggsResp = await diagnosticsClient.listAggregates({ filter });
  const aggregates: AggregateRow[] = aggsResp.aggregates.map((a) => ({
    aggregateId: a.aggregateId,
    type: a.type,
    eventCount: a.eventCount,
    lastEventAtIso: a.lastEventAt
      ? timestampDate(a.lastEventAt).toISOString()
      : null,
  }));

  let events: EventRow[] = [];
  if (aggregate) {
    const evResp = await diagnosticsClient.getAggregateEvents({
      aggregateId: aggregate,
    });
    events = evResp.events.map((e) => {
      const d = e.timestamp ? timestampDate(e.timestamp) : null;
      return {
        id: e.id,
        aggregateId: e.aggregateId,
        version: e.version,
        type: e.type,
        position: e.position,
        dataJson: e.dataJson,
        causationId: e.causationId,
        correlationId: e.correlationId,
        timestampMs: d ? d.getTime() : 0,
        timestampIso: d ? d.toISOString() : "",
      };
    });
  }

  return { aggregates, events, aggregate, filter };
}

function prettyJson(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
}

export default function Events({ loaderData }: Route.ComponentProps) {
  const { aggregates, events, aggregate, filter } = loaderData;
  const navigation = useNavigation();
  const [, setSearchParams] = useSearchParams();

  const [filterDraft, setFilterDraft] = useState(filter);
  const [aggregateDraft, setAggregateDraft] = useState(aggregate);
  const [typeFilter, setTypeFilter] = useState<string | null>(null);
  const [textFilter, setTextFilter] = useState("");
  const [sortField, setSortField] = useState<SortField>("version");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  const aggregatesLoading =
    navigation.state === "loading" &&
    navigation.location?.pathname === "/events" &&
    new URLSearchParams(navigation.location?.search ?? "").get("filter") !==
      filter;

  const eventsLoading =
    navigation.state === "loading" &&
    navigation.location?.pathname === "/events" &&
    new URLSearchParams(navigation.location?.search ?? "").get("aggregate") !==
      aggregate;

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
          cmp = a.timestampMs - b.timestampMs;
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

  function applyFilter() {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (filterDraft) next.set("filter", filterDraft);
      else next.delete("filter");
      return next;
    });
  }

  function selectAggregate(id: string) {
    setAggregateDraft(id);
    setExpanded({});
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (id) next.set("aggregate", id);
      else next.delete("aggregate");
      return next;
    });
  }

  function toggleExpanded(id: string) {
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));
  }

  return (
    <Stack gap="md">
      <Title order={4}>Events</Title>

      <Group align="end" gap="sm">
        <TextInput
          label="Aggregate filter"
          placeholder="e.g. orderbook or portfolio:my-account"
          value={filterDraft}
          onChange={(e) => setFilterDraft(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") applyFilter();
          }}
          style={{ flexGrow: 1 }}
        />
        <Button onClick={applyFilter} loading={aggregatesLoading}>
          Search
        </Button>
        <TextInput
          label="Aggregate ID"
          placeholder="type:id"
          value={aggregateDraft}
          onChange={(e) => setAggregateDraft(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") selectAggregate(aggregateDraft);
          }}
          style={{ flexGrow: 1 }}
        />
        <Button
          variant="default"
          onClick={() => selectAggregate(aggregateDraft)}
          loading={eventsLoading}
          disabled={!aggregateDraft}
        >
          Load Events
        </Button>
      </Group>

      <Group align="flex-start" gap="md" wrap="nowrap">
        <Stack gap="xs" w={440}>
          <Text size="sm" c="dimmed">
            Aggregates ({aggregates.length})
          </Text>
          <Box pos="relative" h={500}>
            <LoadingOverlay
              visible={aggregatesLoading}
              zIndex={2}
              overlayProps={{ blur: 1 }}
            />
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
                        aggregate === a.aggregateId
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
                            {a.lastEventAtIso
                              ? new Date(a.lastEventAtIso).toLocaleString()
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
          </Box>
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
              checkIconPosition="right"
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

          <Box pos="relative">
            <LoadingOverlay
              visible={eventsLoading}
              zIndex={2}
              overlayProps={{ blur: 1 }}
            />
            <Table highlightOnHover striped>
              <Table.Thead>
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
                {visibleEvents.map((e) => {
                  const key = e.id || `${e.aggregateId}-${e.version}`;
                  return (
                    <RowGroup
                      key={key}
                      event={e}
                      expanded={!!expanded[key]}
                      onToggle={() => toggleExpanded(key)}
                    />
                  );
                })}
              </Table.Tbody>
            </Table>
          </Box>
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
  event: EventRow;
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
            {event.timestampIso}
          </Text>
        </Table.Td>
        <Table.Td>
          <Text size="xs" ff="monospace" c="dimmed">
            {event.position.toString()}
          </Text>
        </Table.Td>
        <Table.Td>
          <Group gap={4} wrap="nowrap" justify="flex-end">
            {event.correlationId && (
              <ActionIcon
                component={Link}
                to={`/chain?correlation=${encodeURIComponent(event.correlationId)}`}
                variant="subtle"
                color="grape"
                size="xs"
                onClick={(e) => e.stopPropagation()}
                title={`View causal chain (${event.correlationId.slice(0, 8)}…)`}
                aria-label="View causal chain"
              >
                ⇢
              </ActionIcon>
            )}
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
          </Group>
        </Table.Td>
      </Table.Tr>
      {expanded && (
        <Table.Tr>
          <Table.Td colSpan={5}>
            <Box p="xs">
              <Group gap="xs" mb="xs" wrap="wrap">
                <Text size="xs" c="dimmed">
                  id:
                </Text>
                <Code>{event.id}</Code>
                {event.causationId && (
                  <>
                    <Text size="xs" c="dimmed">
                      causation:
                    </Text>
                    <Code>{event.causationId}</Code>
                  </>
                )}
                {event.correlationId && (
                  <>
                    <Text size="xs" c="dimmed">
                      correlation:
                    </Text>
                    <Code>{event.correlationId}</Code>
                  </>
                )}
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
