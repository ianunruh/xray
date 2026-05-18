import { Button, Group, Slider, Stack, Text } from "@mantine/core";
import type { ReplayBounds } from "~/lib/replay";

// ReplayControls renders the time scrubber for replay mode. The slider's
// position is held in atDate (ms since epoch). Start/end jump buttons fire
// version-based callbacks since exact endpoint addressing by timestamp can
// undershoot due to Date millisecond precision.
export function ReplayControls({
  bounds,
  atDate,
  onScrub,
  onJumpStart,
  onJumpEnd,
  onRefresh,
}: {
  bounds: ReplayBounds;
  atDate: Date;
  onScrub: (d: Date) => void;
  onJumpStart: () => void;
  onJumpEnd: () => void;
  onRefresh: () => void;
}) {
  const min = bounds.firstDate.getTime();
  const max = bounds.lastDate.getTime();
  const value = Math.min(Math.max(atDate.getTime(), min), max);
  const step = Math.max(1, Math.floor((max - min) / 1000));

  return (
    <Stack gap={4}>
      <Group justify="space-between" align="center">
        <Text size="xs" c="dimmed">
          Replay {atDate.toLocaleString()}
        </Text>
        <Group gap={4}>
          <Button size="compact-xs" variant="subtle" onClick={onJumpStart}>
            ⏮ start
          </Button>
          <Button size="compact-xs" variant="subtle" onClick={onJumpEnd}>
            end ⏭
          </Button>
          <Button size="compact-xs" variant="subtle" onClick={onRefresh}>
            ↻ bounds
          </Button>
        </Group>
      </Group>
      <Slider
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(v) => onScrub(new Date(v))}
        label={(v) => new Date(v).toLocaleTimeString()}
      />
      <Group justify="space-between" px={4}>
        <Text size="xs" c="dimmed" ff="monospace">
          {bounds.firstDate.toLocaleTimeString()}
        </Text>
        <Text size="xs" c="dimmed" ff="monospace">
          {bounds.lastDate.toLocaleTimeString()}
        </Text>
      </Group>
    </Stack>
  );
}
