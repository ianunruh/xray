# Async Snapshotter Implementation Plan

## Context

Snapshots today are captured and persisted on the write path. In
`pkg/es/command.go`, every `Handle` call runs `maybeCaptureSnapshot` under
the per-aggregate lock; when the version crosses an interval boundary the
caller pays for `proto.Marshal` plus `SaveSnapshot` (a PG `INSERT`) before
returning. For the OrderBook (`SnapshotInterval = 5000`) this means one
unlucky `PlaceOrder` every 5000 events absorbs the entire serialization +
DB round trip for the whole resting book. The slow tail it produces is
small in absolute terms but is structurally wrong for an event-sourced
system: snapshots are an optimization for read-side hydration, not a write
concern, and the write side should not block on them.

This plan moves snapshot work off the write path entirely, into a
NATS-driven consumer that maintains an in-memory aggregate per
`aggregate_id` and persists snapshots on its own schedule. The existing
`SnapshotStore` interface, `Snapshotable` aggregate interface, and PG
`snapshots` table are unchanged — only the producer side moves. Loaders
(`Handler.Load`, `Handler.LoadAt`) keep working without modification.

Non-goals for v1: snapshot eviction / TTL (snapshots already overwrite on
PK conflict), snapshot compaction across versions, per-aggregate-type
snapshot scheduling overrides beyond `SnapshotInterval()`, snapshot
storage backends other than the current `Store.SaveSnapshot`.

## Architecture

The snapshotter is a new in-process component (`pkg/es/snapshotter/`) wired
into `cmd/xray` alongside the existing projection consumers. It
subscribes to the `EVENTS` JetStream via its own durable consumer named
`snapshotter`, just like other persistent projections. For each event it
sees, it looks up the aggregate factory by ID prefix, lazily hydrates an
in-memory copy from `LoadSnapshot + LoadFrom`, applies the live event, and
— when the in-memory version crosses an interval boundary past the last
saved snapshot version — marshals and saves.

```
write path                                snapshotter
─────────────────                         ─────────────────────────────
RPC → Handler.Handle                      ProjectionConsumer (durable
   ↳ lock                                   "snapshotter")
   ↳ load / execute / append              ↳ on each event:
   ↳ publish to NATS  ─────► EVENTS ─────►   factory lookup by prefix
   ↳ unlock                                  lazy hydrate (snapshot + tail)
   ↳ return                                  apply(event)
                                             if crossed interval boundary:
                                               marshal + SaveSnapshot
                                             update checkpoint
```

The write path no longer calls `SaveSnapshot`, no longer marshals
aggregate state, and no longer carries `snapshotVersion` in its cache.

```
# Framework
pkg/es/snapshotter/
  snapshotter.go            # Consumer + Registry types, event loop
  snapshotter_test.go       # interval triggering, lazy hydration, restart
  registry.go               # AggregateFactory registration by ID prefix

# Wiring
cmd/xray/main.go            # construct snapshotter, register factories,
                            # add ProjectionConsumer named "snapshotter"
                            # drop .WithSnapshots(store) from obHandler write path
                            # (keep .WithSnapshots on Load-only paths — see below)

# Framework cleanup
pkg/es/command.go           # remove maybeCaptureSnapshot, pendingSnapshot,
                            # saveSnapshot; drop snapshotVersion from
                            # cachedEntry; tryHandle no longer touches snapshots
pkg/es/snapshot.go          # unchanged
pkg/es/command_test.go      # update tests that asserted inline snapshotting
                            # (move those assertions to snapshotter_test.go)
```

`Handler.WithSnapshots` stays — it's still needed on the **read** side for
`Load` and `LoadAt` to find existing snapshots. The behavior change is
that `Handle` (the write path) stops calling `SaveSnapshot` regardless of
whether snapshots are configured.

## Snapshotter as a projection

The snapshotter implements `es.Projection` and slots into the existing
`natsstore.ProjectionConsumer` machinery. It uses `WithPersistent` so it
gets its own durable JetStream cursor and PG checkpoint, surviving
restarts the same way the trade and order projections do.

```go
type Snapshotter struct {
    log       *slog.Logger
    store     es.EventStore        // for lazy hydration (LoadSnapshot + LoadFrom)
    snapshots es.SnapshotStore     // for SaveSnapshot
    registry  *es.Registry         // for catch-up Deserialize during hydration
    factories map[string]factoryFn // prefix → factory
    aggs      map[string]*entry    // aggregate_id → in-memory state
}

type factoryFn func(id string) es.Aggregate

type entry struct {
    agg              es.Aggregate
    version          int  // version of the in-memory aggregate
    lastSavedVersion int  // version of the last SaveSnapshot we performed
    interval         int  // cached from SnapshotInterval()
}
```

Registration is by aggregate-ID prefix, mirroring how aggregates are
named today (`orderbook:AAPL`, `portfolio:acct-1`, etc.):

```go
snap.Register("orderbook", func(id string) es.Aggregate {
    return orderbook.NewOrderBook(id)
})
```

Only aggregates whose factories produce a `Snapshotable` are registered.
Events for unregistered prefixes are ignored.

## HandleEvents loop

```go
func (s *Snapshotter) HandleEvents(ctx context.Context, events []es.Event) error {
    for _, evt := range events {
        e, err := s.ensureAggregate(ctx, evt.AggregateID)
        if err != nil {
            s.log.Error("snapshotter hydrate failed",
                "aggregate_id", evt.AggregateID, "error", err)
            continue
        }
        if e == nil {
            continue // unregistered prefix
        }

        if evt.Version <= e.version {
            // Already applied during lazy hydrate, skip.
            continue
        }
        if err := e.agg.Apply(evt); err != nil {
            return fmt.Errorf("apply event v%d to %s: %w",
                evt.Version, evt.AggregateID, err)
        }
        e.version = evt.Version

        if e.version/e.interval > e.lastSavedVersion/e.interval {
            if err := s.save(ctx, evt.AggregateID, e); err != nil {
                s.log.Error("snapshotter save failed",
                    "aggregate_id", evt.AggregateID, "error", err)
                // Don't return — next threshold boundary will retry.
            }
        }
    }
    return nil
}
```

The `version/interval > lastSavedVersion/interval` predicate is the same
one `command.go` uses today, so the snapshot cadence is unchanged.

### Lazy hydration

On first sight of `aggregate_id` post-restart (or first sight ever), the
snapshotter rebuilds in-memory state from durable storage:

```go
func (s *Snapshotter) ensureAggregate(ctx context.Context, id string) (*entry, error) {
    if e, ok := s.aggs[id]; ok {
        return e, nil
    }
    factory, ok := s.factoryFor(id)
    if !ok {
        return nil, nil
    }

    agg := factory(id)
    sa, ok := agg.(es.Snapshotable)
    if !ok {
        return nil, nil
    }

    snap, err := s.snapshots.LoadSnapshot(ctx, id)
    if err != nil {
        return nil, fmt.Errorf("load snapshot: %w", err)
    }
    startFrom := 1
    lastSaved := 0
    if snap != nil {
        msg := protoCloneFor(sa)
        if err := proto.Unmarshal(snap.Data, msg); err != nil {
            return nil, fmt.Errorf("unmarshal snapshot: %w", err)
        }
        if err := sa.RestoreSnapshot(msg); err != nil {
            return nil, fmt.Errorf("restore snapshot: %w", err)
        }
        if vs, ok := agg.(interface{ SetVersion(int) }); ok {
            vs.SetVersion(snap.Version)
        }
        startFrom = snap.Version + 1
        lastSaved = snap.Version
    }

    raw, err := s.store.LoadFrom(ctx, id, startFrom)
    if err != nil {
        return nil, fmt.Errorf("load events from v%d: %w", startFrom, err)
    }
    for _, r := range raw {
        evt, err := s.registry.Deserialize(r)
        if err != nil {
            return nil, fmt.Errorf("deserialize: %w", err)
        }
        if err := agg.Apply(evt); err != nil {
            return nil, fmt.Errorf("apply v%d during hydrate: %w", evt.Version, err)
        }
    }

    e := &entry{
        agg:              agg,
        version:          snap.versionOr(0) + len(raw),
        lastSavedVersion: lastSaved,
        interval:         sa.SnapshotInterval(),
    }
    s.aggs[id] = e
    return e, nil
}
```

This deliberately reads the store, not just the snapshot, because the
snapshotter's durable cursor may be behind the head — the next live event
could be many versions past the last persisted snapshot. After
hydration, the `evt.Version <= e.version` check in `HandleEvents` swallows
the redundant catch-up events.

### Catch-up vs. live

`ProjectionConsumer` already distinguishes catch-up from live by draining
the JetStream consumer until a poll returns no events. During catch-up,
`HandleEvents` may receive thousands of events for the same aggregate
back-to-back. The same boundary-crossing check applies, but a useful
optimization is to coalesce: during a single `HandleEvents` batch, only
save the snapshot once at the highest crossed boundary. Implemented as
"defer the save until the loop ends":

```go
saved := make(map[string]bool)
for _, evt := range events {
    // ... apply ...
    if e.version/e.interval > e.lastSavedVersion/e.interval {
        saved[evt.AggregateID] = true
    }
}
for id := range saved {
    s.save(ctx, id, s.aggs[id])
}
```

This trims catch-up from N/interval saves down to 1 per aggregate per
batch.

## Removing snapshots from the write path

`pkg/es/command.go` changes:

1. `cachedEntry[A]` drops `snapshotVersion int`. `Put` and `Take`
   signatures simplify accordingly.
2. `tryHandle` no longer calls `maybeCaptureSnapshot`, no longer captures
   `snap`, returns just `(newEvents, error)`.
3. `Handle` drops the `saveSnapshot` call. Returns immediately after
   `publishEvents`.
4. Delete `pendingSnapshot`, `maybeCaptureSnapshot`, `saveSnapshot`.
5. `loadAggregate` (the read-side path used by `Load` / `LoadAt`) is
   **kept** — snapshots are still consumed for fast hydration.

The result: a successful `Handle` does load + execute + append + publish,
period. Snapshot work moves to a different goroutine in a different
process subscription.

## Wiring (`cmd/xray/main.go`)

```go
snap := snapshotter.New(store, store, registry, log)
snap.Register("orderbook", func(id string) es.Aggregate {
    return orderbook.NewOrderBook(id)
})
// future: snap.Register("portfolio", ...) once Portfolio implements Snapshotable

consumers = append(consumers,
    natsstore.NewProjectionConsumer(js, registry, log, "snapshotter").
        WithPersistent(store, snap),
)
```

Drop `.WithSnapshots(store)` from the **write-side** wiring? No — keep
it. `WithSnapshots` still configures the read-side loader. Removing it
would force `LoadAt` to replay from version 1, defeating the snapshot's
purpose.

## Failure handling

- **Marshal error**: log + continue. Aggregate state is fine; next
  threshold boundary will retry.
- **SaveSnapshot error**: log + continue. `lastSavedVersion` is *not*
  advanced, so the next event past the same boundary retries.
- **Apply error during live processing**: this is a structural bug
  (aggregate disagreed with the write-side that produced the event).
  Return the error from `HandleEvents`; the projection consumer will
  retry the batch a few times then surface it. Same behavior as other
  projections that disagree with events.
- **Hydrate error**: log + skip. The aggregate stays absent from
  `s.aggs`, so the next event for it retries hydration. If hydration is
  permanently broken (e.g., unregistered event in `Apply`), the
  snapshotter degrades to "no snapshots for this aggregate"; loads still
  work, they just replay full history.
- **Snapshotter restart**: in-memory map is empty; lazy hydrate rebuilds
  on demand. Durable cursor resumes from the saved checkpoint.

The key invariant preserved: `snapshots.Version <= true aggregate
version` at all times. The snapshotter only writes versions it has
observed and applied; it never advances ahead of the event log.

## Edge cases — explicit tests

| Case | Expected outcome |
|---|---|
| First event for a new aggregate, no prior snapshot | Lazy hydrate is a no-op; apply event; no save unless interval=1 |
| Event arrives for unregistered prefix | Ignored; no error |
| Snapshotter restart mid-stream | Re-hydrate from last snapshot + LoadFrom; catch-up batch dedupes events with `version <= e.version` |
| Catch-up batch spanning many intervals | One save per aggregate per `HandleEvents` invocation (not N) |
| SaveSnapshot fails once, succeeds next time | `lastSavedVersion` stays at old value; retry at next event past the same boundary boundary; second attempt advances it |
| Aggregate whose factory produces a non-`Snapshotable` | `ensureAggregate` returns nil; events ignored |
| Two events for the same aggregate in one batch crossing two boundaries | Both `Apply`d; one save at the latest crossed boundary |
| Hydration sees a snapshot at version V but `LoadFrom(V+1)` returns events that conflict with `Apply` | Return error from hydrate; aggregate not cached; logged |

## Phased rollout

Each step is independently shippable; the system keeps snapshotting
correctly the whole way through.

1. **Add snapshotter scaffolding alongside inline path.**
   New `pkg/es/snapshotter/` package: `Snapshotter`, `Registry`,
   `HandleEvents`, lazy hydration, save-on-boundary. Unit tests against
   memstore-backed `EventStore` and `SnapshotStore`. **No changes to
   `command.go` yet — both producers run.** Verify the snapshotter
   writes the same snapshots the inline path does (in tests, compare
   bytes). Acceptable to have duplicate writes briefly; PK overwrite is
   idempotent.

2. **Wire snapshotter into `cmd/xray`.**
   Construct, register `orderbook`, add as a persistent consumer.
   Manual smoke: place 5000+ orders, observe both inline and async
   saves landing on the same row; confirm latest writer wins, version
   stays monotonic. Watch logs for snapshotter errors.

3. **Remove snapshotting from the write path.**
   Drop `maybeCaptureSnapshot` / `pendingSnapshot` / `saveSnapshot` from
   `command.go`; simplify `cachedEntry` to drop `snapshotVersion`;
   update `command_test.go` (the tests that asserted inline behavior
   move to `snapshotter_test.go`). After this point the snapshotter is
   the sole producer.

4. **Coalesce catch-up saves.**
   The dedupe-by-aggregate-id-per-batch optimization. Add a test that
   replays 50k events for one aggregate (interval 5000) and asserts
   exactly 1 `SaveSnapshot` call per `HandleEvents` invocation
   regardless of batch shape.

5. **(Optional follow-up.)** Make `Portfolio` `Snapshotable`. Portfolio
   has unbounded growth in `Holdings` + per-saga idempotency maps and
   would benefit. Outside this plan's scope but the snapshotter machinery
   makes it a one-liner registration.

## Verification

1. `go build ./...` compiles cleanly at the end of each phase.
2. `go test ./...` passes at the end of each phase. The inline-path tests
   in `pkg/es/command_test.go` need to migrate to the snapshotter after
   phase 3 — they currently assert that `SaveSnapshot` is called from
   `Handle`, which will no longer be true.
3. Manual: start xray with NATS, run `cmd/loadtest` (or the existing
   noise+trend strategies) past 5000 events. Observe:
   - Snapshotter log line per crossed boundary
   - PG `snapshots` table updates
   - `OrderBookService.PlaceOrder` p99 latency drops (any spike at
     event 5000 disappears)
4. Restart xray. Observe snapshotter resumes from checkpoint, lazy
   re-hydrates on next live event per aggregate, and continues saving.

## Tradeoffs and notes

- **Snapshot lag.** Snapshots now lag the write by however long it takes
  the snapshotter to drain its consumer — typically <1s, but seconds
  under burst. The only consumer of snapshot freshness is aggregate
  hydration on cold load, which always pairs the snapshot with
  `LoadFrom(snap.Version+1)` to catch up. So lag is invisible to readers.
- **In-memory cost.** The snapshotter holds one live copy of every
  active aggregate. For xray this is bounded (a few orderbooks, a few
  portfolios). If this grows, evict idle entries on a timer; the next
  event re-hydrates.
- **One process for now.** The snapshotter is a goroutine inside
  `cmd/xray`, not a separate binary. Splitting it out is a wiring
  change with no design consequence — the durable cursor and stateless
  hydration mean a second process can run alongside the first without
  conflict (last writer wins on the snapshots row, both will agree).
