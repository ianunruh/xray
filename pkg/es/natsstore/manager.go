package natsstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ianunruh/xray/pkg/es"
)

// ProjectionPhase is the lifecycle state surfaced to the management UI.
type ProjectionPhase int

const (
	PhaseRunning ProjectionPhase = iota
	PhaseRebuilding
	PhaseStopped
	PhaseFailed
)

func (p ProjectionPhase) String() string {
	switch p {
	case PhaseRunning:
		return "running"
	case PhaseRebuilding:
		return "rebuilding"
	case PhaseStopped:
		return "stopped"
	case PhaseFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// ProjectionStatus is a point-in-time snapshot of a consumer's state.
type ProjectionStatus struct {
	Name                  string
	Phase                 ProjectionPhase
	Checkpoint            uint64
	HeadSequence          uint64
	Lag                   uint64
	Rebuildable           bool
	ReasonNotRebuildable  string
	RebuildStartedAt      time.Time
	RebuildLastError      string
	ProjectionCount       int
	ResettableCount       int
}

// ProgressEvent is published to subscribers during a rebuild.
type ProgressEvent struct {
	Name          string
	Phase         ProjectionPhase
	Position      uint64
	HeadSequence  uint64
	EventsPerSec  float64
	ETASeconds    int64
	BatchSize     int
	Err           string
	At            time.Time
}

// ErrNotRebuildable is returned when Rebuild is called against a consumer
// that hosts a reactor.
var ErrNotRebuildable = errors.New("consumer is not rebuildable (hosts a reactor)")

// ErrUnknownConsumer is returned when the named consumer is not registered.
var ErrUnknownConsumer = errors.New("unknown consumer")

// ErrAlreadyRebuilding is returned when a second Rebuild is requested for
// a consumer that's already rebuilding.
var ErrAlreadyRebuilding = errors.New("consumer is already rebuilding")

type managedConsumer struct {
	consumer *ProjectionConsumer

	mu               sync.Mutex
	phase            ProjectionPhase
	rebuildStartedAt time.Time
	lastError        string

	subsMu sync.Mutex
	subs   map[int]chan ProgressEvent
	nextID int
}

// ProjectionManager owns every ProjectionConsumer and mediates introspection
// and rebuilds. Pass it the same consumer instances passed to consumer.Start
// in main; the manager does not start or stop consumers on its own except
// when fulfilling a Rebuild request.
type ProjectionManager struct {
	js  jetstream.JetStream
	log *slog.Logger

	mu        sync.RWMutex
	consumers map[string]*managedConsumer

	headCache struct {
		sync.RWMutex
		seq       uint64
		fetchedAt time.Time
	}
}

// NewProjectionManager constructs a manager bound to the given JetStream
// context. Register consumers with Add.
func NewProjectionManager(js jetstream.JetStream, log *slog.Logger) *ProjectionManager {
	return &ProjectionManager{
		js:        js,
		log:       log,
		consumers: make(map[string]*managedConsumer),
	}
}

// Add registers a consumer with the manager. Consumers must already have
// been Start-ed (or be about to be) — the manager only intercepts the
// lifecycle during a rebuild.
func (m *ProjectionManager) Add(c *ProjectionConsumer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumers[c.Name()] = &managedConsumer{
		consumer: c,
		phase:    PhaseRunning,
		subs:     make(map[int]chan ProgressEvent),
	}
}

// List returns a status snapshot for every registered consumer, in name order.
func (m *ProjectionManager) List(ctx context.Context) ([]ProjectionStatus, error) {
	head, err := m.headSequence(ctx)
	if err != nil {
		m.log.Warn("failed to read stream head; reporting head=0", "error", err)
		head = 0
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]ProjectionStatus, 0, len(m.consumers))
	for _, mc := range m.consumers {
		out = append(out, m.statusLocked(ctx, mc, head))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Status returns the status of one consumer.
func (m *ProjectionManager) Status(ctx context.Context, name string) (ProjectionStatus, error) {
	m.mu.RLock()
	mc, ok := m.consumers[name]
	m.mu.RUnlock()
	if !ok {
		return ProjectionStatus{}, ErrUnknownConsumer
	}
	head, err := m.headSequence(ctx)
	if err != nil {
		head = 0
	}
	return m.statusLocked(ctx, mc, head), nil
}

func (m *ProjectionManager) statusLocked(ctx context.Context, mc *managedConsumer, head uint64) ProjectionStatus {
	mc.mu.Lock()
	phase := mc.phase
	startedAt := mc.rebuildStartedAt
	lastErr := mc.lastError
	mc.mu.Unlock()

	var checkpoint uint64
	if cs := mc.consumer.CheckpointStore(); cs != nil {
		if seq, err := cs.LoadCheckpoint(ctx, mc.consumer.Name()); err == nil {
			checkpoint = seq
		}
	}

	var lag uint64
	if head > checkpoint {
		lag = head - checkpoint
	}

	resettable := 0
	for _, p := range mc.consumer.Projections() {
		if _, ok := p.(es.Resettable); ok {
			resettable++
		}
	}

	rebuildable, reason := m.rebuildEligibility(mc.consumer, resettable)

	return ProjectionStatus{
		Name:                 mc.consumer.Name(),
		Phase:                phase,
		Checkpoint:           checkpoint,
		HeadSequence:         head,
		Lag:                  lag,
		Rebuildable:          rebuildable,
		ReasonNotRebuildable: reason,
		RebuildStartedAt:     startedAt,
		RebuildLastError:     lastErr,
		ProjectionCount:      len(mc.consumer.Projections()),
		ResettableCount:      resettable,
	}
}

func (m *ProjectionManager) rebuildEligibility(c *ProjectionConsumer, resettable int) (bool, string) {
	if c.IsReactor() {
		return false, "consumer hosts a reactor; rebuild would re-execute commands"
	}
	if c.CheckpointStore() == nil {
		return false, "consumer is ephemeral (rebuilds on every boot)"
	}
	if resettable == 0 {
		return false, "no projections implement Resettable"
	}
	return true, ""
}

// Subscribe returns a channel that receives progress events for the named
// consumer. The returned cancel func unsubscribes and drains the channel.
// Buffered so a slow subscriber drops ticks rather than blocking the
// dispatch goroutine.
func (m *ProjectionManager) Subscribe(name string) (<-chan ProgressEvent, func(), error) {
	m.mu.RLock()
	mc, ok := m.consumers[name]
	m.mu.RUnlock()
	if !ok {
		return nil, nil, ErrUnknownConsumer
	}

	mc.subsMu.Lock()
	id := mc.nextID
	mc.nextID++
	ch := make(chan ProgressEvent, 32)
	mc.subs[id] = ch
	mc.subsMu.Unlock()

	cancel := func() {
		mc.subsMu.Lock()
		if existing, ok := mc.subs[id]; ok {
			delete(mc.subs, id)
			close(existing)
		}
		mc.subsMu.Unlock()
	}
	return ch, cancel, nil
}

func (mc *managedConsumer) publish(evt ProgressEvent) {
	mc.subsMu.Lock()
	defer mc.subsMu.Unlock()
	for _, ch := range mc.subs {
		select {
		case ch <- evt:
		default:
			// Slow subscriber; drop this tick rather than block the
			// dispatch goroutine.
		}
	}
}

// Rebuild orchestrates an in-place rebuild of the named consumer. The
// sequence is:
//  1. Stop the consumer goroutine (waits for in-flight batch).
//  2. Reset every Resettable projection (truncates target tables).
//  3. Delete the PG checkpoint row.
//  4. Delete the JetStream durable so the next Start re-delivers from seq 1.
//  5. Install a progress hook that publishes to subscribers.
//  6. Restart the consumer; Start blocks on catch-up, hook fires per batch.
//  7. Remove the hook, mark Running.
//
// Returns immediately if validation fails. On success, runs the rebuild
// synchronously and returns once catch-up is complete; callers typically
// invoke this from a goroutine.
func (m *ProjectionManager) Rebuild(ctx context.Context, name string) error {
	m.mu.RLock()
	mc, ok := m.consumers[name]
	m.mu.RUnlock()
	if !ok {
		return ErrUnknownConsumer
	}

	resettable := 0
	for _, p := range mc.consumer.Projections() {
		if _, ok := p.(es.Resettable); ok {
			resettable++
		}
	}
	if ok, reason := m.rebuildEligibility(mc.consumer, resettable); !ok {
		return fmt.Errorf("%w: %s", ErrNotRebuildable, reason)
	}

	mc.mu.Lock()
	if mc.phase == PhaseRebuilding {
		mc.mu.Unlock()
		return ErrAlreadyRebuilding
	}
	mc.phase = PhaseRebuilding
	mc.rebuildStartedAt = time.Now()
	mc.lastError = ""
	mc.mu.Unlock()

	err := m.runRebuild(ctx, mc)

	mc.mu.Lock()
	if err != nil {
		mc.phase = PhaseFailed
		mc.lastError = err.Error()
	} else {
		mc.phase = PhaseRunning
	}
	mc.mu.Unlock()

	mc.publish(ProgressEvent{
		Name:  mc.consumer.Name(),
		Phase: func() ProjectionPhase {
			if err != nil {
				return PhaseFailed
			}
			return PhaseRunning
		}(),
		At: time.Now(),
		Err: func() string {
			if err != nil {
				return err.Error()
			}
			return ""
		}(),
	})

	return err
}

func (m *ProjectionManager) runRebuild(ctx context.Context, mc *managedConsumer) error {
	c := mc.consumer
	log := m.log.With("consumer", c.Name())

	// 1. Stop the running consumer.
	log.Info("rebuild: stopping consumer")
	c.Stop()

	// 2. Reset projections.
	log.Info("rebuild: resetting projections")
	for _, p := range c.Projections() {
		r, ok := p.(es.Resettable)
		if !ok {
			continue
		}
		if err := r.Reset(ctx); err != nil {
			return fmt.Errorf("reset projection: %w", err)
		}
	}

	// 3. Delete checkpoint.
	if cs := c.CheckpointStore(); cs != nil {
		log.Info("rebuild: deleting checkpoint")
		if err := cs.DeleteCheckpoint(ctx, c.Name()); err != nil {
			return fmt.Errorf("delete checkpoint: %w", err)
		}
	}

	// 4. Delete the JetStream durable so the next CreateOrUpdateConsumer
	// in ensureConsumer re-delivers from sequence 1. NATS keeps the
	// stream messages themselves; only the cursor is dropped.
	log.Info("rebuild: deleting jetstream durable")
	if err := m.js.DeleteConsumer(ctx, StreamName, c.Name()); err != nil {
		// Treat "not found" as success: durable may already be gone.
		if !errors.Is(err, jetstream.ErrConsumerNotFound) {
			return fmt.Errorf("delete jetstream durable: %w", err)
		}
	}

	// 5/6. Install progress hook, restart. Start blocks until catch-up
	// completes; hook fires per batch from inside fetchAndDispatch.
	head, err := m.headSequence(ctx)
	if err != nil {
		log.Warn("rebuild: failed to read stream head", "error", err)
		head = 0
	}

	startedAt := time.Now()
	var totalEvents int64
	c.SetProgressHook(func(batchSize int, newCheckpoint uint64) {
		totalEvents += int64(batchSize)
		elapsed := time.Since(startedAt).Seconds()
		var eps float64
		if elapsed > 0 {
			eps = float64(totalEvents) / elapsed
		}
		var eta int64
		if eps > 0 && head > newCheckpoint {
			eta = int64(float64(head-newCheckpoint) / eps)
		}
		mc.publish(ProgressEvent{
			Name:         c.Name(),
			Phase:        PhaseRebuilding,
			Position:     newCheckpoint,
			HeadSequence: head,
			EventsPerSec: eps,
			ETASeconds:   eta,
			BatchSize:    batchSize,
			At:           time.Now(),
		})
	})

	log.Info("rebuild: restarting consumer", "head", head)
	startErr := c.Start(ctx)

	// 7. Always remove the hook so steady-state dispatches don't carry
	// rebuild publish overhead.
	c.SetProgressHook(nil)

	if startErr != nil {
		return fmt.Errorf("restart consumer: %w", startErr)
	}
	log.Info("rebuild: complete", "events", totalEvents, "elapsed", time.Since(startedAt))
	return nil
}

// headSequence returns the current stream head (last published sequence),
// cached briefly to avoid hammering NATS when List is polled.
func (m *ProjectionManager) headSequence(ctx context.Context) (uint64, error) {
	const ttl = 2 * time.Second

	m.headCache.RLock()
	if time.Since(m.headCache.fetchedAt) < ttl && m.headCache.seq > 0 {
		seq := m.headCache.seq
		m.headCache.RUnlock()
		return seq, nil
	}
	m.headCache.RUnlock()

	stream, err := m.js.Stream(ctx, StreamName)
	if err != nil {
		return 0, fmt.Errorf("get stream: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("stream info: %w", err)
	}

	m.headCache.Lock()
	m.headCache.seq = info.State.LastSeq
	m.headCache.fetchedAt = time.Now()
	m.headCache.Unlock()

	return info.State.LastSeq, nil
}
