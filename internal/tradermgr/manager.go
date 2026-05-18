package tradermgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/gen/saga/v1/sagav1connect"
	traderv1 "github.com/ianunruh/xray/gen/trader/v1"
	"github.com/ianunruh/xray/internal/mm"
	"github.com/ianunruh/xray/internal/noise"
	"github.com/ianunruh/xray/internal/pricesource"
)

// SymbolWatcher is implemented by price sources that can grow their
// polling set at runtime (PolygonPriceSource). Static sources don't
// need to and the manager skips the call.
type SymbolWatcher interface {
	WatchSymbol(symbol string)
}

// Manager supervises in-process trader engines and persists their config.
//
// On construction it opens HTTP/2 clients pointed at the local xray
// server (same RPCs the standalone CLIs use). Each running trader owns
// its own context; stopping cancels the goroutine and removes it from
// the registry. The persisted enabled flag determines what auto-starts
// on server boot.
type Manager struct {
	store  *Store
	prices pricesource.PriceSource
	log    *slog.Logger

	obClient   orderbookv1connect.OrderBookServiceClient
	pfClient   portfoliov1connect.PortfolioServiceClient
	sagaClient sagav1connect.SagaServiceClient

	mu      sync.Mutex
	running map[string]*runState
}

type runState struct {
	cancel    context.CancelFunc
	lastError string
	failed    bool
}

func NewManager(store *Store, prices pricesource.PriceSource, serverURL string, log *slog.Logger) *Manager {
	// http.DefaultClient (with HTTP/2 enabled via Protocols.SetUnencryptedHTTP2)
	// is what the standalone CLIs use; matching that gives us identical
	// streaming semantics for fills/portfolio.
	httpClient := &http.Client{}
	return &Manager{
		store:      store,
		prices:     prices,
		log:        log,
		obClient:   orderbookv1connect.NewOrderBookServiceClient(httpClient, serverURL),
		pfClient:   portfoliov1connect.NewPortfolioServiceClient(httpClient, serverURL),
		sagaClient: sagav1connect.NewSagaServiceClient(httpClient, serverURL),
		running:    make(map[string]*runState),
	}
}

// AutoStart launches every trader marked enabled in the store. Called once
// during server boot after the price source has been started.
func (m *Manager) AutoStart(ctx context.Context) error {
	records, err := m.store.List(ctx)
	if err != nil {
		return err
	}
	for _, r := range records {
		if !r.Enabled {
			continue
		}
		if err := m.start(ctx, r); err != nil {
			m.log.Error("auto-start failed", "id", r.ID, "name", r.Name, "error", err)
			_ = m.store.SetLastError(ctx, r.ID, err.Error())
		}
	}
	return nil
}

// List returns every persisted trader merged with the in-memory run state.
func (m *Manager) List(ctx context.Context) ([]*traderv1.Trader, error) {
	records, err := m.store.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*traderv1.Trader, 0, len(records))
	for _, r := range records {
		t, err := m.recordToProto(r)
		if err != nil {
			m.log.Warn("decode trader failed", "id", r.ID, "error", err)
			continue
		}
		out = append(out, t)
	}
	return out, nil
}

func (m *Manager) Get(ctx context.Context, id string) (*traderv1.Trader, error) {
	r, err := m.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return m.recordToProto(r)
}

// Create persists a new trader and optionally starts it. The id is
// generated server-side (uuid) so concurrent creates can't collide.
func (m *Manager) Create(ctx context.Context, name string, t traderv1.TraderType, cfg *traderv1.TraderConfig, start bool) (*traderv1.Trader, error) {
	typeStr, err := typeFromProto(t)
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	cfgBytes, err := marshalConfig(typeStr, cfg)
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()
	rec := Record{
		ID:      id,
		Type:    typeStr,
		Name:    name,
		Config:  cfgBytes,
		Enabled: start,
	}
	if err := m.store.Insert(ctx, rec); err != nil {
		return nil, err
	}

	if start {
		// Re-read to get the persisted timestamps before starting; if
		// start fails we surface the error but the row remains so the
		// user can fix the config and try again.
		fresh, err := m.store.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if startErr := m.start(ctx, fresh); startErr != nil {
			_ = m.store.SetEnabled(ctx, id, false)
			_ = m.store.SetLastError(ctx, id, startErr.Error())
			return m.Get(ctx, id)
		}
	}

	return m.Get(ctx, id)
}

// Update changes name + config. If the trader is currently running it is
// stopped and restarted with the new config so changes take effect
// immediately rather than after the next manual restart.
func (m *Manager) Update(ctx context.Context, id, name string, cfg *traderv1.TraderConfig) (*traderv1.Trader, error) {
	rec, err := m.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	cfgBytes, err := marshalConfig(rec.Type, cfg)
	if err != nil {
		return nil, err
	}

	wasRunning := m.isRunning(id)
	if wasRunning {
		m.stop(id)
	}

	if err := m.store.UpdateConfig(ctx, id, name, cfgBytes); err != nil {
		return nil, err
	}

	if wasRunning {
		fresh, err := m.store.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if startErr := m.start(ctx, fresh); startErr != nil {
			_ = m.store.SetEnabled(ctx, id, false)
			_ = m.store.SetLastError(ctx, id, startErr.Error())
		}
	}
	return m.Get(ctx, id)
}

func (m *Manager) Start(ctx context.Context, id string) (*traderv1.Trader, error) {
	rec, err := m.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if m.isRunning(id) {
		return m.Get(ctx, id)
	}
	if err := m.store.SetEnabled(ctx, id, true); err != nil {
		return nil, err
	}
	if err := m.start(ctx, rec); err != nil {
		_ = m.store.SetEnabled(ctx, id, false)
		_ = m.store.SetLastError(ctx, id, err.Error())
		return nil, err
	}
	return m.Get(ctx, id)
}

func (m *Manager) Stop(ctx context.Context, id string) (*traderv1.Trader, error) {
	if _, err := m.store.Get(ctx, id); err != nil {
		return nil, err
	}
	if err := m.store.SetEnabled(ctx, id, false); err != nil {
		return nil, err
	}
	m.stop(id)
	return m.Get(ctx, id)
}

// StartAll starts every persisted trader that isn't already running. Failed
// starts are recorded per-trader (enabled cleared, last_error persisted) but
// don't abort the loop — callers get back counts so the UI can surface a
// summary notification.
func (m *Manager) StartAll(ctx context.Context) (started, failed int, err error) {
	records, err := m.store.List(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, r := range records {
		if m.isRunning(r.ID) {
			continue
		}
		if _, startErr := m.Start(ctx, r.ID); startErr != nil {
			failed++
			m.log.Warn("start-all: trader failed to start", "id", r.ID, "name", r.Name, "error", startErr)
			continue
		}
		started++
	}
	return started, failed, nil
}

// StopAll stops every currently-running trader and clears each one's enabled
// flag so they don't auto-start on the next server boot. Returns the number
// of traders that were actually running.
func (m *Manager) StopAll(ctx context.Context) (int, error) {
	m.mu.Lock()
	ids := make([]string, 0, len(m.running))
	for id := range m.running {
		ids = append(ids, id)
	}
	m.mu.Unlock()

	stopped := 0
	for _, id := range ids {
		if _, err := m.Stop(ctx, id); err != nil {
			if errors.Is(err, ErrNotFound) {
				// Trader was deleted between snapshot and stop — best-effort
				// cancel the goroutine and move on.
				m.stop(id)
				continue
			}
			return stopped, err
		}
		stopped++
	}
	return stopped, nil
}

func (m *Manager) Delete(ctx context.Context, id string) error {
	m.stop(id)
	return m.store.Delete(ctx, id)
}

// --- internals --------------------------------------------------------------

func (m *Manager) isRunning(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.running[id]
	return ok && !st.failed
}

func (m *Manager) stop(id string) {
	m.mu.Lock()
	st, ok := m.running[id]
	if ok {
		delete(m.running, id)
	}
	m.mu.Unlock()
	if ok && st.cancel != nil {
		st.cancel()
	}
}

// start spawns a goroutine running the engine. It must be called with the
// store row freshly loaded so config + timestamps reflect the latest write.
// The goroutine updates last_error in the store on exit so the failure
// reason persists across server restarts.
func (m *Manager) start(parent context.Context, rec Record) error {
	if w, ok := m.prices.(SymbolWatcher); ok {
		if sym := recordSymbol(rec); sym != "" {
			w.WatchSymbol(sym)
		}
	}

	runFn, err := m.buildEngine(rec)
	if err != nil {
		return err
	}

	// Detach from the request context so an HTTP handler returning
	// doesn't cancel the engine. Parent ctx is only used to honor
	// shutdown if start is called during server-wide cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	if parent.Err() != nil {
		cancel()
		return parent.Err()
	}

	m.mu.Lock()
	if existing, ok := m.running[rec.ID]; ok {
		existing.cancel()
	}
	m.running[rec.ID] = &runState{cancel: cancel}
	m.mu.Unlock()

	go func() {
		err := runFn(ctx)
		exitMsg := ""
		failed := false
		if err != nil && !errors.Is(err, context.Canceled) {
			exitMsg = err.Error()
			failed = true
		}

		m.mu.Lock()
		if st, ok := m.running[rec.ID]; ok && st.cancel != nil {
			// Only clear the registry entry if it's still the one we
			// installed. A concurrent restart already overwrote it.
			st.failed = failed
			st.lastError = exitMsg
			if !failed {
				delete(m.running, rec.ID)
			}
		}
		m.mu.Unlock()

		// Persist failure asynchronously; if it succeeds the row's
		// last_error gets cleared on the next manual Start.
		if failed {
			// Use a fresh context: parent may be canceled.
			ctxBg, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			if perr := m.store.SetLastError(ctxBg, rec.ID, exitMsg); perr != nil {
				m.log.Warn("persist last_error failed", "id", rec.ID, "error", perr)
			}
			m.log.Error("trader engine exited with error", "id", rec.ID, "name", rec.Name, "error", exitMsg)
		} else {
			m.log.Info("trader engine stopped", "id", rec.ID, "name", rec.Name)
		}
	}()
	return nil
}

// buildEngine returns a Run function for the trader described by rec.
// Decoding happens here so a bad config errors synchronously and the
// caller can return a useful message to the UI.
func (m *Manager) buildEngine(rec Record) (func(context.Context) error, error) {
	cfg, err := unmarshalConfig(rec.Type, rec.Config)
	if err != nil {
		return nil, err
	}
	switch rec.Type {
	case typeMM:
		mmCfg, ok := cfg.(mm.SymbolConfig)
		if !ok {
			return nil, fmt.Errorf("internal: mm config has wrong type %T", cfg)
		}
		engineLog := m.log.With("trader_id", rec.ID, "trader_name", rec.Name, "trader_type", "mm")
		engine := mm.NewEngine(mmCfg, mm.NewSpreadStrategy(mmCfg), m.prices, m.obClient, m.pfClient, m.sagaClient, engineLog)
		return engine.Run, nil
	case typeNoise:
		nCfg, ok := cfg.(noise.SymbolConfig)
		if !ok {
			return nil, fmt.Errorf("internal: noise config has wrong type %T", cfg)
		}
		engineLog := m.log.With("trader_id", rec.ID, "trader_name", rec.Name, "trader_type", "noise")
		engine := noise.NewEngine(nCfg, m.prices, m.pfClient, m.sagaClient, m.obClient, engineLog)
		return engine.Run, nil
	default:
		return nil, fmt.Errorf("unknown trader type: %s", rec.Type)
	}
}

func (m *Manager) recordToProto(r Record) (*traderv1.Trader, error) {
	cfgProto, err := unmarshalConfigToProto(r.Type, r.Config)
	if err != nil {
		return nil, err
	}
	status := traderv1.TraderStatus_TRADER_STATUS_STOPPED
	lastErr := r.LastError
	m.mu.Lock()
	if st, ok := m.running[r.ID]; ok {
		if st.failed {
			status = traderv1.TraderStatus_TRADER_STATUS_FAILED
			if st.lastError != "" {
				lastErr = st.lastError
			}
		} else {
			status = traderv1.TraderStatus_TRADER_STATUS_RUNNING
		}
	} else if r.LastError != "" && r.Enabled {
		// Enabled but not running means the goroutine has exited with a
		// recorded error since the last successful start.
		status = traderv1.TraderStatus_TRADER_STATUS_FAILED
	}
	m.mu.Unlock()

	t := &traderv1.Trader{
		Id:        r.ID,
		Name:      r.Name,
		Type:      typeToProto(r.Type),
		Status:    status,
		LastError: lastErr,
		Enabled:   r.Enabled,
		Config:    cfgProto,
	}
	if !r.CreatedAt.IsZero() {
		t.CreatedAt = timestamppb.New(r.CreatedAt)
	}
	if !r.UpdatedAt.IsZero() {
		t.UpdatedAt = timestamppb.New(r.UpdatedAt)
	}
	return t, nil
}

// recordSymbol pulls the symbol field out of a persisted config without a
// full unmarshal — used only for WatchSymbol, where a parse failure means
// startup will fail anyway and the error will surface from the engine.
func recordSymbol(r Record) string {
	var probe struct {
		MM    *struct{ Symbol string } `json:"mm,omitempty"`
		Noise *struct{ Symbol string } `json:"noise,omitempty"`
	}
	if err := json.Unmarshal(r.Config, &probe); err != nil {
		return ""
	}
	if probe.MM != nil {
		return probe.MM.Symbol
	}
	if probe.Noise != nil {
		return probe.Noise.Symbol
	}
	return ""
}

// marshalConfig validates the proto config matches the type tag and returns
// the protojson encoding for storage. JSON preserves field semantics across
// proto upgrades without requiring a migration on every wire change.
func marshalConfig(typeStr string, cfg *traderv1.TraderConfig) (json.RawMessage, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}
	switch typeStr {
	case typeMM:
		if cfg.GetMm() == nil {
			return nil, fmt.Errorf("mm config is required for type=mm")
		}
		if _, err := mmConfigFromProto(cfg.GetMm()); err != nil {
			return nil, err
		}
	case typeNoise:
		if cfg.GetNoise() == nil {
			return nil, fmt.Errorf("noise config is required for type=noise")
		}
		if _, err := noiseConfigFromProto(cfg.GetNoise()); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown type: %s", typeStr)
	}
	b, err := protojson.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	return json.RawMessage(b), nil
}

func unmarshalConfigToProto(typeStr string, raw json.RawMessage) (*traderv1.TraderConfig, error) {
	cfg := &traderv1.TraderConfig{}
	if err := protojson.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	switch typeStr {
	case typeMM:
		if cfg.GetMm() == nil {
			return nil, fmt.Errorf("missing mm config for type=mm")
		}
	case typeNoise:
		if cfg.GetNoise() == nil {
			return nil, fmt.Errorf("missing noise config for type=noise")
		}
	}
	return cfg, nil
}

func unmarshalConfig(typeStr string, raw json.RawMessage) (any, error) {
	cfg, err := unmarshalConfigToProto(typeStr, raw)
	if err != nil {
		return nil, err
	}
	switch typeStr {
	case typeMM:
		return mmConfigFromProto(cfg.GetMm())
	case typeNoise:
		return noiseConfigFromProto(cfg.GetNoise())
	default:
		return nil, fmt.Errorf("unknown type: %s", typeStr)
	}
}
