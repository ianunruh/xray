package pricesource

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"
)

type PolygonConfig struct {
	BaseURL      string        `yaml:"base_url"`
	PollInterval time.Duration `yaml:"poll_interval"`
}

type polygonPrevResponse struct {
	Results []struct {
		C float64 `json:"c"`
	} `json:"results"`
}

type PolygonPriceSource struct {
	apiKey       string
	baseURL      string
	pollInterval time.Duration
	httpClient   *http.Client
	log          *slog.Logger

	mu      sync.RWMutex
	prices  map[string]PriceSnapshot
	symbols map[string]struct{}
}

func NewPolygonPriceSource(cfg PolygonConfig, apiKey string, symbols []string, log *slog.Logger) *PolygonPriceSource {
	s := make(map[string]struct{}, len(symbols))
	for _, sym := range symbols {
		s[sym] = struct{}{}
	}
	return &PolygonPriceSource{
		apiKey:       apiKey,
		baseURL:      cfg.BaseURL,
		pollInterval: cfg.PollInterval,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		log:          log,
		prices:       make(map[string]PriceSnapshot),
		symbols:      s,
	}
}

// WatchSymbol adds a symbol to the polling set. Safe to call after Start;
// the next poll tick will include it. No-op if already watched.
func (p *PolygonPriceSource) WatchSymbol(symbol string) {
	p.mu.Lock()
	p.symbols[symbol] = struct{}{}
	p.mu.Unlock()
}

func (p *PolygonPriceSource) GetPrice(symbol string) (PriceSnapshot, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	snap, ok := p.prices[symbol]
	return snap, ok
}

func (p *PolygonPriceSource) Start(ctx context.Context) error {
	p.fetchAll(ctx)

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.fetchAll(ctx)
		}
	}
}

func (p *PolygonPriceSource) fetchAll(ctx context.Context) {
	p.mu.RLock()
	symbols := make([]string, 0, len(p.symbols))
	for sym := range p.symbols {
		symbols = append(symbols, sym)
	}
	p.mu.RUnlock()

	for _, symbol := range symbols {
		if ctx.Err() != nil {
			return
		}
		p.fetchSymbol(ctx, symbol)
	}
}

func (p *PolygonPriceSource) fetchSymbol(ctx context.Context, symbol string) {
	url := fmt.Sprintf("%s/v2/aggs/ticker/%s/prev?apiKey=%s", p.baseURL, symbol, p.apiKey)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		p.log.Error("polygon: failed to create request", "symbol", symbol, "error", err)
		return
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		p.log.Error("polygon: request failed", "symbol", symbol, "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.log.Error("polygon: non-200 status", "symbol", symbol, "status", resp.StatusCode)
		return
	}

	var body polygonPrevResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		p.log.Error("polygon: decode failed", "symbol", symbol, "error", err)
		return
	}

	if len(body.Results) == 0 {
		p.log.Warn("polygon: no results", "symbol", symbol)
		return
	}

	priceInt := int64(math.Round(body.Results[0].C * 10000))

	p.mu.Lock()
	p.prices[symbol] = PriceSnapshot{Price: priceInt, FetchedAt: time.Now()}
	p.mu.Unlock()

	p.log.Debug("polygon: updated price", "symbol", symbol, "price", priceInt)
}
