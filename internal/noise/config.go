package noise

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ianunruh/xray/internal/pricesource"
)

type Config struct {
	ServerURL    string                  `yaml:"server_url"`
	PolygonKey   string                  `yaml:"polygon_api_key"`
	LogLevel     string                  `yaml:"log_level"`
	Symbols      []SymbolConfig          `yaml:"symbols"`
	Polygon      pricesource.PolygonConfig `yaml:"polygon"`
	PriceSource  string                  `yaml:"price_source"`
	StaticPrices map[string]int64        `yaml:"static_prices"`
}

type SymbolConfig struct {
	Symbol         string        `yaml:"symbol"`
	AccountID      string        `yaml:"account_id"`
	InitialDeposit int64         `yaml:"initial_deposit"`
	InitialShares  int64         `yaml:"initial_shares"`
	OrderInterval  time.Duration `yaml:"order_interval"`
	MinQuantity    int64         `yaml:"min_quantity"`
	MaxQuantity    int64         `yaml:"max_quantity"`
	PriceJitter    int64         `yaml:"price_jitter"`
	MarketOrderPct float64       `yaml:"market_order_pct"`
	MaxPosition    int64         `yaml:"max_position"`
	BuyBias        float64       `yaml:"buy_bias"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	cfg.applyEnv()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) applyEnv() {
	if v := os.Getenv("POLYGON_API_KEY"); v != "" && c.PolygonKey == "" {
		c.PolygonKey = v
	}
}

func (c *Config) applyDefaults() {
	if c.ServerURL == "" {
		c.ServerURL = "http://localhost:8080"
	}
	if c.PriceSource == "" {
		c.PriceSource = "polygon"
	}
	if c.Polygon.BaseURL == "" {
		c.Polygon.BaseURL = "https://api.polygon.io"
	}
	if c.Polygon.PollInterval == 0 {
		c.Polygon.PollInterval = 30 * time.Second
	}
	for i := range c.Symbols {
		s := &c.Symbols[i]
		if s.OrderInterval == 0 {
			s.OrderInterval = 5 * time.Second
		}
		if s.MinQuantity == 0 {
			s.MinQuantity = 1
		}
		if s.MaxQuantity == 0 {
			s.MaxQuantity = s.MinQuantity
		}
		if s.BuyBias == 0 {
			s.BuyBias = 0.5
		}
	}
}

func (c *Config) validate() error {
	if len(c.Symbols) == 0 {
		return errors.New("at least one symbol is required")
	}
	if c.PriceSource != "polygon" && c.PriceSource != "static" {
		return fmt.Errorf("unknown price_source: %q", c.PriceSource)
	}
	if c.PriceSource == "polygon" && c.PolygonKey == "" {
		return errors.New("polygon_api_key is required when price_source is polygon")
	}
	for i, s := range c.Symbols {
		if s.Symbol == "" {
			return fmt.Errorf("symbols[%d]: symbol is required", i)
		}
		if s.AccountID == "" {
			return fmt.Errorf("symbols[%d]: account_id is required", i)
		}
		if s.MaxPosition <= 0 {
			return fmt.Errorf("symbols[%d]: max_position must be positive", i)
		}
		if s.MarketOrderPct < 0 || s.MarketOrderPct > 1 {
			return fmt.Errorf("symbols[%d]: market_order_pct must be between 0 and 1", i)
		}
		if s.BuyBias < 0 || s.BuyBias > 1 {
			return fmt.Errorf("symbols[%d]: buy_bias must be between 0 and 1", i)
		}
		if c.PriceSource == "static" {
			if _, ok := c.StaticPrices[s.Symbol]; !ok {
				return fmt.Errorf("symbols[%d]: no static price for %q", i, s.Symbol)
			}
		}
	}
	return nil
}
