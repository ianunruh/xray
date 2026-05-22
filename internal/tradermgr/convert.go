package tradermgr

import (
	"fmt"
	"time"

	traderv1 "github.com/ianunruh/xray/gen/trader/v1"
	"github.com/ianunruh/xray/internal/mm"
	"github.com/ianunruh/xray/internal/noise"
)

// String tags persisted in the traders.type column. Kept stable so existing
// rows continue to load after future enum reshuffles.
const (
	typeMM    = "mm"
	typeNoise = "noise"
)

func typeFromProto(t traderv1.TraderType) (string, error) {
	switch t {
	case traderv1.TraderType_TRADER_TYPE_MM:
		return typeMM, nil
	case traderv1.TraderType_TRADER_TYPE_NOISE:
		return typeNoise, nil
	default:
		return "", fmt.Errorf("unsupported trader type: %s", t)
	}
}

func typeToProto(t string) traderv1.TraderType {
	switch t {
	case typeMM:
		return traderv1.TraderType_TRADER_TYPE_MM
	case typeNoise:
		return traderv1.TraderType_TRADER_TYPE_NOISE
	default:
		return traderv1.TraderType_TRADER_TYPE_UNSPECIFIED
	}
}

func mmConfigFromProto(p *traderv1.MMConfig) (mm.SymbolConfig, error) {
	if p == nil {
		return mm.SymbolConfig{}, fmt.Errorf("mm config is required")
	}
	out := mm.SymbolConfig{
		Symbol:             p.Symbol,
		AccountID:          p.AccountId,
		InitialDeposit:     p.InitialDeposit,
		InitialShares:      p.InitialShares,
		Spread:             p.Spread,
		Quantity:           p.Quantity,
		Levels:             int(p.Levels),
		LevelSpacing:       p.LevelSpacing,
		MaxPosition:        p.MaxPosition,
		RequoteInterval:    time.Duration(p.RequoteIntervalMs) * time.Millisecond,
		PriceMoveThreshold: p.PriceMoveThreshold,
		MaxSkew:            p.MaxSkew,
	}
	applyMMDefaults(&out)
	if err := validateMM(out); err != nil {
		return mm.SymbolConfig{}, err
	}
	return out, nil
}

func noiseConfigFromProto(p *traderv1.NoiseConfig) (noise.SymbolConfig, error) {
	if p == nil {
		return noise.SymbolConfig{}, fmt.Errorf("noise config is required")
	}
	out := noise.SymbolConfig{
		Symbol:              p.Symbol,
		AccountID:           p.AccountId,
		InitialDeposit:      p.InitialDeposit,
		InitialShares:       p.InitialShares,
		RandomInitialShares: p.RandomInitialShares,
		OrderInterval:       time.Duration(p.OrderIntervalMs) * time.Millisecond,
		MinQuantity:         p.MinQuantity,
		MaxQuantity:         p.MaxQuantity,
		PriceJitter:         p.PriceJitter,
		MarketOrderPct:      p.MarketOrderPct,
		MaxPosition:         p.MaxPosition,
		BuyBias:             p.BuyBias,
	}
	noise.ApplyDefaults(&out)
	if err := validateNoise(out); err != nil {
		return noise.SymbolConfig{}, err
	}
	return out, nil
}

// applyMMDefaults mirrors mm.Config.applyDefaults for the per-symbol
// fields. The YAML loader fills these for CLI configs; we do the same
// for UI-created traders so a partial form submission still runs.
func applyMMDefaults(s *mm.SymbolConfig) {
	if s.Levels == 0 {
		s.Levels = 1
	}
	if s.LevelSpacing == 0 {
		s.LevelSpacing = s.Spread
	}
	if s.RequoteInterval == 0 {
		s.RequoteInterval = 30 * time.Second
	}
}

func validateMM(s mm.SymbolConfig) error {
	if s.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	if s.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	if s.Spread <= 0 {
		return fmt.Errorf("spread must be positive")
	}
	if s.Quantity <= 0 {
		return fmt.Errorf("quantity must be positive")
	}
	if s.MaxPosition <= 0 {
		return fmt.Errorf("max_position must be positive")
	}
	return nil
}

func validateNoise(s noise.SymbolConfig) error {
	if s.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	if s.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	if s.MaxPosition <= 0 {
		return fmt.Errorf("max_position must be positive")
	}
	if s.MarketOrderPct < 0 || s.MarketOrderPct > 1 {
		return fmt.Errorf("market_order_pct must be between 0 and 1")
	}
	if s.BuyBias < 0 || s.BuyBias > 1 {
		return fmt.Errorf("buy_bias must be between 0 and 1")
	}
	return nil
}
