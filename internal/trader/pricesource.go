package trader

import (
	"log/slog"

	"github.com/ianunruh/xray/internal/pricesource"
)

type PriceSourceConfig struct {
	PriceSource  string                    `yaml:"price_source"`
	PolygonKey   string                    `yaml:"polygon_api_key"`
	Polygon      pricesource.PolygonConfig `yaml:"polygon"`
	StaticPrices map[string]int64          `yaml:"static_prices"`
}

func SetupPriceSource(cfg PriceSourceConfig, symbols []string, log *slog.Logger) pricesource.PriceSource {
	switch cfg.PriceSource {
	case "static":
		return pricesource.NewStaticPriceSource(cfg.StaticPrices)
	default:
		return pricesource.NewPolygonPriceSource(cfg.Polygon, cfg.PolygonKey, symbols, log)
	}
}
