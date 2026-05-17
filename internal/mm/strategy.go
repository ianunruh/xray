package mm

import orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"

type QuoteLevel struct {
	Side     orderbookv1.Side
	Price    int64
	Quantity int64
}

type InventoryState struct {
	Position    int64
	MaxPosition int64
}

type Strategy interface {
	ComputeQuotes(refPrice int64, inventory InventoryState) []QuoteLevel
}

type SpreadStrategy struct {
	Spread       int64
	Levels       int
	LevelSpacing int64
	Quantity     int64
	MaxSkew      int64
}

func NewSpreadStrategy(cfg SymbolConfig) *SpreadStrategy {
	return &SpreadStrategy{
		Spread:       cfg.Spread,
		Levels:       cfg.Levels,
		LevelSpacing: cfg.LevelSpacing,
		Quantity:     cfg.Quantity,
		MaxSkew:      cfg.MaxSkew,
	}
}

func (s *SpreadStrategy) ComputeQuotes(refPrice int64, inv InventoryState) []QuoteLevel {
	// Long position shifts mid down (cheapen asks, lower bids → encourage
	// selling); short position shifts mid up. Scales linearly with
	// position/MaxPosition.
	mid := refPrice
	if s.MaxSkew > 0 && inv.MaxPosition > 0 {
		mid = refPrice - (inv.Position*s.MaxSkew)/inv.MaxPosition
	}

	halfSpread := s.Spread / 2
	var quotes []QuoteLevel

	if inv.Position < inv.MaxPosition {
		for i := range s.Levels {
			offset := halfSpread + int64(i)*s.LevelSpacing
			price := mid - offset
			if price <= 0 {
				continue
			}
			quotes = append(quotes, QuoteLevel{
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    price,
				Quantity: s.Quantity,
			})
		}
	}

	if inv.Position > -inv.MaxPosition {
		for i := range s.Levels {
			offset := halfSpread + int64(i)*s.LevelSpacing
			price := mid + offset
			quotes = append(quotes, QuoteLevel{
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    price,
				Quantity: s.Quantity,
			})
		}
	}

	return quotes
}
