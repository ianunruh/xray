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
}

func NewSpreadStrategy(cfg SymbolConfig) *SpreadStrategy {
	return &SpreadStrategy{
		Spread:       cfg.Spread,
		Levels:       cfg.Levels,
		LevelSpacing: cfg.LevelSpacing,
		Quantity:     cfg.Quantity,
	}
}

func (s *SpreadStrategy) ComputeQuotes(refPrice int64, inv InventoryState) []QuoteLevel {
	halfSpread := s.Spread / 2
	var quotes []QuoteLevel

	if inv.Position < inv.MaxPosition {
		for i := range s.Levels {
			offset := halfSpread + int64(i)*s.LevelSpacing
			price := refPrice - offset
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
			price := refPrice + offset
			quotes = append(quotes, QuoteLevel{
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    price,
				Quantity: s.Quantity,
			})
		}
	}

	return quotes
}
