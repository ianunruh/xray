package pricesource

import (
	"context"
	"time"
)

type StaticPriceSource struct {
	prices map[string]PriceSnapshot
}

func NewStaticPriceSource(prices map[string]int64) *StaticPriceSource {
	m := make(map[string]PriceSnapshot, len(prices))
	now := time.Now()
	for sym, p := range prices {
		m[sym] = PriceSnapshot{Price: p, FetchedAt: now}
	}
	return &StaticPriceSource{prices: m}
}

func (s *StaticPriceSource) GetPrice(symbol string) (PriceSnapshot, bool) {
	snap, ok := s.prices[symbol]
	return snap, ok
}

func (s *StaticPriceSource) Start(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
