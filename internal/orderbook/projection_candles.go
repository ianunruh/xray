package orderbook

import (
	"context"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

var allIntervals = []orderbookv1.CandleInterval{
	orderbookv1.CandleInterval_CANDLE_INTERVAL_1M,
	orderbookv1.CandleInterval_CANDLE_INTERVAL_5M,
	orderbookv1.CandleInterval_CANDLE_INTERVAL_15M,
	orderbookv1.CandleInterval_CANDLE_INTERVAL_1H,
	orderbookv1.CandleInterval_CANDLE_INTERVAL_1D,
}

type candleKey struct {
	symbol   string
	interval orderbookv1.CandleInterval
	openTime time.Time
}

type candle struct {
	open      int64
	high      int64
	low       int64
	close     int64
	volume    int64
	closeTime time.Time
}

type CandleProjection struct {
	mu      sync.RWMutex
	candles map[candleKey]*candle
}

func NewCandleProjection() *CandleProjection {
	return &CandleProjection{
		candles: make(map[candleKey]*candle),
	}
}

func (p *CandleProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, evt := range events {
		data, ok := evt.Data.(*orderbookv1.TradeExecuted)
		if !ok {
			continue
		}

		executedAt := data.ExecutedAt.AsTime()

		for _, interval := range allIntervals {
			key := candleKey{
				symbol:   data.Symbol,
				interval: interval,
				openTime: truncateToInterval(executedAt, interval),
			}

			c := p.candles[key]
			if c == nil {
				c = &candle{
					open:      data.Price,
					high:      data.Price,
					low:       data.Price,
					close:     data.Price,
					volume:    data.Quantity,
					closeTime: executedAt,
				}
				p.candles[key] = c
				continue
			}

			if data.Price > c.high {
				c.high = data.Price
			}
			if data.Price < c.low {
				c.low = data.Price
			}
			if !executedAt.Before(c.closeTime) {
				c.close = data.Price
				c.closeTime = executedAt
			}
			c.volume += data.Quantity
		}
	}

	return nil
}

func (p *CandleProjection) GetCandles(symbol string, interval orderbookv1.CandleInterval, from, to time.Time) []*orderbookv1.Candle {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var out []*orderbookv1.Candle
	for key, c := range p.candles {
		if key.symbol != symbol || key.interval != interval {
			continue
		}
		if key.openTime.Before(from) || key.openTime.After(to) {
			continue
		}
		out = append(out, toProtoCandle(key, c))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].OpenTime.AsTime().Before(out[j].OpenTime.AsTime())
	})

	return out
}

func (p *CandleProjection) GetLatestCandle(symbol string, interval orderbookv1.CandleInterval) *orderbookv1.Candle {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var latestKey candleKey
	var latestCandle *candle
	for key, c := range p.candles {
		if key.symbol != symbol || key.interval != interval {
			continue
		}
		if latestCandle == nil || key.openTime.After(latestKey.openTime) {
			latestKey = key
			latestCandle = c
		}
	}

	if latestCandle == nil {
		return nil
	}

	return toProtoCandle(latestKey, latestCandle)
}

func toProtoCandle(key candleKey, c *candle) *orderbookv1.Candle {
	return &orderbookv1.Candle{
		Symbol:   key.symbol,
		Interval: key.interval,
		OpenTime: timestamppb.New(key.openTime),
		Open:     c.open,
		High:     c.high,
		Low:      c.low,
		Close:    c.close,
		Volume:   c.volume,
	}
}

func truncateToInterval(t time.Time, interval orderbookv1.CandleInterval) time.Time {
	switch interval {
	case orderbookv1.CandleInterval_CANDLE_INTERVAL_1M:
		return t.Truncate(time.Minute)
	case orderbookv1.CandleInterval_CANDLE_INTERVAL_5M:
		return t.Truncate(5 * time.Minute)
	case orderbookv1.CandleInterval_CANDLE_INTERVAL_15M:
		return t.Truncate(15 * time.Minute)
	case orderbookv1.CandleInterval_CANDLE_INTERVAL_1H:
		return t.Truncate(time.Hour)
	case orderbookv1.CandleInterval_CANDLE_INTERVAL_1D:
		return t.Truncate(24 * time.Hour)
	default:
		return t.Truncate(time.Minute)
	}
}
