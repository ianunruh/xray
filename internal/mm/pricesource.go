package mm

import (
	"context"
	"time"
)

type PriceSnapshot struct {
	Price     int64
	FetchedAt time.Time
}

type PriceSource interface {
	GetPrice(symbol string) (PriceSnapshot, bool)
	Start(ctx context.Context) error
}
