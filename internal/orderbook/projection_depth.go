package orderbook

import (
	"context"
	"sort"
	"sync"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

type depthOrder struct {
	symbol       string
	side         orderbookv1.Side
	price        int64
	remainingQty int64
}

type depthLevel struct {
	quantity   int64
	orderCount int32
}

// depthPrice rounds a price (4 implied decimals) to 2 decimal places for
// depth aggregation. E.g. 1505012 ($150.5012) → 1505000 ($150.50).
func depthPrice(price int64) int64 {
	return price / 100 * 100
}

// DepthProjection maintains aggregated price levels per symbol, updated
// incrementally from order lifecycle events. Prices are rounded to 2 decimal
// places for aggregation.
type DepthProjection struct {
	mu           sync.RWMutex
	orders       map[string]*depthOrder           // orderID -> tracked order
	pendingStops map[string]*depthOrder           // stop orders awaiting trigger
	bids         map[string]map[int64]*depthLevel // symbol -> price -> level
	asks         map[string]map[int64]*depthLevel // symbol -> price -> level
}

// NewDepthProjection creates a new DepthProjection.
func NewDepthProjection() *DepthProjection {
	return &DepthProjection{
		orders:       make(map[string]*depthOrder),
		pendingStops: make(map[string]*depthOrder),
		bids:         make(map[string]map[int64]*depthLevel),
		asks:         make(map[string]map[int64]*depthLevel),
	}
}

// HandleEvents processes events to maintain aggregated depth.
func (p *DepthProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.OrderPlaced:
			p.applyOrderPlaced(data)
		case *orderbookv1.TradeExecuted:
			p.applyTradeExecuted(data)
		case *orderbookv1.OrderCancelled:
			p.applyOrderCancelled(data)
		case *orderbookv1.StopTriggered:
			p.applyStopTriggered(data)
		}
	}

	return nil
}

func (p *DepthProjection) applyOrderPlaced(data *orderbookv1.OrderPlaced) {
	if data.OrderType == orderbookv1.OrderType_ORDER_TYPE_STOP_MARKET ||
		data.OrderType == orderbookv1.OrderType_ORDER_TYPE_STOP_LIMIT {
		p.pendingStops[data.OrderId] = &depthOrder{
			symbol:       data.Symbol,
			side:         data.Side,
			price:        depthPrice(data.Price),
			remainingQty: data.Quantity,
		}
		return
	}

	price := depthPrice(data.Price)
	p.orders[data.OrderId] = &depthOrder{
		symbol:       data.Symbol,
		side:         data.Side,
		price:        price,
		remainingQty: data.Quantity,
	}

	levels := p.levelsFor(data.Symbol, data.Side)
	lvl := levels[price]
	if lvl == nil {
		lvl = &depthLevel{}
		levels[price] = lvl
	}
	lvl.quantity += data.Quantity
	lvl.orderCount++
}

func (p *DepthProjection) applyTradeExecuted(data *orderbookv1.TradeExecuted) {
	for _, orderID := range []string{data.BuyOrderId, data.SellOrderId} {
		o := p.orders[orderID]
		if o == nil {
			continue
		}

		levels := p.levelsFor(o.symbol, o.side)
		lvl := levels[o.price]
		if lvl == nil {
			continue
		}

		lvl.quantity -= data.Quantity
		o.remainingQty -= data.Quantity

		if o.remainingQty <= 0 {
			lvl.orderCount--
			if lvl.orderCount <= 0 {
				delete(levels, o.price)
			}
			delete(p.orders, orderID)
		}
	}
}

func (p *DepthProjection) applyOrderCancelled(data *orderbookv1.OrderCancelled) {
	if _, ok := p.pendingStops[data.OrderId]; ok {
		delete(p.pendingStops, data.OrderId)
		return
	}

	o := p.orders[data.OrderId]
	if o == nil {
		return
	}

	levels := p.levelsFor(o.symbol, o.side)
	lvl := levels[o.price]
	if lvl != nil {
		lvl.quantity -= o.remainingQty
		lvl.orderCount--
		if lvl.orderCount <= 0 {
			delete(levels, o.price)
		}
	}

	delete(p.orders, data.OrderId)
}

func (p *DepthProjection) applyStopTriggered(data *orderbookv1.StopTriggered) {
	pending, ok := p.pendingStops[data.OrderId]
	if !ok {
		return
	}
	delete(p.pendingStops, data.OrderId)

	// Only add to depth if activated as limit (it may rest on the book).
	// Stop-market activates as IOC — fills immediately and remainder is cancelled.
	if data.ActivatedAs != orderbookv1.OrderType_ORDER_TYPE_LIMIT {
		return
	}

	p.orders[data.OrderId] = pending
	levels := p.levelsFor(pending.symbol, pending.side)
	lvl := levels[pending.price]
	if lvl == nil {
		lvl = &depthLevel{}
		levels[pending.price] = lvl
	}
	lvl.quantity += pending.remainingQty
	lvl.orderCount++
}

func (p *DepthProjection) levelsFor(symbol string, side orderbookv1.Side) map[int64]*depthLevel {
	var m map[string]map[int64]*depthLevel
	if side == orderbookv1.Side_SIDE_BUY {
		m = p.bids
	} else {
		m = p.asks
	}

	levels := m[symbol]
	if levels == nil {
		levels = make(map[int64]*depthLevel)
		m[symbol] = levels
	}
	return levels
}

// GetDepth returns aggregated price levels for the given symbol. Bids are
// sorted highest-first, asks lowest-first. If depth > 0, at most that many
// levels per side are returned.
func (p *DepthProjection) GetDepth(symbol string, depth int32) (bids, asks []*orderbookv1.PriceLevel) {
	// Snapshot the raw data under the read lock (O(n) copy).
	p.mu.RLock()
	bidSnap := snapshotLevels(p.bids[symbol])
	askSnap := snapshotLevels(p.asks[symbol])
	p.mu.RUnlock()

	// Sort and build proto objects outside the lock.
	bids = buildLevels(bidSnap, depth, true)
	asks = buildLevels(askSnap, depth, false)
	return bids, asks
}

type levelSnapshot struct {
	price      int64
	quantity   int64
	orderCount int32
}

func snapshotLevels(levels map[int64]*depthLevel) []levelSnapshot {
	if len(levels) == 0 {
		return nil
	}
	out := make([]levelSnapshot, 0, len(levels))
	for price, lvl := range levels {
		out = append(out, levelSnapshot{
			price:      price,
			quantity:   lvl.quantity,
			orderCount: lvl.orderCount,
		})
	}
	return out
}

func buildLevels(snap []levelSnapshot, depth int32, descending bool) []*orderbookv1.PriceLevel {
	if len(snap) == 0 {
		return nil
	}

	if descending {
		sort.Slice(snap, func(i, j int) bool { return snap[i].price > snap[j].price })
	} else {
		sort.Slice(snap, func(i, j int) bool { return snap[i].price < snap[j].price })
	}

	if depth > 0 && int(depth) < len(snap) {
		snap = snap[:depth]
	}

	out := make([]*orderbookv1.PriceLevel, len(snap))
	for i, s := range snap {
		out[i] = &orderbookv1.PriceLevel{
			Price:      s.price,
			Quantity:   s.quantity,
			OrderCount: s.orderCount,
		}
	}
	return out
}
