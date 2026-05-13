package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
)

func main() {
	addr := flag.String("addr", "http://localhost:8080", "Server URL")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	symbolsFlag := flag.String("symbols", "AAPL,GOOG,MSFT", "Comma-separated symbols")
	placeWorkers := flag.Int("place-workers", 4, "Workers placing orders")
	cancelWorkers := flag.Int("cancel-workers", 2, "Workers cancelling orders")
	depthWorkers := flag.Int("depth-workers", 2, "Workers reading market depth")
	orderWorkers := flag.Int("order-workers", 2, "Workers reading order status")
	flag.Parse()

	symbols := strings.Split(*symbolsFlag, ",")
	for i := range symbols {
		symbols[i] = strings.TrimSpace(symbols[i])
	}

	client := orderbookv1connect.NewOrderBookServiceClient(
		&http.Client{},
		*addr,
	)

	tracker := &orderTracker{}
	collector := newStatsCollector()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	fmt.Printf("Starting load test against %s for %s\n", *addr, *duration)
	fmt.Printf("Symbols: %s\n", strings.Join(symbols, ", "))
	fmt.Printf("Workers: place=%d cancel=%d depth=%d order=%d\n",
		*placeWorkers, *cancelWorkers, *depthWorkers, *orderWorkers)
	fmt.Println()

	var wg sync.WaitGroup

	for range *placeWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runPlaceWorker(ctx, client, symbols, tracker, collector)
		}()
	}
	for range *cancelWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runCancelWorker(ctx, client, tracker, collector)
		}()
	}
	for range *depthWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runDepthWorker(ctx, client, symbols, collector)
		}()
	}
	for range *orderWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runOrderWorker(ctx, client, tracker, collector)
		}()
	}

	// Reporter goroutine
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(start).Truncate(time.Second)
				collector.printReport(elapsed)
			}
		}
	}()

	wg.Wait()

	fmt.Println()
	fmt.Println("=== Final Summary ===")
	collector.printFinalSummary()
}

// orderEntry holds a placed order's symbol and ID.
type orderEntry struct {
	symbol  string
	orderID string
}

// orderTracker is a thread-safe collection of placed orders.
type orderTracker struct {
	mu      sync.RWMutex
	entries []orderEntry
}

func (t *orderTracker) add(symbol, orderID string) {
	t.mu.Lock()
	t.entries = append(t.entries, orderEntry{symbol, orderID})
	t.mu.Unlock()
}

func (t *orderTracker) random() (orderEntry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.entries) == 0 {
		return orderEntry{}, false
	}
	return t.entries[rand.IntN(len(t.entries))], true
}

// rpcStats collects latency samples and counts for a single RPC type.
type rpcStats struct {
	requests atomic.Int64
	errors   atomic.Int64

	mu       sync.Mutex
	samples  []time.Duration
	totalAll atomic.Int64
	errAll   atomic.Int64
	allMin   time.Duration
	allMax   time.Duration
	allSum   time.Duration
	allMu    sync.Mutex
}

func (s *rpcStats) record(d time.Duration, err error) {
	s.requests.Add(1)
	s.totalAll.Add(1)
	if err != nil {
		s.errors.Add(1)
		s.errAll.Add(1)
	}

	s.mu.Lock()
	s.samples = append(s.samples, d)
	s.mu.Unlock()

	s.allMu.Lock()
	s.allSum += d
	if s.allMin == 0 || d < s.allMin {
		s.allMin = d
	}
	if d > s.allMax {
		s.allMax = d
	}
	s.allMu.Unlock()
}

// snapshot takes the current interval samples and resets them.
func (s *rpcStats) snapshot() (reqs, errs int64, samples []time.Duration) {
	reqs = s.requests.Swap(0)
	errs = s.errors.Swap(0)

	s.mu.Lock()
	samples = s.samples
	s.samples = nil
	s.mu.Unlock()
	return
}

type statsCollector struct {
	rpcs map[string]*rpcStats
}

func newStatsCollector() *statsCollector {
	return &statsCollector{
		rpcs: map[string]*rpcStats{
			"PlaceOrder":     {},
			"CancelOrder":    {},
			"GetMarketDepth": {},
			"GetOrder":       {},
		},
	}
}

func (c *statsCollector) record(name string, d time.Duration, err error) {
	c.rpcs[name].record(d, err)
}

func (c *statsCollector) printReport(elapsed time.Duration) {
	names := []string{"PlaceOrder", "CancelOrder", "GetMarketDepth", "GetOrder"}
	var parts []string
	for _, name := range names {
		s := c.rpcs[name]
		reqs, errs, samples := s.snapshot()
		if reqs == 0 {
			continue
		}
		p50, p99 := percentiles(samples)
		parts = append(parts, fmt.Sprintf("%s: %d req, %d err, p50=%s p99=%s",
			name, reqs, errs, fmtDur(p50), fmtDur(p99)))
	}
	if len(parts) > 0 {
		fmt.Printf("[%s] %s\n", elapsed, strings.Join(parts, " | "))
	}
}

func (c *statsCollector) printFinalSummary() {
	names := []string{"PlaceOrder", "CancelOrder", "GetMarketDepth", "GetOrder"}
	fmt.Printf("%-16s %10s %10s %10s %10s %10s\n", "RPC", "Requests", "Errors", "Min", "Avg", "Max")
	fmt.Println(strings.Repeat("-", 68))
	for _, name := range names {
		s := c.rpcs[name]
		total := s.totalAll.Load()
		errs := s.errAll.Load()

		s.allMu.Lock()
		min := s.allMin
		max := s.allMax
		sum := s.allSum
		s.allMu.Unlock()

		var avg time.Duration
		if total > 0 {
			avg = sum / time.Duration(total)
		}
		fmt.Printf("%-16s %10d %10d %10s %10s %10s\n",
			name, total, errs, fmtDur(min), fmtDur(avg), fmtDur(max))
	}
}

func percentiles(samples []time.Duration) (p50, p99 time.Duration) {
	if len(samples) == 0 {
		return 0, 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p50 = samples[len(samples)*50/100]
	p99 = samples[len(samples)*99/100]
	return
}

func fmtDur(d time.Duration) string {
	switch {
	case d == 0:
		return "-"
	case d < time.Millisecond:
		return fmt.Sprintf("%.0fus", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.2fs", float64(d)/float64(time.Second))
	}
}

func runPlaceWorker(ctx context.Context, client orderbookv1connect.OrderBookServiceClient, symbols []string, tracker *orderTracker, stats *statsCollector) {
	sides := []orderbookv1.Side{orderbookv1.Side_SIDE_BUY, orderbookv1.Side_SIDE_SELL}
	for ctx.Err() == nil {
		symbol := symbols[rand.IntN(len(symbols))]
		side := sides[rand.IntN(len(sides))]
		price := int64(1480000 + rand.IntN(40001)) // $148.00 - $152.00
		quantity := int64(1 + rand.IntN(100))

		req := connect.NewRequest(&orderbookv1.PlaceOrderRequest{
			Symbol:      symbol,
			Side:        side,
			Price:       price,
			Quantity:    quantity,
			OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
			TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
		})

		start := time.Now()
		resp, err := client.PlaceOrder(ctx, req)
		d := time.Since(start)
		stats.record("PlaceOrder", d, err)

		if err == nil {
			tracker.add(symbol, resp.Msg.OrderId)
		}
	}
}

func runCancelWorker(ctx context.Context, client orderbookv1connect.OrderBookServiceClient, tracker *orderTracker, stats *statsCollector) {
	for ctx.Err() == nil {
		entry, ok := tracker.random()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}

		req := connect.NewRequest(&orderbookv1.CancelOrderRequest{
			Symbol:  entry.symbol,
			OrderId: entry.orderID,
		})

		start := time.Now()
		_, err := client.CancelOrder(ctx, req)
		d := time.Since(start)

		if err != nil && connect.CodeOf(err) == connect.CodeNotFound {
			err = nil
		}
		stats.record("CancelOrder", d, err)
	}
}

func runDepthWorker(ctx context.Context, client orderbookv1connect.OrderBookServiceClient, symbols []string, stats *statsCollector) {
	for ctx.Err() == nil {
		symbol := symbols[rand.IntN(len(symbols))]

		req := connect.NewRequest(&orderbookv1.GetMarketDepthRequest{
			Symbol: symbol,
			Depth:  5,
		})

		start := time.Now()
		_, err := client.GetMarketDepth(ctx, req)
		d := time.Since(start)
		stats.record("GetMarketDepth", d, err)

		time.Sleep(time.Millisecond)
	}
}

func runOrderWorker(ctx context.Context, client orderbookv1connect.OrderBookServiceClient, tracker *orderTracker, stats *statsCollector) {
	for ctx.Err() == nil {
		entry, ok := tracker.random()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}

		req := connect.NewRequest(&orderbookv1.GetOrderRequest{
			Symbol:  entry.symbol,
			OrderId: entry.orderID,
		})

		start := time.Now()
		_, err := client.GetOrder(ctx, req)
		d := time.Since(start)

		if err != nil && connect.CodeOf(err) == connect.CodeNotFound {
			err = nil
		}
		stats.record("GetOrder", d, err)

		time.Sleep(time.Millisecond)
	}
}

