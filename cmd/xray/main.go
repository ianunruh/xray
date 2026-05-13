package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/pgstore"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://xray:xray@localhost:5432/xray?sslmode=disable"
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	store := pgstore.New(pool)
	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	registry := es.NewRegistry()
	registry.Register("OrderPlaced", func() proto.Message { return new(orderbookv1.OrderPlaced) })
	registry.Register("TradeExecuted", func() proto.Message { return new(orderbookv1.TradeExecuted) })
	registry.Register("OrderCancelled", func() proto.Message { return new(orderbookv1.OrderCancelled) })

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	})

	// Demo scenario: place some orders on AAPL
	fmt.Println("=== Order Book Demo: AAPL ===")
	fmt.Println()

	// Place sell orders (asks)
	for _, order := range []orderbook.PlaceOrder{
		{Symbol: "AAPL", Side: orderbook.Sell, Price: 1505000, Quantity: 100}, // $150.50
		{Symbol: "AAPL", Side: orderbook.Sell, Price: 1510000, Quantity: 50},  // $151.00
		{Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 75},  // $150.00
	} {
		if err := placeOrder(ctx, handler, order); err != nil {
			log.Fatalf("place sell order: %v", err)
		}
	}

	// Place buy orders (bids)
	for _, order := range []orderbook.PlaceOrder{
		{Symbol: "AAPL", Side: orderbook.Buy, Price: 1495000, Quantity: 200}, // $149.50
		{Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 120}, // $150.00 — should match the $150.00 ask
	} {
		if err := placeOrder(ctx, handler, order); err != nil {
			log.Fatalf("place buy order: %v", err)
		}
	}

	// Print resulting event stream
	fmt.Println()
	fmt.Println("=== Event Stream ===")
	raw, err := store.Load(ctx, "orderbook:AAPL")
	if err != nil {
		log.Fatalf("load events: %v", err)
	}

	for i, r := range raw {
		evt, err := registry.Deserialize(r)
		if err != nil {
			log.Fatalf("deserialize: %v", err)
		}
		fmt.Printf("[%d] %s\n", i+1, evt.Type)
		switch data := evt.Data.(type) {
		case *orderbookv1.OrderPlaced:
			fmt.Printf("    Order %s: %s %s %d @ $%.2f\n",
				data.OrderId, sideName(data.Side), data.Symbol, data.Quantity, float64(data.Price)/10000)
		case *orderbookv1.TradeExecuted:
			fmt.Printf("    Trade %s: %d @ $%.2f (buy=%s, sell=%s)\n",
				data.TradeId, data.Quantity, float64(data.Price)/10000, data.BuyOrderId, data.SellOrderId)
		case *orderbookv1.OrderCancelled:
			fmt.Printf("    Cancelled: %s\n", data.OrderId)
		}
	}

	// Print final book state
	fmt.Println()
	fmt.Println("=== Final Book State ===")
	book := orderbook.NewOrderBook("orderbook:AAPL")
	for _, r := range raw {
		evt, _ := registry.Deserialize(r)
		book.Apply(evt)
	}

	fmt.Println("Bids:")
	for _, bid := range book.Bids {
		fmt.Printf("  %d @ $%.2f (remaining: %d)\n", bid.Quantity, float64(bid.Price)/10000, bid.RemainingQty)
	}
	if len(book.Bids) == 0 {
		fmt.Println("  (empty)")
	}

	fmt.Println("Asks:")
	for _, ask := range book.Asks {
		fmt.Printf("  %d @ $%.2f (remaining: %d)\n", ask.Quantity, float64(ask.Price)/10000, ask.RemainingQty)
	}
	if len(book.Asks) == 0 {
		fmt.Println("  (empty)")
	}
}

func placeOrder(ctx context.Context, handler *es.Handler[*orderbook.OrderBook], cmd orderbook.PlaceOrder) error {
	fmt.Printf("Placing %s order: %d @ $%.2f\n", sideNameFromDomain(cmd.Side), cmd.Quantity, float64(cmd.Price)/10000)
	return handler.Handle(ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, cmd)
	})
}

func sideName(s orderbookv1.Side) string {
	switch s {
	case orderbookv1.Side_SIDE_BUY:
		return "BUY"
	case orderbookv1.Side_SIDE_SELL:
		return "SELL"
	default:
		return "UNKNOWN"
	}
}

func sideNameFromDomain(s orderbook.Side) string {
	switch s {
	case orderbook.Buy:
		return "BUY"
	case orderbook.Sell:
		return "SELL"
	default:
		return "UNKNOWN"
	}
}
