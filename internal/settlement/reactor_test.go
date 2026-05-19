package settlement_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/internal/settlement"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

// fakeReader is a minimal in-memory PendingSettlementsReader keyed
// by (accountID, tradeID, kind). The settlement reactor walks it the
// same way the PG-backed reader would.
type fakeReader struct {
	rows []portfolio.PendingLegRow
}

func (f *fakeReader) add(row portfolio.PendingLegRow) {
	f.rows = append(f.rows, row)
}

func (f *fakeReader) remove(accountID, tradeID string, kind portfoliov1.SettlementLegKind) {
	out := f.rows[:0]
	for _, r := range f.rows {
		if r.AccountID == accountID && r.TradeID == tradeID && r.Kind == kind {
			continue
		}
		out = append(out, r)
	}
	f.rows = out
}

func (f *fakeReader) AccountsWithDueSettlements(_ context.Context, before time.Time) ([]string, error) {
	seen := make(map[string]struct{})
	var out []string
	for _, r := range f.rows {
		if r.SettlesAt.After(before) {
			continue
		}
		if _, ok := seen[r.AccountID]; ok {
			continue
		}
		seen[r.AccountID] = struct{}{}
		out = append(out, r.AccountID)
	}
	return out, nil
}

func (f *fakeReader) DueLegs(_ context.Context, accountID string, before time.Time) ([]portfolio.PendingLegRow, error) {
	var out []portfolio.PendingLegRow
	for _, r := range f.rows {
		if r.AccountID != accountID {
			continue
		}
		if r.SettlesAt.After(before) {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeReader) PendingTotals(_ context.Context, accountID string) (portfolio.PendingTotals, error) {
	var totals portfolio.PendingTotals
	for _, r := range f.rows {
		if r.AccountID != accountID {
			continue
		}
		if r.CashAmount > 0 {
			totals.Credits += r.CashAmount
		} else if r.CashAmount < 0 {
			totals.Debits += -r.CashAmount
		}
	}
	return totals, nil
}

func (f *fakeReader) PendingSharesBySymbol(_ context.Context, accountID string) (map[string]int64, error) {
	out := make(map[string]int64)
	for _, r := range f.rows {
		if r.AccountID != accountID || r.Quantity <= 0 {
			continue
		}
		out[r.Symbol] += r.Quantity
	}
	return out, nil
}

// fakeReaderSubscribed wraps fakeReader and removes a leg from the
// fake row list when the reactor's ClearSettlement succeeds. The
// reactor talks to the aggregate via the handler; the projection
// would normally drop the row on the resulting SettlementCleared
// event. Tests stitch the two manually since there's no NATS bus.
type subscribingReader struct {
	*fakeReader
	handler *es.Handler[*portfolio.Portfolio]
	t       *testing.T
}

func newTestEnv(t *testing.T) (*es.Handler[*portfolio.Portfolio], *subscribingReader, *settlement.Reactor, context.Context) {
	registry := es.NewRegistry()
	portfolio.RegisterEvents(registry)
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	reader := &subscribingReader{
		fakeReader: &fakeReader{},
		handler:    handler,
		t:          t,
	}
	r := settlement.New(handler, reader, time.Now, settlement.Config{Interval: time.Second}, slog.Default())
	return handler, reader, r, context.Background()
}

// stampedEvent injects a SharesSettled event with a future settles_at
// and also adds the matching row to the fake reader so the reactor
// will pick it up.
func injectDeferredSale(t *testing.T, handler *es.Handler[*portfolio.Portfolio], reader *subscribingReader, accountID, tradeID string, proceeds int64, settlesAt time.Time) {
	t.Helper()
	tradeDate := settlesAt.Add(-24 * time.Hour)
	evt := es.Event{
		AggregateID: portfolio.AggregateID(accountID),
		Type:        portfolio.EventSharesSettled,
		Timestamp:   tradeDate,
		Data: &portfoliov1.SharesSettled{
			AccountId:     accountID,
			OrderSagaId:   "saga-" + tradeID,
			TradeId:       tradeID,
			Symbol:        "AAPL",
			Quantity:      100,
			PricePerShare: proceeds / 100,
			Proceeds:      proceeds,
			SettledAt:     timestamppb.New(tradeDate),
			SettlesAt:     timestamppb.New(settlesAt),
		},
	}
	require.NoError(t, handler.Handle(context.Background(), injectCmd{id: evt.AggregateID}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		if err := p.Apply(evt); err != nil {
			return nil, err
		}
		return []es.Event{evt}, nil
	}))
	reader.add(portfolio.PendingLegRow{
		AccountID:   accountID,
		OrderSagaID: "saga-" + tradeID,
		TradeID:     tradeID,
		Kind:        portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_CREDIT,
		Symbol:      "AAPL",
		CashAmount:  proceeds,
		SettlesAt:   settlesAt,
	})
}

type injectCmd struct{ id string }

func (c injectCmd) AggregateID() string { return c.id }

func deposit(t *testing.T, h *es.Handler[*portfolio.Portfolio], accountID string, amount int64) {
	t.Helper()
	cmd := portfolio.DepositCash{AccountID: accountID, Amount: amount}
	require.NoError(t, h.Handle(context.Background(), cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, cmd)
	}))
}

func TestReactor_NoDueLegs_NoOp(t *testing.T) {
	_, _, r, ctx := newTestEnv(t)
	require.NoError(t, r.Tick(ctx, time.Now()))
	s := r.Status()
	assert.Equal(t, 0, s.LastTickAccounts)
	assert.Equal(t, 0, s.LastTickCleared)
}

func TestReactor_ClearsDueLeg(t *testing.T) {
	handler, reader, r, ctx := newTestEnv(t)
	deposit(t, handler, "acct-1", 100_000_000)
	past := time.Now().Add(-time.Hour)
	injectDeferredSale(t, handler, reader, "acct-1", "trade-1", 5_000_000, past)

	require.NoError(t, r.Tick(ctx, time.Now()))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(105_000_000), p.SettledCash, "due leg should clear into settled cash")
	assert.Empty(t, p.PendingLegs)

	// Reader still has the row (no projection in the loop). In production
	// the SettlementCleared event would drop it via the projection
	// consumer; for the test we simulate that, then re-tick to confirm
	// no double-clear.
	reader.remove("acct-1", "trade-1", portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_CREDIT)
	require.NoError(t, r.Tick(ctx, time.Now()))
	p, err = handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(105_000_000), p.SettledCash, "re-tick must not move settled cash again")
	status := r.Status()
	assert.Equal(t, 0, status.LastTickAccounts, "no accounts due on re-tick")
}

func TestReactor_SkipsFutureLegs(t *testing.T) {
	handler, reader, r, ctx := newTestEnv(t)
	deposit(t, handler, "acct-1", 100_000_000)
	future := time.Now().Add(time.Hour)
	injectDeferredSale(t, handler, reader, "acct-1", "trade-1", 5_000_000, future)

	require.NoError(t, r.Tick(ctx, time.Now()))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(100_000_000), p.SettledCash, "future leg should not clear")
	assert.Len(t, p.PendingLegs, 1)
}

func TestReactor_Idempotent_AlreadyCleared(t *testing.T) {
	// A leg that's still in the reader's view but has already been
	// cleared on the aggregate should produce no extra effect — the
	// applier short-circuits when the leg is missing.
	handler, reader, r, ctx := newTestEnv(t)
	deposit(t, handler, "acct-1", 100_000_000)
	past := time.Now().Add(-time.Hour)
	injectDeferredSale(t, handler, reader, "acct-1", "trade-1", 5_000_000, past)

	require.NoError(t, r.Tick(ctx, time.Now()))
	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(105_000_000), p.SettledCash)

	// Second tick with stale row still in reader: clear is a no-op
	// because the leg is already gone on the aggregate.
	require.NoError(t, r.Tick(ctx, time.Now()))
	p, err = handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(105_000_000), p.SettledCash, "duplicate clear must be no-op")
}
