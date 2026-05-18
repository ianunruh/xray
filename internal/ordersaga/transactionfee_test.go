package ordersaga_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/margin"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

func withTxnFeeBps(t *testing.T, bps int64) {
	t.Helper()
	prev := margin.TxnFeeBps
	margin.TxnFeeBps = bps
	t.Cleanup(func() { margin.TxnFeeBps = prev })
}

// Full happy-path lifecycle with transaction fees enabled — the
// buying account pays a fee against its cash and the saga's
// FeesPaid mirrors the per-fill TransactionFeeCharged.amount. Resting
// liquidity here is placed as a bare orderbook order (not a saga), so
// the seller's side isn't observed; the buyer-side accounting is the
// invariant we care about for this integration test.
func TestReactor_TransactionFee_FullLifecycle(t *testing.T) {
	withTxnFeeBps(t, 10)
	env := setupReactorTest(t)

	const notional = int64(150_000_000) // 100 shares @ $150.00
	fee := margin.TxnFeeAmount(notional)
	require.Greater(t, fee, int64(0), "test rate should produce non-zero fee")

	// Deposit exactly notional + fee so the buyer ends at zero — proves
	// the buy-side hold padding kept enough cash available for the fee.
	depositCash(t, env, "acct-1", notional+fee)

	// Resting sell liquidity at $150 for the saga to sweep.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1_500_000, 100)
	env.pub.events = nil

	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1_500_000, 100)
	env.flush()

	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Completed, s.Status)
	assert.Equal(t, int64(100), s.FilledQty)
	assert.Equal(t, fee, s.FeesPaid)

	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(0), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
}

// Buy-side hold padding: with exactly notional cash deposited, the
// hold padding makes the saga draw a small margin loan (CashBalance
// goes negative by the fee amount) rather than fail. This is the
// designed behavior — margin trading is allowed; the over-leverage
// gate, not raw cash availability, is what blocks unaffordable
// orders. We assert the loan amount here so a regression that
// removed the padding (and silently underpaid the fee at settlement)
// would surface.
func TestReactor_TransactionFee_FeeDrawsMarginLoanWhenUnpadded(t *testing.T) {
	withTxnFeeBps(t, 10)
	env := setupReactorTest(t)

	const notional = int64(150_000_000)
	fee := margin.TxnFeeAmount(notional)
	depositCash(t, env, "acct-1", notional)

	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1_500_000, 100)
	env.pub.events = nil

	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1_500_000, 100)
	env.flush()

	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Completed, s.Status)
	assert.Equal(t, fee, s.FeesPaid)

	// Account picks up a small broker loan equal to the fee.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, -fee, p.CashBalance)
}

// Keep es.Event referenced for forward compatibility — future tests
// in this file may inspect emitted events directly.
var _ es.Event = es.Event{}
var _ = (*portfolio.Portfolio)(nil)
