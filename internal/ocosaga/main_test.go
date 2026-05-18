package ocosaga_test

import (
	"os"
	"testing"

	"github.com/ianunruh/xray/internal/margin"
)

// TestMain pins TxnFeeBps to zero — this package's tests pre-date
// transaction fees and assert exact pre-fee cash balances. Fee
// integration is exercised in internal/ordersaga.
func TestMain(m *testing.M) {
	margin.TxnFeeBps = 0
	os.Exit(m.Run())
}
