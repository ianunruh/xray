package portfolio_test

import (
	"os"
	"testing"

	"github.com/ianunruh/xray/internal/margin"
)

// TestMain pins TxnFeeBps to zero for this package's tests. The bulk
// of commands_test.go pre-dates transaction fees and asserts exact
// pre-fee cash balances; fee-specific behavior is exercised by the
// dedicated TestTransactionFee_* tests, which restore the production
// rate locally.
func TestMain(m *testing.M) {
	margin.TxnFeeBps = 0
	os.Exit(m.Run())
}
