package portfolio

import (
	"google.golang.org/protobuf/proto"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

func RegisterEvents(r *es.Registry) {
	r.Register("CashDeposited", func() proto.Message { return new(portfoliov1.CashDeposited) })
	r.Register("CashWithdrawn", func() proto.Message { return new(portfoliov1.CashWithdrawn) })
	r.Register("CashHeld", func() proto.Message { return new(portfoliov1.CashHeld) })
	r.Register("CashReleased", func() proto.Message { return new(portfoliov1.CashReleased) })
	r.Register("CashSettled", func() proto.Message { return new(portfoliov1.CashSettled) })
	r.Register("SharesDebited", func() proto.Message { return new(portfoliov1.SharesDebited) })
}
