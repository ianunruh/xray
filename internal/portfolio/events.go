package portfolio

import (
	"google.golang.org/protobuf/proto"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const (
	EventCashDeposited      = "CashDeposited"
	EventCashWithdrawn      = "CashWithdrawn"
	EventCashHeld           = "CashHeld"
	EventCashReleased       = "CashReleased"
	EventCashSettled        = "CashSettled"
	EventSharesCredited     = "SharesCredited"
	EventSharesDebited      = "SharesDebited"
	EventSharesHeld         = "SharesHeld"
	EventSharesReleased     = "SharesReleased"
	EventSharesSettled      = "SharesSettled"
	EventCollateralHeld     = "CollateralHeld"
	EventCollateralReleased = "CollateralReleased"
	EventShortOpened        = "ShortOpened"
	EventShortCoverHeld     = "ShortCoverHeld"
	EventShortCoverReleased = "ShortCoverReleased"
	EventShortCovered       = "ShortCovered"
	EventMarginCallIssued     = "MarginCallIssued"
	EventMarginCallCovered    = "MarginCallCovered"
	EventMarginInterestAccrued = "MarginInterestAccrued"
	EventShortBorrowFeeAccrued = "ShortBorrowFeeAccrued"
)

func RegisterEvents(r *es.Registry) {
	r.Register(EventCashDeposited, func() proto.Message { return new(portfoliov1.CashDeposited) })
	r.Register(EventCashWithdrawn, func() proto.Message { return new(portfoliov1.CashWithdrawn) })
	r.Register(EventCashHeld, func() proto.Message { return new(portfoliov1.CashHeld) })
	r.Register(EventCashReleased, func() proto.Message { return new(portfoliov1.CashReleased) })
	r.Register(EventCashSettled, func() proto.Message { return new(portfoliov1.CashSettled) })
	r.Register(EventSharesCredited, func() proto.Message { return new(portfoliov1.SharesCredited) })
	r.Register(EventSharesDebited, func() proto.Message { return new(portfoliov1.SharesDebited) })
	r.Register(EventSharesHeld, func() proto.Message { return new(portfoliov1.SharesHeld) })
	r.Register(EventSharesReleased, func() proto.Message { return new(portfoliov1.SharesReleased) })
	r.Register(EventSharesSettled, func() proto.Message { return new(portfoliov1.SharesSettled) })
	r.Register(EventCollateralHeld, func() proto.Message { return new(portfoliov1.CollateralHeld) })
	r.Register(EventCollateralReleased, func() proto.Message { return new(portfoliov1.CollateralReleased) })
	r.Register(EventShortOpened, func() proto.Message { return new(portfoliov1.ShortOpened) })
	r.Register(EventShortCoverHeld, func() proto.Message { return new(portfoliov1.ShortCoverHeld) })
	r.Register(EventShortCoverReleased, func() proto.Message { return new(portfoliov1.ShortCoverReleased) })
	r.Register(EventShortCovered, func() proto.Message { return new(portfoliov1.ShortCovered) })
	r.Register(EventMarginCallIssued, func() proto.Message { return new(portfoliov1.MarginCallIssued) })
	r.Register(EventMarginCallCovered, func() proto.Message { return new(portfoliov1.MarginCallCovered) })
	r.Register(EventMarginInterestAccrued, func() proto.Message { return new(portfoliov1.MarginInterestAccrued) })
	r.Register(EventShortBorrowFeeAccrued, func() proto.Message { return new(portfoliov1.ShortBorrowFeeAccrued) })
}
