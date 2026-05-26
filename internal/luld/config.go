// Package luld implements the LULD (Limit-Up/Limit-Down) volatility
// moderator. The reactor watches continuous trade prints to update
// each symbol's price bands via SetLULDBands, observes
// LULDLimitStateEntered / TradingHalted to register symbols for
// time-driven transitions, and exposes EvaluateLULDExpiry for the
// reconciler to drive grace-window and halt-reopen expirations.
//
// Band trip detection itself lives in the orderbook matcher
// (cmd/orderbook/commands.go); this package is the post-trip
// orchestrator and the band-source.
package luld

import (
	"time"
)

// Tiers maps symbols to their band width in basis points. Real LULD
// has Tier 1 (most-liquid S&P 500 / R1000 names: ±5%) and Tier 2
// (everything else: ±10%), with widened bands during the first/last
// 15 minutes of the session. V1 supports only the static-percent
// flavor; the open/close doubling is a TODO.
type Tiers struct {
	bands   map[string]int32
	defBand int32
}

// NewTiers constructs a Tiers config from a per-symbol bps map and a
// default applied to symbols not in the map. Both must be > 0.
func NewTiers(perSymbol map[string]int32, defaultBps int32) Tiers {
	cp := make(map[string]int32, len(perSymbol))
	for k, v := range perSymbol {
		cp[k] = v
	}
	if defaultBps <= 0 {
		defaultBps = 1000 // ±10% Tier 2 default
	}
	return Tiers{bands: cp, defBand: defaultBps}
}

// BandBps returns the band width in basis points for symbol.
func (t Tiers) BandBps(symbol string) int32 {
	if v, ok := t.bands[symbol]; ok && v > 0 {
		return v
	}
	return t.defBand
}

// ComputeBands derives upper/lower band prices from a reference price
// and the band width in bps. Bands are computed symmetrically around
// the reference. Returns (0,0) when reference <= 0 or bps <= 0.
func ComputeBands(reference int64, bps int32) (lower, upper int64) {
	if reference <= 0 || bps <= 0 {
		return 0, 0
	}
	delta := reference * int64(bps) / 10000
	if delta <= 0 {
		delta = 1 // never let the band collapse to a single tick
	}
	return reference - delta, reference + delta
}

// Config bundles the reactor's tunables. Defaults align with the
// real-world LULD spec: 15s limit-state grace, 5-minute halt.
type Config struct {
	// LimitStateGrace must match orderbook.LULDLimitStateGrace
	// (stamped on the LULDLimitStateEntered event at trip time). The
	// reactor reads this only for its Status output and validation;
	// the authoritative deadline lives on the aggregate.
	LimitStateGrace time.Duration
	// HaltDuration is the wall-clock distance between TradingHalted's
	// `at` and `reopen_at`. The reactor stamps `reopen_at` at halt
	// time and the reconciler drives the reopening auction once it
	// passes. Default 5 minutes.
	HaltDuration time.Duration
}
