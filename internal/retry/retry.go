// Package retry computes retry delays: exponential backoff with jitter.
package retry

import (
	"math/rand/v2"
	"time"
)

// jitter spreads each delay by ±20% so jobs that failed together (for example
// during a dependency outage) don't all retry at the same instant.
const jitter = 0.2

type Policy struct {
	Base time.Duration // delay after the first failed attempt
	Max  time.Duration // upper bound for any delay

	rand func() float64 // [0,1); nil means math/rand/v2
}

// Delay returns how long to wait after failed attempt n (1-based):
// Base·2^(n-1), randomized by ±20%, capped at Max.
// With Base = 2s: ~2s, ~4s, ~8s, ~16s, ...
func (p Policy) Delay(n int) time.Duration {
	d := p.Max
	if k := n - 1; k >= 0 && k < 62 && p.Base <= p.Max>>k {
		d = p.Base << k
	}
	r := rand.Float64
	if p.rand != nil {
		r = p.rand
	}
	d = time.Duration(float64(d) * (1 + jitter*(2*r()-1)))
	return max(0, min(d, p.Max))
}
