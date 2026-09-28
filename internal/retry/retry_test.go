package retry

import (
	"testing"
	"time"
)

func TestDelayDoublesAndCaps(t *testing.T) {
	p := Policy{Base: 2 * time.Second, Max: 10 * time.Second, rand: func() float64 { return 0.5 }} // no jitter
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := p.Delay(i + 1); got != w {
			t.Errorf("Delay(%d) = %s, want %s", i+1, got, w)
		}
	}
	if got := p.Delay(1000); got != p.Max { // no overflow at large attempt numbers
		t.Errorf("Delay(1000) = %s, want %s", got, p.Max)
	}
}

func TestDelayJitterBounds(t *testing.T) {
	low := Policy{Base: time.Second, Max: time.Hour, rand: func() float64 { return 0 }}
	high := Policy{Base: time.Second, Max: time.Hour, rand: func() float64 { return 0.999999 }}
	if got := low.Delay(1); got != 800*time.Millisecond {
		t.Errorf("min jitter = %s, want 800ms", got)
	}
	if got := high.Delay(1); got < 1199*time.Millisecond || got > 1200*time.Millisecond {
		t.Errorf("max jitter = %s, want ~1.2s", got)
	}
	if got := high.Delay(20); got != time.Hour {
		t.Errorf("jitter must not exceed Max: got %s", got)
	}

	// Real randomness: every sample stays within ±20% and samples differ.
	p := Policy{Base: time.Second, Max: time.Hour}
	seen := map[time.Duration]bool{}
	for range 100 {
		d := p.Delay(1)
		if d < 800*time.Millisecond || d > 1200*time.Millisecond {
			t.Fatalf("delay %s outside ±20%%", d)
		}
		seen[d] = true
	}
	if len(seen) < 50 {
		t.Fatalf("jitter produced only %d distinct delays in 100 samples", len(seen))
	}
}
