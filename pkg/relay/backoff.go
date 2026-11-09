package relay

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

// BackoffPolicy configures exponential backoff with optional Full Jitter.
type BackoffPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Factor    float64
	Jitter    bool
}

// DefaultBackoffPolicy returns a production-ready Full Jitter exponential backoff policy.
// Base delay: 1s, Max delay: 60s, Factor: 2.0, Full Jitter enabled.
func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{
		BaseDelay: 1 * time.Second,
		MaxDelay:  60 * time.Second,
		Factor:    2.0,
		Jitter:    true,
	}
}

// Compute calculates the backoff duration for the given attempt count (1-indexed).
func (p *BackoffPolicy) Compute(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	b := backoff.NewExponentialBackOff()
	if p.BaseDelay > 0 {
		b.InitialInterval = p.BaseDelay
	} else {
		b.InitialInterval = 100 * time.Millisecond
	}

	if p.Factor >= 1.0 {
		b.Multiplier = p.Factor
	} else {
		b.Multiplier = 2.0
	}

	if p.MaxDelay > 0 {
		b.MaxInterval = p.MaxDelay
	} else {
		b.MaxInterval = 60 * time.Second
	}

	b.MaxElapsedTime = 0
	if !p.Jitter {
		b.RandomizationFactor = 0
	}
	b.Reset()

	for i := 1; i < attempt; i++ {
		b.NextBackOff()
	}
	return b.NextBackOff()
}

// Duration returns the backoff duration for the given attempt count.
func (p *BackoffPolicy) Duration(attempt int) time.Duration {
	return p.Compute(attempt)
}
