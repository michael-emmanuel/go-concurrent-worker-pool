package workerpool

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy controls whether and when a failed attempt is retried.
//
// The zero value performs a single attempt and never retries. Retries are
// opt-in because retrying is only safe for idempotent work against transient
// failures.
//
// Terminology: MaxAttempts counts executions of Run, including the first. With
// MaxAttempts = 5 there is at most one initial attempt and four retries:
//
//	attempt 1, backoff, attempt 2 (retry 1), backoff, attempt 3 (retry 2),
//	backoff, attempt 4 (retry 3), backoff, attempt 5 (retry 4)
//
// Retries are performed by the worker that is running the job. A retrying job
// keeps its worker (including while it waits out the backoff) and is never
// placed back on the queue, so retries cannot inflate queue depth. The cost is
// that a job in backoff occupies a worker slot.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of executions per job. Zero means 1
	// (no retries). Negative values are invalid.
	MaxAttempts int

	// InitialBackoff is the delay before the first retry. When MaxAttempts is
	// greater than 1, zero means 100ms.
	InitialBackoff time.Duration

	// MaxBackoff is the ceiling for any single delay, applied before jitter.
	// When MaxAttempts is greater than 1, zero means 30s.
	MaxBackoff time.Duration

	// Multiplier is the growth factor between consecutive delays. When
	// MaxAttempts is greater than 1, zero means 2.0. Values below 1 are
	// invalid.
	Multiplier float64

	// Jitter is the fraction, in [0, 1], of each delay that is randomly
	// subtracted. A delay d becomes a uniform random value in
	// [d*(1-Jitter), d], so MaxBackoff remains a true ceiling. Zero disables
	// jitter, which is usually undesirable when many jobs can fail together.
	Jitter float64

	// Retryable classifies errors. It is consulted only for errors returned
	// while the job's own context is still live. If nil, every error except
	// those marked with Permanent is retried. It is called from worker
	// goroutines and must be safe for concurrent use.
	Retryable func(err error) bool
}

// DefaultRetryPolicy returns a conservative policy: 3 attempts, 100ms initial
// backoff doubling to at most 5s, with 20% jitter.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2.0,
		Jitter:         0.2,
	}
}

// Validate reports every problem with the policy, joined into one error.
func (p RetryPolicy) Validate() error {
	var errs []error
	if p.MaxAttempts < 0 {
		errs = append(errs, fmt.Errorf("MaxAttempts must be >= 0, got %d", p.MaxAttempts))
	}
	if p.InitialBackoff < 0 {
		errs = append(errs, fmt.Errorf("InitialBackoff must be >= 0, got %v", p.InitialBackoff))
	}
	if p.MaxBackoff < 0 {
		errs = append(errs, fmt.Errorf("MaxBackoff must be >= 0, got %v", p.MaxBackoff))
	}
	if p.InitialBackoff > 0 && p.MaxBackoff > 0 && p.MaxBackoff < p.InitialBackoff {
		errs = append(errs, fmt.Errorf("MaxBackoff (%v) must be >= InitialBackoff (%v)", p.MaxBackoff, p.InitialBackoff))
	}
	if p.Multiplier != 0 && (p.Multiplier < 1 || math.IsNaN(p.Multiplier) || math.IsInf(p.Multiplier, 0)) {
		errs = append(errs, fmt.Errorf("Multiplier must be a finite value >= 1 (or 0 for the default), got %v", p.Multiplier))
	}
	if p.Jitter < 0 || p.Jitter > 1 || math.IsNaN(p.Jitter) {
		errs = append(errs, fmt.Errorf("Jitter must be within [0, 1], got %v", p.Jitter))
	}
	return errors.Join(errs...)
}

// normalized returns the policy with zero values replaced by defaults. It
// assumes Validate has passed.
func (p RetryPolicy) normalized() RetryPolicy {
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 1
	}
	if p.MaxAttempts > 1 {
		if p.InitialBackoff == 0 {
			p.InitialBackoff = 100 * time.Millisecond
		}
		if p.MaxBackoff == 0 {
			p.MaxBackoff = 30 * time.Second
			if p.MaxBackoff < p.InitialBackoff {
				p.MaxBackoff = p.InitialBackoff
			}
		}
		if p.Multiplier == 0 {
			p.Multiplier = 2.0
		}
	}
	return p
}

// attempts returns the effective maximum number of attempts.
func (p RetryPolicy) attempts() int {
	if p.MaxAttempts <= 0 {
		return 1
	}
	return p.MaxAttempts
}

// Classify reports whether err is of a kind worth retrying, ignoring how many
// attempts have been made.
func (p RetryPolicy) Classify(err error) bool {
	if err == nil {
		return false
	}
	if p.Retryable != nil {
		return p.Retryable(err)
	}
	return !IsPermanent(err)
}

// ShouldRetry reports whether a job whose attempt number attempt (1-based)
// just failed with err should be attempted again.
func (p RetryPolicy) ShouldRetry(err error, attempt int) bool {
	return attempt < p.attempts() && p.Classify(err)
}

// Backoff returns the randomized delay to wait before retry number retry
// (1-based: retry 1 follows attempt 1).
func (p RetryPolicy) Backoff(retry int) time.Duration {
	return p.backoff(retry, rand.Float64())
}

// backoff is Backoff with the random source injected. r must be in [0, 1).
func (p RetryPolicy) backoff(retry int, r float64) time.Duration {
	p = p.normalized()
	if retry < 1 {
		retry = 1
	}
	d := float64(p.InitialBackoff) * math.Pow(p.Multiplier, float64(retry-1))
	if limit := float64(p.MaxBackoff); math.IsInf(d, 0) || math.IsNaN(d) || d > limit {
		d = limit
	}
	d -= d * p.Jitter * r
	return time.Duration(d)
}

// sleepContext waits for d or until ctx is done, whichever comes first. It
// returns ctx's error in the latter case and stops its timer in all cases.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
