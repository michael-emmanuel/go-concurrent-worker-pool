// Command retries shows retrying transient failures with exponential backoff
// and jitter, and how a permanent error skips the retry loop.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/example/concurrent-worker-pool/workerpool"
)

var errTemporary = errors.New("temporary upstream error")

// printer is an Observer that prints retry events.
type printer struct{ workerpool.NopObserver }

func (printer) JobRetried(e workerpool.Event) {
	fmt.Printf("  %s: attempt %d failed (%v); retrying in %v\n", e.JobID, e.Attempt, e.Err, e.Backoff.Round(time.Millisecond))
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pool, err := workerpool.New(workerpool.Config{
		Workers:   2,
		QueueSize: 8,
		RetryPolicy: workerpool.RetryPolicy{
			MaxAttempts:    5,
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     200 * time.Millisecond,
			Multiplier:     2,
			Jitter:         0.2,
		},
		Observer: printer{},
		Logger:   logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Fails twice, then succeeds: 3 attempts, 2 retries.
	calls := 0
	flaky, _ := pool.SubmitTask(context.Background(), workerpool.Task{ID: "flaky", Job: workerpool.JobFunc(func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return errTemporary
		}
		return nil
	})})

	// A permanent error is never retried, whatever MaxAttempts says.
	invalid, _ := pool.SubmitTask(context.Background(), workerpool.Task{ID: "invalid", Job: workerpool.JobFunc(func(ctx context.Context) error {
		return workerpool.Permanent(errors.New("malformed request"))
	})})

	// Always fails with a retryable error: exhausts all 5 attempts.
	broken, _ := pool.SubmitTask(context.Background(), workerpool.Task{ID: "broken", Job: workerpool.JobFunc(func(ctx context.Context) error {
		return errTemporary
	})})

	for _, h := range []*workerpool.Handle{flaky, invalid, broken} {
		res, _ := h.Wait(context.Background())
		fmt.Printf("%s: %s after %d attempt(s), exhausted=%t err=%v\n",
			res.JobID, res.Outcome, res.Attempts, errors.Is(res.Err, workerpool.ErrRetriesExhausted), res.Err)
	}
	if err := pool.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
}
