package workerpool

import (
	"context"
	"time"
)

// Job is a unit of work. Run should return promptly when ctx is canceled;
// cancellation is cooperative and the pool cannot stop a goroutine that
// ignores it.
//
// The context passed to Run is derived from the pool, not from the context
// given to Submit. It is canceled when the pool is aborted, when the job's
// Handle is canceled, when a per-attempt timeout expires, and never merely
// because the submitting request finished.
//
// If retries are enabled, Run may be called more than once for the same
// submission and must therefore be idempotent, or at least safe to repeat.
type Job interface {
	Run(ctx context.Context) error
}

// JobFunc adapts an ordinary function to the Job interface.
type JobFunc func(ctx context.Context) error

// Run calls f(ctx).
func (f JobFunc) Run(ctx context.Context) error { return f(ctx) }

// Task is a Job plus per-job options. It is accepted by SubmitTask and
// TrySubmitTask, which return a Handle for tracking and canceling the job.
type Task struct {
	// Job is the work to run. It must not be nil.
	Job Job
	// ID is an optional caller-chosen identifier used in logs, events and
	// results. When empty the pool assigns a unique one ("job-N").
	ID string
	// Timeout, if positive, overrides Config.JobTimeout for this task. It
	// applies to each attempt individually, not to the task as a whole.
	Timeout time.Duration
}

// Outcome is the terminal state of a job.
type Outcome int

const (
	// Succeeded means Run returned nil.
	Succeeded Outcome = iota + 1
	// Failed means the job returned an error that was not retried, or
	// exhausted its attempts.
	Failed
	// Canceled means the job's context ended (pool abort, Handle.Cancel)
	// before the job succeeded, including jobs canceled while still queued.
	Canceled
	// Panicked means Run panicked. Panics are never retried.
	Panicked
)

// String returns a lower-case name for the outcome.
func (o Outcome) String() string {
	switch o {
	case Succeeded:
		return "succeeded"
	case Failed:
		return "failed"
	case Canceled:
		return "canceled"
	case Panicked:
		return "panicked"
	default:
		return "unknown"
	}
}

// Result is the terminal report for one accepted job.
type Result struct {
	// JobID is the job's identifier.
	JobID string
	// Outcome is the terminal state.
	Outcome Outcome
	// Err is nil for Succeeded. For Failed it is the last error returned by
	// the job, wrapped with ErrRetriesExhausted if attempts ran out. For
	// Canceled it wraps the cancellation cause (ErrJobCanceled or
	// ErrAborted). For Panicked it is a *PanicError.
	Err error
	// Attempts is the number of times Run was invoked. It is zero for jobs
	// canceled before they started.
	Attempts int
	// QueueWait is the time between acceptance and the first attempt starting
	// (or the job being discarded).
	QueueWait time.Duration
	// Duration is the time from the first attempt starting until the terminal
	// outcome, including retry backoff.
	Duration time.Duration
}

// Info describes the execution a job is currently part of. Retrieve it inside
// Run with InfoFromContext.
type Info struct {
	// JobID is the job's identifier.
	JobID string
	// WorkerID is the 1-based worker executing the attempt.
	WorkerID int
	// Attempt is the 1-based attempt number. Attempt 1 is the first execution;
	// Attempt n > 1 is retry n-1.
	Attempt int
}

type infoKey struct{}

// InfoFromContext returns the execution Info attached to a context passed to
// Job.Run. The boolean is false for any other context.
func InfoFromContext(ctx context.Context) (Info, bool) {
	info, ok := ctx.Value(infoKey{}).(Info)
	return info, ok
}
