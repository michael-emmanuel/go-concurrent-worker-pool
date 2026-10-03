package workerpool

import (
	"errors"
	"fmt"
)

var (
	// ErrQueueFull is returned by TrySubmit (and TrySubmitTask) when the queue
	// has no free capacity. It is a load signal, not a failure of the pool.
	ErrQueueFull = errors.New("workerpool: queue full")

	// ErrClosed is returned by submission methods once Shutdown or Close has
	// been called, including to callers that were blocked waiting for capacity
	// when shutdown began.
	ErrClosed = errors.New("workerpool: pool is shut down")

	// ErrShutdownTimeout is returned by Shutdown when its context ended before
	// the pool drained. The pool has then been aborted: running jobs are being
	// canceled and queued jobs are discarded. The returned error also wraps the
	// context's error.
	ErrShutdownTimeout = errors.New("workerpool: shutdown deadline exceeded, pool aborted")

	// ErrAborted is the cancellation cause seen by jobs (via context.Cause)
	// and the Result of queued jobs when the pool is aborted by Close or by an
	// expired Shutdown context.
	ErrAborted = errors.New("workerpool: pool aborted")

	// ErrJobCanceled is the cancellation cause set by Handle.Cancel.
	ErrJobCanceled = errors.New("workerpool: job canceled")

	// ErrRetriesExhausted is wrapped into a Result's error when a retryable
	// error persisted through every allowed attempt.
	ErrRetriesExhausted = errors.New("workerpool: retries exhausted")

	// ErrPanic matches (via errors.Is) any *PanicError.
	ErrPanic = errors.New("workerpool: job panicked")
)

// PanicError describes a panic recovered at the job execution boundary.
type PanicError struct {
	// Value is the value passed to panic.
	Value any
	// Stack is the goroutine stack captured at the point of recovery.
	Stack []byte
	// JobID identifies the job that panicked.
	JobID string
	// WorkerID identifies the worker (1-based) that was running the job.
	WorkerID int
	// Attempt is the 1-based attempt during which the panic occurred.
	Attempt int
}

// Error implements error. The stack is deliberately omitted; use the Stack
// field when it is needed.
func (e *PanicError) Error() string {
	return fmt.Sprintf("workerpool: job %s panicked on worker %d (attempt %d): %v",
		e.JobID, e.WorkerID, e.Attempt, e.Value)
}

// Is reports whether target is ErrPanic.
func (e *PanicError) Is(target error) bool { return target == ErrPanic }

// Unwrap returns the panic value if it was itself an error, so that
// errors.Is and errors.As can see through panic(err).
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}

// permanentError marks an error as not worth retrying.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so that the default retry classifier will not retry it.
// Use it for failures that another attempt cannot fix, such as validation
// errors or HTTP 4xx responses. Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, was marked with
// Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// ErrNilJob is returned when a nil Job is submitted.
var ErrNilJob = errors.New("workerpool: nil job")
