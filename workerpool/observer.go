package workerpool

import (
	"log/slog"
	"time"
)

// Event carries the details of one observation. Which fields are meaningful
// depends on the Observer method receiving it, as documented there. Fields
// that do not apply are zero.
type Event struct {
	// JobID identifies the job.
	JobID string
	// WorkerID is the 1-based worker involved, or zero if no worker was.
	WorkerID int
	// Attempt is the 1-based attempt number involved.
	Attempt int
	// QueueDepth is the number of jobs waiting in the queue at the moment of
	// the event. It is a racy snapshot by nature.
	QueueDepth int
	// ActiveWorkers is the number of workers executing a job at the moment of
	// the event.
	ActiveWorkers int
	// QueueWait is how long the job spent queued.
	QueueWait time.Duration
	// Duration is the execution time: from the first attempt starting to the
	// terminal outcome, including backoff.
	Duration time.Duration
	// Backoff is the delay about to be waited (JobRetried only).
	Backoff time.Duration
	// Retries is the number of retries performed so far for the job.
	Retries int
	// Err is the associated error, if any.
	Err error
}

// Observer receives lifecycle events. It is the seam for metrics, tracing and
// audit logging: implement it with Prometheus counters, OpenTelemetry spans or
// anything else, without the pool depending on any of them.
//
// Methods are called synchronously on the goroutine that produced the event:
// submission methods on the caller's goroutine, everything else on a worker
// goroutine. They must therefore be fast, non-blocking and safe for concurrent
// use. A panic inside an Observer is recovered and logged so that a faulty
// observer cannot kill a worker.
//
// Embed NopObserver to implement only the methods of interest.
type Observer interface {
	// JobSubmitted: a job was accepted onto the queue.
	// Fields: JobID, QueueDepth, ActiveWorkers.
	JobSubmitted(Event)
	// JobRejected: a submission was refused (queue full, pool closed, or the
	// caller's context ended). Fields: JobID, QueueDepth, Err.
	JobRejected(Event)
	// JobStarted: a worker began the first attempt.
	// Fields: JobID, WorkerID, QueueWait, QueueDepth, ActiveWorkers.
	JobStarted(Event)
	// JobRetried: an attempt failed and a retry will follow after Backoff.
	// Fields: JobID, WorkerID, Attempt (the one that failed), Backoff, Retries, Err.
	JobRetried(Event)
	// JobCompleted: terminal success.
	// Fields: JobID, WorkerID, Attempt, Retries, QueueWait, Duration.
	JobCompleted(Event)
	// JobFailed: terminal failure, including exhausted retries.
	// Fields: JobID, WorkerID, Attempt, Retries, QueueWait, Duration, Err.
	JobFailed(Event)
	// JobCanceled: terminal cancellation, including jobs discarded from the
	// queue by an abort. Fields: JobID, WorkerID (0 if never started), Attempt,
	// Retries, QueueWait, Duration, Err.
	JobCanceled(Event)
	// JobPanicked: terminal panic. Err is a *PanicError.
	// Fields: JobID, WorkerID, Attempt, Retries, QueueWait, Duration, Err.
	JobPanicked(Event)
}

// NopObserver implements Observer by doing nothing. It is the default and is
// intended for embedding.
type NopObserver struct{}

// JobSubmitted implements Observer and does nothing.
func (NopObserver) JobSubmitted(Event) {}

// JobRejected implements Observer and does nothing.
func (NopObserver) JobRejected(Event) {}

// JobStarted implements Observer and does nothing.
func (NopObserver) JobStarted(Event) {}

// JobRetried implements Observer and does nothing.
func (NopObserver) JobRetried(Event) {}

// JobCompleted implements Observer and does nothing.
func (NopObserver) JobCompleted(Event) {}

// JobFailed implements Observer and does nothing.
func (NopObserver) JobFailed(Event) {}

// JobCanceled implements Observer and does nothing.
func (NopObserver) JobCanceled(Event) {}

// JobPanicked implements Observer and does nothing.
func (NopObserver) JobPanicked(Event) {}

// safeObserver shields the pool from a panicking Observer.
type safeObserver struct {
	next Observer
	log  *slog.Logger
}

func (o safeObserver) recovered(method string) {
	if r := recover(); r != nil {
		o.log.Error("observer panicked", "method", method, "panic", r)
	}
}

func (o safeObserver) JobSubmitted(e Event) {
	defer o.recovered("JobSubmitted")
	o.next.JobSubmitted(e)
}
func (o safeObserver) JobRejected(e Event) { defer o.recovered("JobRejected"); o.next.JobRejected(e) }
func (o safeObserver) JobStarted(e Event)  { defer o.recovered("JobStarted"); o.next.JobStarted(e) }
func (o safeObserver) JobRetried(e Event)  { defer o.recovered("JobRetried"); o.next.JobRetried(e) }
func (o safeObserver) JobCompleted(e Event) {
	defer o.recovered("JobCompleted")
	o.next.JobCompleted(e)
}
func (o safeObserver) JobFailed(e Event)   { defer o.recovered("JobFailed"); o.next.JobFailed(e) }
func (o safeObserver) JobCanceled(e Event) { defer o.recovered("JobCanceled"); o.next.JobCanceled(e) }
func (o safeObserver) JobPanicked(e Event) { defer o.recovered("JobPanicked"); o.next.JobPanicked(e) }
