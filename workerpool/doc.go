// Package workerpool implements a bounded, fixed-size concurrent worker pool
// with explicit backpressure, retries with exponential backoff and jitter,
// per-job cancellation, panic isolation, graceful shutdown and lightweight
// observability hooks. It depends only on the Go standard library.
//
// # Model
//
// Producers call Submit or TrySubmit to place a Job on a bounded queue. A
// fixed number of worker goroutines take jobs from the queue and execute them.
// The queue capacity is the only place where work can accumulate: when it is
// full, the caller is told (TrySubmit returns ErrQueueFull) or made to wait
// (Submit blocks until space is available, the caller's context ends, or the
// pool starts shutting down).
//
// # Lifecycle
//
// A Pool moves through three states, and only forward:
//
//	Running -> Draining -> Stopped
//
// Shutdown stops accepting work, lets queued and running jobs finish, and
// waits for the workers to exit, bounded by the caller's context. If that
// context ends first the pool is aborted: running jobs have their contexts
// canceled and jobs still in the queue are discarded (reported as canceled).
// Close is the immediate variant of the same operation.
//
// # Delivery semantics
//
// Every job accepted by Submit or TrySubmit reaches exactly one terminal
// outcome (succeeded, failed, canceled or panicked), which is reported to the
// Observer and, for tracked tasks, to the Handle. A job may be executed more
// than once when retries are enabled, so jobs that are retried must be
// idempotent. The pool is an in-memory component: jobs do not survive a
// process exit.
package workerpool
