# Cancellation

## Contexts in play

| Context | Given by | Governs |
| --- | --- | --- |
| Submit context | Caller of `Submit`/`SubmitTask` | Only the wait for queue space. |
| Job context | The pool, passed to `Job.Run` | Execution of the job, including retry backoff. |
| Attempt context | The pool, derived from the job context | One attempt; adds the per-attempt timeout and `Info`. |
| Shutdown context | Caller of `Shutdown` | How long to wait for a graceful drain. |

## Why the Submit context is not the job context

The usual argument for the opposite: "if the caller gives up, the job should stop". For request/response code that is often wrong. An HTTP handler that enqueues a background job and returns 202 has a request context that is canceled the moment the response is written. If that context became the job's context, every job would be canceled immediately. So `Submit`'s context has one meaning, "how long am I willing to wait to get in the queue", and job cancellation is expressed separately, either by pool abort or per job through a `Handle`:

```go
h, err := pool.SubmitTask(ctx, workerpool.Task{Job: job, Timeout: 2 * time.Second})
...
h.Cancel() // cancels this job only
```

If a job should be tied to a caller-supplied context, capture it in the job itself and combine (for example, using `context.AfterFunc` or by selecting on both).

## Cooperative cancellation

Go cannot stop a goroutine from outside. Cancellation is a signal (`ctx.Done()` closes, `ctx.Err()` becomes non-nil, `context.Cause(ctx)` says why) that the job must observe: pass `ctx` to every blocking call, check `ctx.Err()` between units of CPU-bound work, and return promptly. A job that ignores its context:

- Continues to hold its worker after a timeout, cancel or abort.
- Delays graceful shutdown up to the shutdown deadline, and delays `Close` indefinitely.

The pool runs each attempt synchronously on its worker; it does not spawn a goroutine to abandon on timeout. That is a deliberate choice: abandoning a goroutine would let the "timed out" attempt keep running concurrently with the retry, which is worse for a job with side effects.

## Cancellation causes

Job contexts are created with `context.WithCancelCause`, so `context.Cause(ctx)` distinguishes:

| Cause | Meaning |
| --- | --- |
| `ErrJobCanceled` | `Handle.Cancel` was called. |
| `ErrAborted` | The pool was aborted by `Close` or an expired `Shutdown`. |
| `context.DeadlineExceeded` (from `ctx.Err()`) | The per-attempt timeout expired. This is not pool cancellation and is retryable by default. |

A `Canceled` `Result.Err` wraps the cause and, if the job returned an error of its own, that error too (`errors.Is` finds both).

## Where cancellation is observed

- Blocked `Submit`: returns `ctx.Err()` promptly. Verified by `TestSubmitContextCanceledWhileBlocked` and `TestSubmitContextDeadline`. A context that is already done is rejected before any queue interaction, so `Submit(canceledCtx, job)` never enqueues (`TestSubmitWithCanceledContextDoesNotEnqueue`).
- Queued job: if its context is done when a worker dequeues it, it is not run and is reported `Canceled` with zero attempts.
- Running job: the job sees `ctx.Done()`.
- Retry backoff: the sleep is a `select` on a timer and the job context; cancellation ends it at once (`TestCancelDuringBackoff`).
- Shutdown: abort cancels all of the above.

## Cancellation is not an application error

A canceled job is `Canceled`, not `Failed`. It is never retried, it does not increment the failure counter, it is reported through `Observer.JobCanceled`, and it is logged at Debug. Alerting on `Failed` therefore does not fire on deploys.

The classification is by the job context, not by the error type returned: if the job context is live and the job returns `context.DeadlineExceeded` from its own downstream call, that is an ordinary failure and is subject to the retry policy.
