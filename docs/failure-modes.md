# Failure Modes

| Failure | Behavior | Reason | Operational consideration |
| --- | --- | --- | --- |
| Queue full, `TrySubmit` | Returns `ErrQueueFull` immediately; `JobRejected` event; `Rejected` counter | Bound memory and latency; tell the producer now | Alert on sustained rejections. Map to HTTP 503 with `Retry-After`. |
| Queue full, `Submit` | Blocks until space, context end, or shutdown | Lets bounded producers be slowed instead of failed | Always pass a context with a deadline. Blocked producers hold resources. |
| Job returns error, no retry policy | `Failed`, error in `Result.Err`, logged at Error | Errors are reported, never swallowed | Alert on failure rate. |
| Job returns retryable error | Retry after backoff on the same worker | Transient faults often clear | Retries multiply downstream load; keep attempts low. |
| Job returns `Permanent` error | `Failed` after that attempt, no retry | Retrying cannot fix it | Mark non-transient errors explicitly. |
| Retry exhaustion | `Failed`; `Result.Err` matches `ErrRetriesExhausted` and the last error | Bounded work per job | No dead-letter mechanism here: log, count, or hand to a durable store from an `Observer`. |
| Job panics | Recovered at the attempt boundary; `Panicked`; `*PanicError` with value, stack, job/worker/attempt; Error log; observer event; never retried; worker keeps running | A panic in one job must not take down the process; a panic signals a defect, so re-running is not a fix | Treat every panic as a bug to fix. Recovery keeps the service up; it does not make the panic acceptable. |
| Panic in `Observer` | Recovered and logged at Error; job outcome unaffected | Observability must not break execution | Fix the observer. |
| Panic in `RetryPolicy.Retryable` | Recovered and logged; error treated as not retryable | Same | Fix the classifier. |
| Job ignores context | Holds its worker past cancel, timeout or abort | Go cannot stop goroutines; the pool runs attempts synchronously to avoid concurrent zombie attempts | Timeouts and shutdown are only as good as job cooperation. Test jobs with canceled contexts. |
| Job calls `runtime.Goexit` | The worker goroutine terminates, that job's outcome is never reported (its `Handle` never resolves), and the pool does not replace the worker | Not a supported outcome; `Goexit` cannot be recovered and is the one way a job can escape the terminal-outcome guarantee | Capacity silently shrinks. Do not call `Goexit` (or `t.FailNow`) from job code. Shutdown still completes, because the exiting worker is accounted for. |
| Context canceled during `Submit` | Returns `ctx.Err()`; job not enqueued | Caller's deadline is respected | Distinguish from `ErrQueueFull` when reporting. |
| Context canceled while job queued | Job not run; `Canceled`, 0 attempts | Cancellation is a request to stop | None. |
| Context canceled while job running | Job sees it; `Canceled` if the job's context is done when it returns an error | Cooperative cancellation | Return promptly. |
| Cancel during backoff | Sleep ends immediately; `Canceled` with attempts so far | Backoff is context-aware | None. |
| Submit after shutdown began | `ErrClosed` | Stop intake first | Stop upstream sources before shutting the pool down. |
| Submit blocked when shutdown begins | Returns `ErrClosed` | Otherwise shutdown could wait on producers forever | Callers must treat `ErrClosed` as "not accepted". |
| Submit racing Shutdown | Either `ErrClosed` or accepted-and-drained; never a panic or a lost accepted job | Send/close protocol in [concurrency-model.md](concurrency-model.md) | Covered by tests under `-race`. |
| Graceful shutdown completes | `Shutdown` returns nil; queued jobs ran; state `Stopped` | Drain semantics | Time is bounded by the slowest job plus retry backoff. |
| Shutdown timeout | Abort: running jobs canceled (`ErrAborted`), queued jobs discarded and reported `Canceled`; `Shutdown` returns `ErrShutdownTimeout` wrapping `ctx.Err()` without waiting for workers | Termination time must be bounded | Discarded work is lost. Use `Done()` or `Close()` to wait for slow workers. |
| Process crash / `SIGKILL` | Queued and running jobs are lost | In-memory design | Use a durable queue for work that must survive. |
| Downstream slows down | Jobs take longer, workers stay busy, queue fills, rejections start | This is backpressure propagating to the edge | Watch queue wait and rejection rate; do not raise workers past what the downstream tolerates. |
| Downstream outage with retries | Workers spend time in attempts and backoff; throughput drops; queue fills; rejections | Retries trade throughput for success probability | Low `MaxAttempts`, jitter, and consider a retry budget. |
| Invalid configuration | `New` returns an error listing every problem | Fail at construction, not at 3 a.m. | None. |
