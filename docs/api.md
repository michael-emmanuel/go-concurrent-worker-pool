# API Reference and Design Rationale

`go doc -all ./workerpool` is the authoritative reference; this document explains the semantics and why the API has this shape.

## Why not expose channels

The queue is a channel internally, but the public API is methods. Reasons:

- Shutdown safety. If callers held the channel they could send after it is closed (a panic) or forget to close it (a leak). The send/close protocol only works if the pool is the sole closer and every sender registers first.
- Explicit policy per call. Blocking, non-blocking and context-aware submission are different operations with different error contracts; a channel offers only "send" and "select" that each caller must write correctly.
- Freedom to change the internals (see design tradeoffs) without breaking callers.
- Observability: every submission passes through code that can count it and report it.

## Job design

```go
type Job interface{ Run(ctx context.Context) error }
type JobFunc func(ctx context.Context) error
```

Alternatives considered:

| Question | Decision | Why |
| --- | --- | --- |
| Should a job have an ID? | Optional, via `Task.ID`; otherwise generated (`job-N`). | Needed for logs and events; forcing every caller to invent one is friction. Not in the `Job` interface so plain functions stay plain. |
| Metadata? | No. | Any metadata a caller needs can be captured by the job closure or struct. A generic metadata bag invites unstructured data flowing through the pool. |
| Are attempts visible? | Yes, through the context: `InfoFromContext(ctx)` gives `JobID`, `WorkerID`, `Attempt`. | Keeps `Run`'s signature minimal, and the information is only meaningful during a run. |
| Should the pool own retries? | Yes. | See [retries.md](retries.md). |
| Should execution be context-aware? | Yes; `Run` takes a `context.Context`. | Required for cancellation, timeouts and shutdown. |
| Should `Run` return a value? | No. | Results are typically delivered by the job into a channel, struct, or store the caller owns; generic result plumbing would force type parameters onto the pool. |

`Submit` and `TrySubmit` take a `Job` and return an `error`. `SubmitTask` and `TrySubmitTask` take a `Task` (job plus ID and timeout) and return a `*Handle`. The four methods exist because the two axes (blocking or not; tracked or not) are independent, and the untracked forms avoid allocating a handle and a cancelable context for fire-and-forget work.

## Pool

`New(cfg Config) (*Pool, error)`: validates, applies defaults, starts `cfg.Workers` goroutines. Errors from `Validate` list every problem.

`Submit(ctx, job) error`: blocks while the queue is full. Returns nil (accepted), `ctx.Err()`, `ErrClosed` or `ErrNilJob`. `ctx` bounds only the wait.

`TrySubmit(job) error`: never blocks. Returns nil, `ErrQueueFull`, `ErrClosed` or `ErrNilJob`.

`SubmitTask` / `TrySubmitTask`: as above, returning `*Handle` (nil on error).

`Shutdown(ctx) error`, `Close() error`, `Done() <-chan struct{}`, `State() State`, `Stats() Stats`: see [graceful-shutdown.md](graceful-shutdown.md).

Accepted means: the item is on the queue. From then on the job will reach exactly one terminal outcome (the single exception, `runtime.Goexit` in job code, is documented in [failure-modes.md](failure-modes.md)).

## Config

| Field | Meaning | Zero value |
| --- | --- | --- |
| `Workers` | Worker goroutines | Invalid (must be > 0) |
| `QueueSize` | Queue capacity | 0: unbuffered hand-off (valid, strict) |
| `RetryPolicy` | See below | One attempt, no retries |
| `JobTimeout` | Per-attempt timeout | None |
| `Observer` | Event sink | `NopObserver` |
| `Logger` | `*slog.Logger` | Discard |

Why `Workers` has no default: choosing it is a capacity decision (downstream limits, CPU); a default of `GOMAXPROCS` would be wrong for most I/O-bound uses and would hide that the choice exists. `QueueSize`'s zero value is meaningful (rendezvous) rather than a trap-door default, and the docs say so.

## RetryPolicy

| Field | Meaning | Default when `MaxAttempts > 1` |
| --- | --- | --- |
| `MaxAttempts` | Total executions including the first | 1 |
| `InitialBackoff` | Delay before retry 1 | 100ms |
| `MaxBackoff` | Ceiling on any delay | 30s |
| `Multiplier` | Growth factor | 2.0 |
| `Jitter` | Fraction of each delay randomly subtracted, in [0, 1] | 0 (none) |
| `Retryable` | Error classifier | Retry all except `Permanent` |

Methods: `Validate`, `Classify(err)`, `ShouldRetry(err, attempt)`, `Backoff(retry)`. `DefaultRetryPolicy()` returns 3 attempts, 100ms to 5s, multiplier 2, jitter 0.2.

The spec-style alternative, an interface `ShouldRetry(err, attempt) bool`, was considered. A struct with an optional classifier function covers the same needs (custom classification) and keeps the common case a plain composite literal that is comparable, printable and validatable. A custom retry schedule would justify an interface; there is no demand for one here.

## Task, Handle, Result

`Task{Job, ID, Timeout}`. `Handle`: `ID()`, `Cancel()`, `Done()`, `Result()`, `Wait(ctx)`. `Result{JobID, Outcome, Err, Attempts, QueueWait, Duration}`; `Outcome` is `Succeeded`, `Failed`, `Canceled` or `Panicked`.

`Handle.Wait(ctx)` returning `ctx.Err()` does not cancel the job; it only stops waiting.

## Errors

| Error | Returned by / found in | Test with |
| --- | --- | --- |
| `ErrQueueFull` | `TrySubmit`, `TrySubmitTask` | `errors.Is` |
| `ErrClosed` | Submission methods after shutdown begins | `errors.Is` |
| `ErrShutdownTimeout` | `Shutdown` when its context expired first | `errors.Is`; also matches `ctx.Err()` |
| `ErrNilJob` | Submission methods | `errors.Is` |
| `ErrAborted` | `Result.Err` of jobs canceled by abort; `context.Cause` | `errors.Is` |
| `ErrJobCanceled` | `Result.Err` after `Handle.Cancel`; `context.Cause` | `errors.Is` |
| `ErrRetriesExhausted` | `Result.Err` when a retryable error outlasted all attempts | `errors.Is` |
| `ErrPanic`, `*PanicError` | `Result.Err` of a panicked job | `errors.Is(err, ErrPanic)`, `errors.As` |

## Observer, Event, Stats

See [observability.md](observability.md).

## Exported surface

The exported set is deliberately small: the pool, its config and retry policy, the job/task/handle/result types, the observer, the errors, and `Permanent`/`IsPermanent`/`InfoFromContext`. Test seams (the sleeper and random source), the safe-observer wrapper, and the discard log handler are unexported.
