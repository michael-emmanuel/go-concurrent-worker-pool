# Retries

## Attempts versus retries

`MaxAttempts` counts executions of `Run`, including the first. A retry is any attempt after the first.

```
MaxAttempts = 5

attempt 1
  backoff before retry 1
attempt 2   (retry 1)
  backoff before retry 2
attempt 3   (retry 2)
  backoff before retry 3
attempt 4   (retry 3)
  backoff before retry 4
attempt 5   (retry 4)
```

`MaxAttempts: 1` (or the zero value) means no retries. `Result.Attempts` reports how many times `Run` was called; `Stats().Retries` counts scheduled retries.

## When to retry: transient versus permanent

Retrying helps when the next attempt has a materially different chance of succeeding: a timeout, a 503, a connection reset, a lock conflict. It hurts when it does not: invalid input, authorization failure, a 4xx, a violated constraint. Retrying those wastes worker time and load on the downstream, and delays the failure signal.

The default classifier retries every error except ones wrapped with `Permanent`. That is a deliberate compromise: the pool cannot know which errors are transient in your domain, so the safe default is to make the job say so explicitly (`return workerpool.Permanent(err)`), or to supply `RetryPolicy.Retryable`:

```go
Retryable: func(err error) bool {
    var ne net.Error
    return errors.As(err, &ne) && ne.Timeout() || errors.Is(err, errServiceUnavailable)
},
```

Never retried regardless of classifier:

- A panic. It signals a defect; running the same code again with the same input will usually panic again, and the state it left behind may be inconsistent.
- Any outcome where the job's own context is done (`Handle.Cancel`, pool abort). Cancellation is a request to stop, not a failure to fix.

A per-attempt timeout (`JobTimeout`) is different: the job context is live, only that attempt's derived context expired, so the resulting `context.DeadlineExceeded` is an ordinary error and is retried by default.

## Exponential backoff

Delay before retry `n` (1-based):

```
d = min(InitialBackoff * Multiplier^(n-1), MaxBackoff)
delay = d - d * Jitter * r        with r uniform in [0, 1)
```

Example: `Initial 100ms, Multiplier 2, Max 5s`: 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, 5s, 5s, ...

Exponential growth gives a struggling dependency progressively more room to recover, while keeping early retries fast for the common case of a brief blip. The cap prevents absurd waits.

## Jitter

Without jitter, jobs that failed together retry together. If a downstream blip fails 1,000 jobs at the same instant, they all retry at `t + 100ms`, then all again at `t + 300ms`. Each wave is as large as the first; the downstream sees synchronized spikes (the thundering herd). Randomizing the delay spreads the retries over a window and lets the dependency recover.

This implementation subtracts a random fraction up to `Jitter` from the delay, so the delay is in `[d * (1 - Jitter), d]` and `MaxBackoff` remains a true ceiling. (Adding jitter symmetrically would sometimes exceed the cap; clamping afterwards would pile many retries on exactly the cap value, re-creating the herd at the cap.) A `Jitter` of 0.2 gives modest spreading; larger values spread more but make each individual delay less predictable. `Jitter: 0` disables it, which is almost never what you want in production; `DefaultRetryPolicy()` uses 0.2.

The random source is the package-level `math/rand/v2` generator, which is safe for concurrent use and needs no shared state in the pool.

## Retry storms and amplification

If a downstream is failing, every job retrying multiplies the load on it by up to `MaxAttempts`. A system at 80% capacity that starts retrying 3x on failure is immediately at far above 100%, which causes more failures, which causes more retries. Mitigations built in:

- Retries occupy the worker; they do not add jobs to the queue. Total in-flight attempts never exceed `Workers`.
- The default is no retries. Low `MaxAttempts` (2 to 3) is usually correct.
- Backoff plus jitter spreads the load in time.
- The bounded queue turns the slowdown that retries cause into rejections at the edge instead of unbounded backlog.

Not built in, and worth knowing about: a retry budget (cap retries to, say, 10% of recent requests) is the standard defence against amplification across many independent jobs. See future extensions.

## Where retries belong

Options: in the caller, in the job body, in the pool, or in the downstream client library. This package places them in the pool because:

- The pool already owns concurrency, cancellation and observability, so it can bound total attempts, cancel backoff on shutdown and abort, and emit retry events uniformly.
- Retrying in the caller requires every caller to reimplement backoff and to keep waiting after they have handed the work off.
- Retrying inside the job body hides attempts from the observer and makes it hard to cancel promptly.

Retries at more than one layer multiply: a client library that retries 3 times inside a job that the pool retries 3 times yields 9 calls. Pick one layer per dependency.

## Idempotency

A retried job may execute more than once, and an attempt that "failed" may have partly or fully succeeded (a timeout after the server committed). Jobs that are retried must therefore be idempotent, for example by carrying an idempotency key the downstream deduplicates on, or by being naturally idempotent (an upsert, a set-to-value). `InfoFromContext(ctx)` exposes the job ID and attempt number, which can serve as an idempotency key component.

## Interaction with shutdown and cancellation

- A job in backoff during a graceful `Shutdown` continues retrying; drain time includes remaining backoff. The shutdown deadline is the bound; after it expires the abort cancels the backoff immediately.
- `Handle.Cancel` during backoff ends the wait immediately and the job is reported `Canceled` with the attempts made so far.
- The backoff sleep is a `select` on a timer and the job context; no `time.Sleep`.
