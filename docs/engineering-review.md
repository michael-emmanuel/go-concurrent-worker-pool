# Engineering Review

Questions this repository can answer, with concise answers and the code or test that demonstrates each.

## Design

### Why is the queue bounded?

An unbounded queue hides overload rather than handling it. If arrivals exceed service rate, the backlog grows, so memory grows and, by Little's Law (L = lambda x W), time-in-system grows too. Requests wait so long that clients time out and retry, adding load. The process looks healthy until it runs out of memory. A bounded queue makes the limit explicit: worst-case queueing delay is about QueueSize divided by throughput, and when it is full the producer is told immediately. It moves the overload decision to the edge, where there is context to make it (return 503, slow the producer, shed).

### What happens when the queue is full?

Depends on the method. `TrySubmit` returns `ErrQueueFull` immediately. `Submit(ctx, job)` blocks until a worker frees a slot, the context ends (returns `ctx.Err()`), or shutdown begins (returns `ErrClosed`). Either way the job is not accepted, and the rejection is counted and reported to the observer. The first saturation after a healthy period logs one warning (edge-triggered so it does not spam). Tests: `TestTrySubmitQueueFull`, `TestSubmitBlocksUntilCapacity`, `TestConcurrentTrySubmitNeverExceedsCapacity` (50 goroutines against a queue of 5: exactly 5 accepted).

Follow-up: why not drop the oldest job? It needs removal from the middle of a channel, forces a data-loss policy on every user, and is a few lines of caller code around `TrySubmit` if someone wants it. The pool does not make data-loss decisions for the caller.

### Why not spawn one goroutine per job?

Goroutines are cheap, but the resources jobs use are not. One goroutine per job means concurrency equals arrival rate: a spike becomes thousands of simultaneous database queries or HTTP calls, exhausting connection pools, file descriptors and downstream capacity, and there is no place to apply backpressure. `go f()` also has no built-in cancellation, retry or shutdown story. A fixed pool makes concurrency a configuration value that maps to a real constraint. If concurrency limiting alone is enough, a semaphore around `go f()` works, but then the waiting goroutines are the unbounded queue, which is the problem again.

### How do you prevent goroutine leaks?

By construction and by test. Every goroutine has an owner and a defined exit: workers exit when the queue is closed and drained; a single finalizer exits after closing `Done()`; nothing is spawned per job. Blocking operations all have an exit path: a blocked `Submit` selects on the caller's context and the pool's `stopping` channel; backoff sleeps select on a timer and the job context, and the timer is stopped on every path. Job contexts are always canceled in `finish`. The limits: a job that ignores its context holds its worker, and Go cannot kill it. `TestNoGoroutineLeak` runs pools through submit/shutdown cycles and checks the goroutine count returns to baseline.

### What happens if Submit() races with Shutdown()?

The result is one of two outcomes, both safe: `Submit` returns `ErrClosed` (not accepted), or it returns nil and the job is drained (or, on abort, discarded and reported as canceled). It cannot panic on a closed channel, block forever, or be accepted and silently lost.

The mechanism: submitters take a read lock, check state is `Running`, and register in a `WaitGroup`, then release the lock. Shutdown takes the write lock, flips to `Draining`, closes a `stopping` channel (which wakes blocked submitters), releases the lock, and a finalizer waits on the `WaitGroup` before closing the queue. Because registration happens under the read lock and the state flip under the write lock, no submitter can register after the flip, and the queue is only closed after every registered sender has returned. `TestSubmitRacingShutdown` runs eight looping producers against `Shutdown`, 25 times per run, and asserts accepted equals executed; it runs under `-race`.

Follow-up: why not just `defer recover()` around the send? Recovering a send-on-closed-channel panic would hide the real problem and leave the job's accounting wrong: you would not know whether it was enqueued.

## Retries

### Why is retrying every error dangerous?

Two reasons. First, correctness of effort: retrying non-transient errors (validation failure, authorization, 4xx) cannot succeed and only adds load and latency. Second, amplification: if a downstream is failing, every job retrying multiplies its load, which pushes it further into failure (a retry storm). So retries here are opt-in (default one attempt), errors marked `Permanent` are never retried, `Retryable` lets you classify, panics and cancellation are never retried, and attempts are capped. Also non-idempotent work must not be retried blindly.

### Why add jitter to exponential backoff?

Backoff alone spaces retries in time but keeps them synchronized: 1,000 jobs that failed together retry together at t+100ms, then again at t+300ms, so the downstream sees repeating spikes (thundering herd). Jitter randomizes delays so retries spread across the window. In this implementation jitter subtracts up to a fraction of the delay, so `MaxBackoff` remains a hard ceiling; adding symmetric jitter would exceed the cap, and clamping afterwards would pile retries onto the cap value.

### What is the difference between attempts and retries?

`MaxAttempts` is total executions. With 5 there is one initial attempt and up to four retries. Tests assert the exact backoff sequence using an injected sleeper (`TestRetryTransientThenSuccess` expects 100ms then 200ms) without waiting.

## Sizing and downstream behaviour

### How would you size the worker pool?

From the constraint, not the request rate. For I/O-bound work, Little's Law: workers needed is roughly target throughput times latency (200 jobs/s at 50 ms needs about 10 in flight), then capped by what the downstream tolerates: its connection limit, its rate limit, its CPU. For CPU-bound work, about the number of cores. Then verify with `ActiveWorkers`, queue wait and rejection metrics. Queue size is chosen from the latency you can tolerate: worst-case wait is about QueueSize x job-duration / Workers. Separate pools per downstream so one slow dependency cannot occupy every worker.

### How does this interact with a database connection pool?

If each job holds a connection while running, Workers above `MaxOpenConns` just moves the queue into the driver: workers block waiting for a connection, and the pool's backpressure no longer reflects real capacity. Keep Workers at or below the connections allocated to this workload. Acquire the connection per attempt rather than per job, so a job in retry backoff is not holding a connection while idle. A related trap is a connection pool smaller than the retry-inflated demand causing timeouts that trigger more retries.

### What happens when downstream latency increases?

Each job takes longer, so service rate falls (workers x 1/latency). Workers stay busy, the queue fills, then rejections start. That is backpressure propagating back to the edge, as designed. If retries are enabled and the slowness causes timeouts, retries make it worse: more attempts per job, lower throughput, more load on the struggling service. Mitigations: per-attempt `JobTimeout` so slow calls do not hold workers indefinitely, low `MaxAttempts`, jitter, a small queue so rejection starts early, and alerting on queue wait. What the pool deliberately does not do is add workers automatically; that would send more load to the service that is already slow.

### How would you add rate limiting?

Put a limiter inside the job before the downstream call (`limiter.Wait(ctx)` from `golang.org/x/time/rate`). Waiting inside the job is intentional: the wait shows up as worker occupancy, which shows up as queue growth and then rejections, so the rate limit propagates as backpressure through the existing mechanism instead of needing a second one. Use a shared limiter across the pool's jobs (or one per downstream). Concurrency limiting (workers) and rate limiting (calls per second) are different controls and are usually both needed. Alternative: wrap `TrySubmit` at the edge with a limiter to shed load before it enters the queue.

### How would you distribute this across multiple processes?

The pool is in-process by design; distribution is a different problem: durability and coordination. The common architecture is a durable queue (SQS, Kafka, RabbitMQ, a database table with `SELECT ... FOR UPDATE SKIP LOCKED`) shared by all instances, with each instance running this pool as its bounded local executor: a consumer loop pulls a message, `Submit`s a job that processes it and acknowledges only on success, and pulls the next message only when `Submit` accepts (or uses `TrySubmit` and pauses consumption on `ErrQueueFull`). That preserves per-process backpressure and gives cross-process durability. Things to solve at that layer: visibility timeouts or leases, acknowledgment after completion, idempotent handlers, dead-letter queues, and per-instance concurrency times instance count against downstream limits. Sharding or priorities would live in the broker layer.

## Guarantees

### What guarantees does the pool provide about job execution?

- Bounded resources: at most `Workers` jobs running, at most `QueueSize` waiting.
- Accepted means terminal outcome: every job whose submission returned nil reaches exactly one of Succeeded, Failed, Canceled or Panicked, observable through the observer and `Handle`. Abort does not drop jobs silently; it reports them as canceled. (Exception: a job that calls `runtime.Goexit`.)
- A job never runs concurrently with itself; retries are sequential on one worker.
- FIFO dequeue order from the queue, but no ordering guarantee across workers on completion.
- No execution after `Done()` closes.
- Each attempt is isolated from panics.
- Events: `JobSubmitted` happens before `JobStarted` for a job; a terminal event happens before the handle resolves.

What it does not guarantee: durability, exactly-once, ordering of completion, prompt termination of jobs that ignore their context.

### Can a job execute more than once?

Yes, if retries are enabled: up to `MaxAttempts` times, and a failed attempt may have had side effects (a timeout after the remote committed). It will not execute more than once after it returns nil, and not more than once concurrently. With retries off (the default) a job executes at most once.

### Is the system at-most-once, at-least-once, or exactly-once?

Precisely: within the process, the pool provides at-most-once delivery of each accepted job, with bounded re-execution on failure when retries are on. A job can execute zero times (discarded on abort, canceled while queued, lost on crash). It is not at-least-once, because nothing is persisted or redelivered, and not exactly-once, because a failed attempt can have partial effects and be repeated. Exactly-once is not a property a worker pool can provide alone; it is achieved end-to-end by at-least-once delivery from a durable queue plus idempotent processing.

### How would you make jobs idempotent?

Choose per case. Natural idempotence: upserts, set-to-value, delete-if-exists. Idempotency keys: derive a stable key (business ID, or message ID plus operation) and have the downstream deduplicate on it (Stripe-style idempotency keys; a unique constraint and `INSERT ... ON CONFLICT DO NOTHING`). Record-then-act using a transaction that checks and writes a processed marker atomically with the effect. Conditional writes (compare-and-set on a version). `InfoFromContext` gives the job ID and attempt so an attempt can be logged and, if needed, made part of a key, but for cross-restart idempotency the key must come from the business data, not from the pool's generated IDs.

## Operations

### What happens during Kubernetes termination?

The pod is marked terminating and `SIGTERM` is sent while endpoint removal propagates asynchronously; after `terminationGracePeriodSeconds` comes `SIGKILL`. The sequence I would use: optionally fail readiness and pause briefly for endpoint propagation; `http.Server.Shutdown` to stop new requests and finish in-flight ones; then `pool.Shutdown(ctx)` with a deadline of the grace period minus a margin. Stopping intake before the pool matters, otherwise late requests get `ErrClosed`. If the deadline expires, the pool aborts: running jobs get a canceled context, queued jobs are discarded and reported; log the counts and exit. Anything still queued at `SIGKILL` is lost, which is why work that cannot be lost belongs in a durable queue and the pool is only the executor. `examples/http-api` shows the ordering.

### How would you instrument this with OpenTelemetry?

Two parts, kept out of the core so it stays dependency-free. Metrics: implement `Observer` with OTel instruments (counter for outcomes labeled by outcome, counter for retries, histograms for duration and queue wait, an observable gauge reading `Stats()` for queue depth and active workers). Do not label by job ID (unbounded cardinality). Tracing: the observer cannot wrap job execution, so wrap the job: a `tracedJob` whose `Run` starts a span per attempt, attaches attributes from `InfoFromContext` (job ID, worker, attempt), records errors, and links to the submitting span's context. Trace context does not flow automatically because the job context is deliberately independent of the submit context (a request-scoped context would cancel background jobs when the request ends), so it is captured at submit time and passed explicitly.

## Judgment questions

### What would you change if this had to handle 100x the load?

Measure first. Cheap wins: reduce per-job allocation (about 4 to 5 per job now), avoid `SubmitTask` where a handle is not needed, batch small jobs into one. If the single channel becomes a contention point across many cores, shard into several pools with a hash or round-robin front, which also gives isolation; that is a change I would justify with a benchmark on real multi-core hardware. The provided benchmarks were taken on a single-CPU machine, so I would not claim scaling numbers from them.

### What are you least happy with?

Cooperative timeouts and cancellation are only as good as the job's behaviour; a job that ignores its context can hold a worker indefinitely and there is no in-process fix. The `Goexit` gap is real but obscure. Retry uses no shared budget, so many independent jobs can still amplify load together. In-memory only. And `QueueSize: 0` makes `TrySubmit` racy by nature. These are documented in the README and failure-modes.

### Why a channel and not a mutex-protected queue?

A buffered channel gives bounded capacity, FIFO, blocking with cancellation through `select`, and efficient wake-ups from the runtime scheduler, with nothing to get wrong. A custom queue with a mutex and condition variable would need its own cancellation story, since `sync.Cond` does not compose with contexts. I would switch if I needed priorities, removal of queued items, or inspection, none of which this pool offers. The claim is "sufficient here", not "always better".

### Why are retries inside the worker rather than re-enqueued?

Re-enqueuing lets retries compete with fresh work and can make the queue longer, weakening the bounded-queue guarantee and forcing a decision about queue-full on the retry path. In-place retry keeps total in-flight attempts at most `Workers`. The cost is a worker held during backoff. For very long backoffs (minutes), that cost is too high and the right design is to hand the job to a durable delayed queue instead.

### Why is recovering panics safe, and when is it not?

At the attempt boundary, recovery converts a crash into a reported failure, so one bad job cannot take down every other job. It is risky because a recovered panic can leave shared state inconsistent and can normalise defects. So it is never silent: Error log with stack, observer event, counter, structured `*PanicError`, and never retried. Recovery is for containment, and alerts on the panic count are how the defect gets fixed. Fatal runtime errors (out of memory, concurrent map writes) are not recoverable and are not affected.
