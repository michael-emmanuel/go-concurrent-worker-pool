# Production Considerations

## Sizing workers

The worker count is a concurrency limit on whatever the jobs touch. Start from the constraint, not from the request rate.

- CPU-bound jobs: about `GOMAXPROCS` workers. More adds scheduling overhead, not throughput.
- I/O-bound jobs: use Little's Law. Concurrency needed = target throughput x per-job latency. For 200 jobs/s at 50 ms, about 10. Add headroom for variance, then cap by what the downstream tolerates.
- Mixed workloads: separate pools per downstream so a slow dependency cannot occupy every worker (the bulkhead pattern). One pool per failure domain is cheap; goroutines are cheap and the pool has no global state.

Measure rather than guess: watch `ActiveWorkers` (persistently at `Workers` means no headroom; persistently far below means over-provisioned) and queue wait.

## Sizing the queue

Queue capacity sets the worst-case queueing delay for an accepted job: about `QueueSize / (Workers / job_duration)`. Choose it from the latency you can tolerate, not from "big enough not to reject".

- A queue absorbs bursts, not sustained overload. If demand exceeds capacity for longer than the queue can cover, requests are rejected regardless of size; a larger queue only adds latency first.
- Memory is `O(QueueSize + Workers)` items plus whatever each job holds. If jobs carry large payloads, capacity times payload size is your memory bound.
- Small queues make rejections start early, which is usually what you want in a request path so callers can back off or fail over. Large queues fit offline pipelines where latency is irrelevant.

## Downstream limits

The pool is the natural place to enforce "at most N concurrent calls to X". Set `Workers` at or below what X can handle, and remember retries: with retries enabled, attempts in flight never exceed `Workers`, but the total call rate to X can exceed the job rate by up to `MaxAttempts`.

Database connection pools: if a job holds a connection while it runs, `Workers` above the pool's `MaxOpenConns` just moves the queue into the driver (workers block waiting for a connection), and the pool's own backpressure no longer reflects real capacity. Keep `Workers` at or below the connections you are willing to give this workload, and give it a share, not the whole pool, if other code needs connections. If a job holds a connection across a retry backoff, it also holds a connection while doing nothing; acquire the connection per attempt, not per job.

Rate limits: concurrency limits and rate limits are different. Workers bound how many calls are in flight; they do not bound calls per second when calls are fast. Add a limiter inside the job (`limiter.Wait(ctx)` from `golang.org/x/time/rate`) before the downstream call. Waiting inside the job makes rate limiting visible as worker occupancy and therefore as queue growth and backpressure, which is the desired propagation. A limiter that rejects instead of waits should return `Permanent`-style errors or a dedicated retryable error with backoff at least as long as the limit's refill interval.

## Retry amplification

See [retries.md](retries.md). In production: keep `MaxAttempts` low, always use jitter, classify errors (do not retry 4xx or validation failures), make jobs idempotent, and avoid retrying at two layers.

## CPU-bound versus I/O-bound

For I/O-bound jobs the pool mostly waits, so worker count can exceed cores. For CPU-bound jobs the cap is cores. Do not use one pool for both: a CPU-heavy job holds a worker (and a core) for its duration and starves short I/O jobs sharing the pool.

## Memory

- Fixed: the queue's buffer (one pointer per slot), `Workers` goroutines (a few KB of stack each initially).
- Per accepted job: one small `item` plus whatever the job closure captures. Benchmarks measured about 200 bytes and 4 to 5 allocations per job of pool overhead on the machine described in the README.
- Per tracked task (`SubmitTask`): additionally a `Handle` and a cancelable context. If you do not need the handle, use `Submit`.
- Jobs that retain large captured data stay live until they finish, including while in the queue.

## Observability

Wire an `Observer` and export `Stats()`. Minimum useful set: rejection count, queue depth and wait time, active workers, terminal outcomes, retry count, panic count. See [observability.md](observability.md).

## Graceful termination in orchestrated environments

Typical Kubernetes sequence: the pod is marked terminating and removed from Service endpoints while `SIGTERM` is sent; after `terminationGracePeriodSeconds` the container gets `SIGKILL`. Endpoint removal propagates asynchronously, so new requests can still arrive for a short time after `SIGTERM`.

1. On `SIGTERM`, optionally fail readiness first and wait a few seconds for endpoint propagation.
2. `http.Server.Shutdown` (stop accepting, finish in-flight requests).
3. `pool.Shutdown(ctx)` with a deadline of the grace period minus a margin for steps 1, 2 and process exit.
4. If it returns `ErrShutdownTimeout`, log discarded counts and exit.

Anything in the queue when `SIGKILL` arrives is lost; the pool is in-memory. If that is unacceptable, persist jobs elsewhere and treat the pool as a bounded executor of durable work (see the engineering review md).

## What this package does not do

No persistence, no cross-process coordination, no priority, no scheduling of future runs, no autoscaling, no dead-letter queue, no shared retry budget. Each is a reasonable extension; none is hidden inside the core.
