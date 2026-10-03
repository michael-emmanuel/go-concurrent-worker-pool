# Backpressure

## Why the queue is bounded

A queue absorbs the difference between how fast work arrives and how fast it is completed. If arrivals exceed completions for long enough, an unbounded queue grows without limit:

- Memory grows with the backlog, until the process is killed.
- Latency grows with the backlog. By Little's Law, the average number of items in a stable system equals arrival rate times average time in system (`L = lambda * W`). If the queue keeps growing, the system is not stable, and `W` grows with it. A request that waits ten minutes in a queue is often already useless to its client, which has timed out and retried, adding more load.
- The failure is delayed and indirect. The process looks healthy until it is not.

A bounded queue converts that into a constant, local signal: when it is full, the producer knows now.

Worked example: 8 workers, each job takes 100 ms, so capacity is about 80 jobs/s. With a queue of 100, a job accepted into a full queue waits roughly 100 / 80 = 1.25 s before starting (plus its own 100 ms). That is the worst-case queueing delay for an accepted job; it is a property you can choose by choosing `QueueSize`. With an unbounded queue there is no such bound.

## What "full" means

`Workers` jobs are running and `QueueSize` jobs are waiting. At that point:

- `TrySubmit` returns `ErrQueueFull`.
- `Submit` blocks until a worker takes a job off the queue, its context ends, or shutdown begins.

The first rejection after a period of health logs a single Warn ("queue saturated"); it re-arms when depth falls to half capacity. Every rejection is reported to `Observer.JobRejected` and counted in `Stats().Rejected`.

## The policies and their tradeoffs

| Policy | Provided? | Producer experience | Good for | Cost |
| --- | --- | --- | --- | --- |
| Block (with context) | Yes: `Submit` | Waits; latency absorbs the imbalance | Batch pipelines, internal producers that can be slowed | Blocked producers hold resources (goroutines, connections, memory for their payload). If the producer is a request handler this just moves the unbounded queue into the HTTP server. Always pass a deadline. |
| Reject | Yes: `TrySubmit` | Immediate error | Request/response services that can return 503; callers with a retry/backoff of their own | Caller must handle the error; naive callers retry immediately and amplify load. Return `Retry-After`. |
| Drop (newest or oldest) | No | Silent or counted loss | Telemetry, sampling, "latest value wins" | Correctness: dropping silently loses work. Can be built on `TrySubmit` by ignoring `ErrQueueFull` and counting. Dropping the oldest requires removing from the middle of a channel, which the design excludes. |
| Caller-runs | No | The submitting goroutine executes the job itself | Natural throttling of an in-process producer | Bypasses the worker limit (the whole point of the pool), executes on an unexpected goroutine, and can run arbitrarily long inside a request handler. Can be built by the caller: `if errors.Is(err, ErrQueueFull) { job.Run(ctx) }`, knowing that trade. |
| Unbounded buffer | No | Never blocks | Nothing in a long-running service | Memory and latency grow without bound; failure is delayed. |

Dropping and caller-runs are one line of caller code around `TrySubmit`. They are left out of the API so that the package never makes a data-loss or concurrency-limit decision on the caller's behalf.

## Producer/consumer imbalance

If producers outpace workers persistently, no queue size fixes it; a larger queue only delays the moment it fills and increases the delay each accepted job experiences. The remedies are: more downstream capacity (more workers, if the downstream can take it), less demand (rate limiting or shedding at the edge), or faster jobs. The queue's role is to absorb bursts shorter than `QueueSize / (arrival rate - service rate)`, not sustained overload.

## Why more workers do not mean more throughput

Throughput is limited by the scarcest resource a job uses. Once workers saturate CPU (for CPU-bound jobs), a database connection pool, or an upstream rate limit, additional workers add contention and queueing at that resource rather than completed jobs. For I/O-bound jobs, useful concurrency is roughly `throughput_target * latency` (Little's Law again): to complete 200 jobs/s at 50 ms each, about 10 in flight are needed; 100 workers would not go faster and would put 100 concurrent requests on the downstream.

## Interaction with retries

Retries execute on the worker and do not re-enter the queue, so they cannot make the queue longer. They do reduce effective service rate: a worker in backoff or re-executing is not taking new jobs. A downstream outage with `MaxAttempts: 5` can turn a worker's per-job time from 100 ms into seconds, which drops throughput and fills the queue, which in turn causes rejections. That is backpressure working as intended, but it is the reason retry limits should be low and the queue small enough that rejections start early.

## `QueueSize: 0`

An unbuffered channel: a submission succeeds only if a worker is parked in receive at that instant. This is the strictest backpressure (nothing is ever queued), but `TrySubmit` can fail spuriously even when a worker is about to become idle. Prefer a small buffer unless you specifically want rendezvous semantics.
