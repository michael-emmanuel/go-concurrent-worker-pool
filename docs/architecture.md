# Architecture

## Components and responsibilities

| Component | Responsibility | Where |
| --- | --- | --- |
| `Pool` | Owns the queue, workers, lifecycle state and counters. The only type with mutable shared state. | `pool.go` |
| Queue | A buffered `chan *item` of capacity `QueueSize`. The only place accepted-but-unstarted work waits. | `pool.go` |
| Worker | A goroutine looping over the queue. Runs attempts, applies the retry policy, reports the outcome. | `pool.go` |
| `Job` / `JobFunc` / `Task` | The unit of work and its per-job options. | `job.go` |
| `Handle` | Optional per-job future: wait for the `Result`, cancel the job. | `handle.go` |
| `RetryPolicy` | Pure value type: classification and backoff arithmetic. No state, no goroutines. | `retry.go` |
| `Observer` | Event sink. Called synchronously; the default does nothing. | `observer.go` |
| `Config` | Validated construction parameters. | `config.go` |

There is no scheduler goroutine and no dispatcher. Workers pull directly from the queue, so the only goroutines are the workers and, transiently during shutdown, one finalizer.

## Data flow

1. A producer calls `Submit`/`TrySubmit` (or the `Task` variants). The pool builds an `item` (ID, job, context, optional handle).
2. `enqueue` checks the caller's context, registers as an in-flight sender (only while `Running`), then sends on the queue channel: non-blocking first, then, for `Submit`, a `select` over the send, `ctx.Done()` and the pool's `stopping` channel.
3. A worker receives the item, waits for the submitter to finish reporting `JobSubmitted`, and calls `process`.
4. `process` discards the item if its context is already canceled (job canceled while queued, or pool aborted). Otherwise it reports `JobStarted` and calls `run`.
5. `run` loops: execute one attempt inside the panic boundary, then classify the result. Success, panic, cancellation and non-retryable errors end the loop. A retryable error with attempts remaining reports `JobRetried`, sleeps the backoff on the job's context, and loops.
6. `finish` updates counters, logs, calls the observer, then resolves the `Handle` (if any) and releases the job context.

## Job states

```
queued --> running --> succeeded
   |          |  ^
   |          |  +---- backoff (retry) ----+
   |          +--> failed
   |          +--> canceled
   |          +--> panicked
   +--> canceled   (canceled while queued, or discarded by an abort; never ran)
```

Every accepted job leaves this graph through exactly one terminal state. There is no path on which an accepted job disappears without a terminal event, short of the process dying.

## Pool states

```
Running  --Shutdown/Close-->  Draining  --queue drained, workers exited-->  Stopped
```

- `Running`: submissions accepted.
- `Draining`: submissions rejected with `ErrClosed`; queued and running jobs proceed. If an abort occurs, this state is where running jobs are canceled and queued jobs discarded. Abort is an action within `Draining`, not a fourth state, because it changes what happens to jobs, not what the pool accepts.
- `Stopped`: all workers have returned; `Done()` is closed.

Transitions are forward-only and performed under `mu`. `Running -> Draining` happens exactly once (the caller that wins the lock owns it). `Draining -> Stopped` happens exactly once, in the finalizer goroutine.

## Concurrency boundaries

- Producer goroutines never touch worker state; they only send on the channel and update atomic counters.
- Worker goroutines never touch producer state; they only receive.
- The state machine (`mu`) is the only lock, and it is never held across a blocking operation: submitters hold the read lock only long enough to check the state and increment a `WaitGroup`.
- Job code and observer callbacks run on caller/worker goroutines without any pool lock held, so a slow observer can slow the goroutine that invoked it but cannot deadlock the pool's own state.

One narrow exception: each `item` has a `gate` mutex, held by the submitter from before the send until `JobSubmitted` has been reported, and briefly acquired and released by the worker after receive. It exists to guarantee the event order `JobSubmitted` before `JobStarted`. It is per-item, uncontended in the common case, and cannot participate in a cycle because the submitter never waits for anything while holding it except the send itself, at which point the item is not yet visible to any worker.

## Why no internal/ directory

Everything unexported already lives behind the package boundary of `workerpool`. An `internal/` tree would add indirection without hiding anything further.

## Design decisions in depth

Each decision states what was chosen, what was rejected, and the condition under which the choice would flip.

**Channel queue versus a mutex-protected queue.** A buffered channel is sufficient here because the requirements are bounded FIFO, blocking with cancellation, and efficient wake-up, and `select` gives all three. A mutex plus `sync.Cond` needs its own cancellation mechanism (`Cond.Wait` cannot be selected on with a context), which is where custom queues usually go wrong. A custom structure earns its complexity if you need priorities, removing or inspecting queued items, or dropping the oldest entry; this pool offers none of those. Channel operations take an internal lock, so they are not free under heavy contention, but no measurement here suggests that matters relative to job cost.

**Fixed workers versus dynamic scaling.** The worker count is the concurrency limit that protects whatever the jobs call. Growing it under load sends more traffic to a system that is probably the reason the queue is filling. Dynamic pools also add a controller (when to grow, when to shrink, hysteresis) that is a source of oscillation. Flip the decision when jobs are cheap, downstream capacity is elastic, and you have a measured signal to scale on.

**Bounded versus unbounded queue.** Bounded, see [backpressure.md](backpressure.md). There is no configuration for unbounded, on purpose.

**Blocking versus rejecting.** Both exist because the right answer depends on who the producer is. A request handler must not wait (`TrySubmit`); a batch loader should wait (`Submit`). Neither is default in the sense of a hidden global setting; the caller chooses per call.

**Pool-owned retries versus caller-owned.** See [retries.md](retries.md). In short: the pool can bound total attempts, cancel backoff on shutdown, and report uniformly. The cost is that a backing-off job holds a worker, which is why very long delays belong in a durable delayed queue.

**Context design.** The job context comes from the pool and the submit context bounds only admission. This separates "how long will I wait to hand this over" from "how long may this run", which are different questions with different owners.

**Panic recovery.** At the attempt boundary only. Recovering higher (in the worker loop) would lose the attempt context; recovering lower is the job's own business. Panics are reported loudly and never retried.

**Observer versus a metrics library.** An interface with one `Event` struct: no vendor dependency, new fields do not break implementers, and the no-op default costs a method call. The cost is that tracing cannot be done from events alone (see [observability.md](observability.md)).

**Shutdown semantics.** Drain by default with an explicit abort on deadline. The alternative of abandoning queued work immediately is `Close`. A separate `Abort` method was not added: `Shutdown(expiredContext)` and `Close` already cover it, and more entry points would make the state machine harder to explain.

**Tracked and untracked submission.** `Handle` allocates a channel and a cancelable context per task. Making it opt-in keeps the fire-and-forget path lean.

**In-place execution of attempts.** Each attempt runs synchronously on its worker; there is no goroutine spawned to enforce timeouts. Timeouts are therefore cooperative, in exchange for never having two executions of the same job overlap.

## Performance characteristics

Theoretical:

- Submission: expected O(1); a channel send plus atomic increments, one small allocation set per job. `TrySubmit` on a full queue is O(1) and allocation-light.
- Queue: O(1) enqueue and dequeue (ring buffer inside the channel).
- Worker execution: dependent on the job; pool overhead per job is a channel receive, a few atomics, a context per attempt, and observer/logger calls (no-ops by default).
- Memory: O(QueueSize + Workers) for the pool itself, plus what the queued jobs retain.

Measured (see README for the machine and the caveats): roughly 0.6 microseconds per job for submit-plus-execute of a no-op job with a buffered queue, about 1.3 to 1.4 microseconds with an unbuffered queue, 4 to 5 allocations and about 210 bytes per job, and about 0.26 microseconds for a rejected `TrySubmit` on a full queue. These are overhead figures on one single-CPU virtual machine and should not be extrapolated to other hardware or to multi-core scaling.

Real-world throughput is set by job duration, CPU, I/O, downstream latency and limits, synchronization and queue contention, and worker count; on realistic jobs the pool's overhead is negligible next to the job, which is why the design optimizes for clarity and correctness over micro-optimization.
