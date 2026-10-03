# Graceful Shutdown

## Definition

`Shutdown(ctx)` means: stop accepting work, finish the work already accepted, wait until the workers have exited, but never wait longer than `ctx` allows.

| Question | Answer |
| --- | --- |
| Are new submissions accepted after shutdown begins? | No. `Submit` and `TrySubmit` return `ErrClosed`. |
| What happens to callers blocked in `Submit` when shutdown begins? | They return `ErrClosed` without enqueuing. |
| Are queued jobs drained? | Yes, by graceful shutdown. They run normally. |
| Are running jobs allowed to finish? | Yes. |
| What about jobs in retry backoff? | They continue retrying; drain time includes their remaining backoff. |
| What if `ctx` expires first? | The pool is aborted (below) and `Shutdown` returns an error matching `ErrShutdownTimeout` and `ctx.Err()`. |
| Is `Shutdown` idempotent? | Yes; safe to call any number of times, concurrently. |
| Is anything lost? | Only on abort: queued jobs are discarded, running jobs are canceled. Each is reported as `Canceled`; none disappears silently. |

## State machine

```
Running --(first Shutdown or Close)--> Draining --(queue empty, workers exited)--> Stopped
```

- Transitions are forward-only. There is no way to restart a pool; create a new one.
- The `Running -> Draining` transition happens under the write lock, exactly once. The caller that performs it starts a finalizer goroutine and is the "first" caller (see below).
- The finalizer waits for in-flight submissions to finish, closes the queue, waits for the workers, sets `Stopped`, and closes `Done()`.

## Abort

Abort is what happens when patience runs out. It is triggered by an expired `Shutdown` context (first caller only) or by `Close`. It:

1. Cancels the pool's base context with cause `ErrAborted`. Every running job's context, and every backoff sleep, observes this.
2. Causes workers to discard remaining queued jobs without running them. Each is reported via `JobCanceled` with `Result.Attempts == 0` and an error wrapping `ErrAborted`.

Abort does not kill goroutines. A running job that ignores its context keeps its worker until it returns on its own. `Shutdown` therefore returns immediately after triggering an abort rather than waiting; use `<-pool.Done()` (optionally with your own timeout) or `Close()` if you need to wait for the workers.

## Who owns the deadline

Only the first caller of `Shutdown` may abort the pool with its context. Later callers wait for the pool to stop and return `nil`, or their own `ctx.Err()` if their context ends first, without aborting anything. Otherwise a stray short-lived caller (a health check, a test cleanup) could abort a shutdown that the deployment system intended to be patient.

An already-expired context passed to `Shutdown` is a request to abort immediately.

## `Close`

`Close()` is `Shutdown` with no patience: begin draining, abort at once, wait for workers to exit. It always returns `nil` and exists so `*Pool` satisfies `io.Closer` and works with `defer`/`t.Cleanup`. It is not redundant with `Shutdown(expiredCtx)`, which does not wait for the workers.

## Submit racing Shutdown

See [concurrency-model.md](concurrency-model.md) for the proof. Outcome for any submission concurrent with shutdown: it either returns `ErrClosed` (not accepted) or `nil` (accepted, and it will reach a terminal outcome). It cannot panic, block forever, or be accepted and then silently lost.

## Recommended sequence for a service

1. Stop the source of work first (`http.Server.Shutdown`, stop consuming from a message queue). Otherwise submissions during pool shutdown are rejected with `ErrClosed`, which may be surfaced to clients as errors.
2. `pool.Shutdown(ctx)` with a deadline shorter than the platform's kill timeout (for example, `terminationGracePeriodSeconds` minus a safety margin).
3. If it returns `ErrShutdownTimeout`, log how much was discarded (`Stats().Canceled`), and exit.

For work that must not be lost on abort, the pool alone is the wrong tool; jobs need to be durable elsewhere and re-driven after restart.
