# Concurrency Model

## Goroutines and their lifecycle

| Goroutine | Started by | Ends when |
| --- | --- | --- |
| Worker (`Workers` of them) | `New` | The queue is closed and empty. |
| Finalizer (at most one) | First `Shutdown`/`Close` | It has closed `Done()`. |
| Producers | The caller | Caller's decision. Not owned by the pool. |

The pool never starts a goroutine per job and never starts a goroutine that can outlive `Done()`. The test `TestNoGoroutineLeak` creates and stops pools repeatedly and checks the process goroutine count returns to its baseline.

## Ownership

- The queue channel is owned by the pool. Producers hold the right to send only while registered in `senders`; workers hold the right to receive; only the finalizer closes.
- An `item` is created by the submitter, then handed off through the channel. After the send, the submitter does not touch the item except through the `gate` mutex it still holds (see below); the worker owns it from receive onward.
- A `Handle` is written by the worker exactly once (`resolve`) and read by any number of goroutines after `Done()` is closed.

## Synchronization primitives and what each protects

| Primitive | Protects |
| --- | --- |
| `chan *item` (buffered) | FIFO transfer of items; bounded capacity is the backpressure mechanism. |
| `sync.RWMutex mu` + `state` | The `Running -> Draining` transition and the guarantee that no new sender registers afterwards. |
| `sync.WaitGroup senders` | Counts in-flight submissions so the queue is closed only when none can still send. |
| `sync.WaitGroup workers` | Lets the finalizer know all workers have returned. |
| `stopping` channel | Wakes submitters blocked on a full queue when shutdown begins. |
| `done` channel | Broadcasts `Stopped`. |
| `context` (base context with cancel cause) | Broadcasts abort to running jobs, backoff sleeps and queued items. |
| `atomic` counters | Statistics. No ordering role. |
| `item.gate` mutex | Orders `JobSubmitted` before `JobStarted`. |

## The critical race: Submit versus Shutdown

The dangerous interleaving is a producer executing `queue <- item` while shutdown executes `close(queue)`. Sending on a closed channel panics. The protocol:

Submitter:
```
mu.RLock()
if state != Running { RUnlock; return ErrClosed }
senders.Add(1)
mu.RUnlock()
defer senders.Done()
select { queue <- item | ctx.Done() | stopping }
```

Shutdown:
```
mu.Lock(); state = Draining; close(stopping); mu.Unlock()
// finalizer:
senders.Wait()      // every registered submitter has returned
close(queue)
workers.Wait()
```

Why it is correct:

1. A submitter either observed `Running` under the read lock, or it did not. If not, it never sends.
2. If it did, it called `senders.Add(1)` before releasing the read lock. Shutdown takes the write lock, so it cannot proceed until that read lock is released; therefore the `Add` happens before shutdown's later `senders.Wait()`. This also satisfies the `WaitGroup` rule that `Add` with a positive delta on a zero counter must happen before `Wait`.
3. Any submitter blocked on a full queue is woken by `stopping` (closed inside the write-locked section), so `senders.Wait()` returns promptly instead of waiting for space that may never come.
4. Only after `senders.Wait()` returns is `close(queue)` executed, at which point no goroutine can be sending.

A submitter's `select` may see both "space available" and "stopping" ready; Go picks randomly. Either outcome is safe: if the send wins, the item is on the queue and will be drained (or discarded and reported by an abort); if `stopping` wins, the caller gets `ErrClosed` and the item is not enqueued. In both cases the caller's return value tells the truth about whether the job was accepted.

`TestSubmitRacingShutdown` runs this race 25 times per invocation with eight producers submitting in a loop while `Shutdown` runs, and checks that the number of jobs whose `Submit` returned nil equals the number of jobs executed. The race detector runs over the same test in CI.

## Happens-before relationships that matter

- A successful send on the queue happens-before the corresponding receive (Go memory model, channels). So everything the submitter wrote into the `item` before sending is visible to the worker.
- `Handle.resolve` writes `res` and then closes `done`; a close happens-before a receive that returns because of it. So a goroutine that observed `Done()` closed sees a fully written `Result`.
- `finish` calls the observer before `resolve`. So a goroutine woken by `Handle.Wait` may rely on the observer having already seen the terminal event. Tests use this to avoid sleeping before asserting on observer state.
- `item.gate`: the submitter locks it before the item can be received, unlocks it after `JobSubmitted` returns. The worker locks and unlocks it after receive. Therefore `JobSubmitted` happens-before `JobStarted` for the same job.
- The finalizer's `workers.Wait()` happens-before `close(done)`, so `Done()` closed implies every worker (and every job) has returned.

## Avoiding busy loops and leaks

- Workers block in a channel receive; no polling anywhere.
- Backoff uses a `time.Timer` that is always stopped (`defer t.Stop()`), so canceled backoffs do not leave timers behind.
- Per-job contexts created for tracked tasks are always cancelled in `finish`.
- The pool does not rely on finalizers.

## What is not synchronized

`Stats()` reads independent atomics, so the snapshot is not a consistent cut: it may show a job counted as submitted but not yet started, or counters read a moment apart. This is intentional; consistent snapshots would require a lock on the hot path for no operational benefit.
