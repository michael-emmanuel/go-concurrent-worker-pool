# Testing

## Commands

```sh
go test ./...                 # unit and concurrency tests
go test -race ./...           # the same under the race detector (also run in CI)
go test -race -count=3 ./...  # repeat to shake out scheduling-dependent failures
go test -race -cpu 1,2,4,8 ./workerpool   # vary GOMAXPROCS
go vet ./...
go test -run '^$' -bench . -benchmem ./workerpool
```

The race detector requires cgo (a C toolchain) on Linux. It instruments memory accesses at run time and reports unsynchronized concurrent accesses that actually occur during the run, so it is only as good as the tests that provoke concurrency. Passing `-race` is necessary, not sufficient; the tests below are written to create the interleavings.

## Determinism: how tests avoid sleeping

Tests never use `time.Sleep` to wait for a condition. The mechanisms:

- Channels as signals. A `gate` job body sends on `started` when it begins and blocks on `release`; the test receives from `started` to know a worker is busy, and closes `release` to let it continue.
- Barriers. `TestWorkersRunConcurrently` has N jobs that each wait for all N to have started; it can only pass if N workers really run concurrently. No timing involved.
- Handles. `Handle.Wait` blocks until a job is terminal, and the observer has been notified before it returns.
- An injected sleeper and random source (unexported `hooks`). Retry tests replace the backoff sleep with a recorder that returns immediately, so a test with a `time.Hour` backoff runs instantly, and assert on the exact delays requested (`[100ms 200ms]`). A blocking sleeper that waits for context cancellation tests cancel-during-backoff without any elapsed time.
- Watchdog timeouts (10 s) exist only so that a broken test fails instead of hanging. A correct run never waits for them.

Where a real timer is used (per-attempt timeout tests use 1 ms), the assertion is on what must eventually happen, not on how long it takes.

Two limitations, stated plainly: the check that `Submit` "is blocked" while the queue is full cannot be made airtight without observing goroutine state, so `TestSubmitBlocksUntilCapacity` asserts the call has not returned at a point where it should not have, which can miss a bug but cannot fail spuriously. And `TestNoGoroutineLeak` polls the goroutine count until it returns to baseline; it runs sequentially with other tests so other pools do not inflate the count.

## What is covered

Unit: basic execution, multiple workers, queue capacity, queue full, blocking submission, context cancellation and deadline, submit-context independence from the job context, shutdown drain, submit after shutdown, idempotent shutdown, empty pool, abort on expired shutdown context, `Close`, deadline ownership (a later caller cannot abort), retry success/exhaustion/non-retryable/no-retry-by-default, cancel during backoff, panic recovery (value, stack, IDs, worker survival, no retry, error-valued panics), per-attempt timeout and per-task override, per-job cancellation (running and queued), `Handle` semantics, `InfoFromContext`, observer event order, panicking observer, panicking classifier, stats, IDs, configuration validation, backoff arithmetic and jitter bounds, logging levels and edge-triggered saturation warning.

Concurrency: many concurrent producers; saturation with 16 producers blocked on a full queue; `Submit` racing `Shutdown` (invariant: accepted == executed); concurrent `Shutdown` callers; jobs completing during shutdown with `Stats` and `State` read concurrently; abort accounting (every accepted job has exactly one terminal outcome); 100,000 jobs through 8 workers; `TrySubmit` from 50 goroutines against a queue of 5 accepts exactly 5.

Coverage on the last run: 97.5% of statements in `workerpool` (`go test -coverprofile`). Coverage shows lines executed, not interleavings explored; the concurrency tests matter more than the number.

## Benchmarks

| Benchmark | What it isolates |
| --- | --- |
| `BenchmarkSubmit/workers=W/queue=Q` | Submit plus execute of a no-op job, W in {1, 4, 16}, Q in {0, 16, 1024}. |
| `BenchmarkSubmitParallel/workers=W` | Many producers contending on the queue. |
| `BenchmarkExecuteCPU/workers=W` | A small fixed CPU job, to compare overhead with real work. |
| `BenchmarkTrySubmitFull` | The rejection fast path. |

None depends on sleeping. Each drains the pool before the timer stops so all submitted work is counted. Numbers depend heavily on the machine; the README reports one run and the machine it came from. On a single-CPU host, worker count cannot demonstrate scaling.

## CI

`.github/workflows/ci.yml` runs on Go 1.22 and 1.24: gofmt check, `go vet`, `go test`, `go test -race`, build of all examples, and a one-iteration run of every benchmark to keep them compiling. A separate job runs `golangci-lint` (default linters plus `errorlint`, `bodyclose`, `noctx`, `copyloopvar`, `unconvert`, `misspell`).
