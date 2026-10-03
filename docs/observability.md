# Observability

The core package has no dependency on any metrics or tracing system. It offers three seams: an `Observer` for events, `Stats()` for a pull snapshot, and a `*slog.Logger` for logs.

## Observer

```go
type Observer interface {
    JobSubmitted(Event)   // accepted onto the queue
    JobRejected(Event)    // refused: queue full, closed, or caller context ended
    JobStarted(Event)     // first attempt began
    JobRetried(Event)     // an attempt failed; retry after Event.Backoff
    JobCompleted(Event)   // terminal: success
    JobFailed(Event)      // terminal: failure (including exhausted retries)
    JobCanceled(Event)    // terminal: canceled (including discarded from the queue)
    JobPanicked(Event)    // terminal: panic; Event.Err is *PanicError
}
```

`Event` fields: `JobID`, `WorkerID`, `Attempt`, `QueueDepth`, `ActiveWorkers`, `QueueWait`, `Duration`, `Backoff`, `Retries`, `Err`. Each method's doc comment lists which fields are set.

Contract:

- Methods are called synchronously on the goroutine that produced the event: the caller for `JobSubmitted`/`JobRejected`, a worker for the rest. Keep them fast and non-blocking (increment counters, record into histograms, hand off to a channel); a slow observer slows that goroutine.
- Implementations must be safe for concurrent use.
- Ordering: for one job, `JobSubmitted` happens-before `JobStarted` (guaranteed), and a terminal event happens before the `Handle` is resolved. Events of different jobs have no relative ordering.
- A panic in an observer is recovered and logged at Error; it cannot kill a worker or lose the job's outcome.
- The default is `NopObserver`. Embed it to implement a subset.

The "requested" metrics map as follows:

| Metric | Source |
| --- | --- |
| Submitted, started, completed, failed, canceled, panicked | The corresponding events, or `Stats()` counters |
| Retry count | `JobRetried` events, `Stats().Retries`, `Event.Retries` on terminal events |
| Queue depth | `Event.QueueDepth` (sampled at events) or `Stats().QueueDepth` (sampled at scrape) |
| Active workers | `Event.ActiveWorkers` or `Stats().ActiveWorkers` |
| Execution duration | `Event.Duration` on terminal events (includes backoff) |
| Queue wait | `Event.QueueWait` |

For gauges, prefer `Stats()` polled by the exporter over event-sampled values: a gauge sampled only when events happen is stale during idle periods.

## Sketch: Prometheus

Kept out of the module so the core stays dependency-free; a separate module or your application can do this:

```go
type promObserver struct {
    workerpool.NopObserver
    outcomes  *prometheus.CounterVec   // label: outcome
    retries   prometheus.Counter
    duration  prometheus.Histogram
    queueWait prometheus.Histogram
}

func (o promObserver) JobCompleted(e workerpool.Event) { o.outcomes.WithLabelValues("succeeded").Inc(); o.duration.Observe(e.Duration.Seconds()) }
func (o promObserver) JobFailed(e workerpool.Event)    { o.outcomes.WithLabelValues("failed").Inc();    o.duration.Observe(e.Duration.Seconds()) }
func (o promObserver) JobRetried(workerpool.Event)     { o.retries.Inc() }
func (o promObserver) JobStarted(e workerpool.Event)   { o.queueWait.Observe(e.QueueWait.Seconds()) }
```

and a `prometheus.Collector` (or `GaugeFunc`s) reading `pool.Stats()` for queue depth and active workers.

Do not use `JobID` as a metric label: it is unbounded cardinality.

## Sketch: OpenTelemetry

Tracing needs a span around execution, which an event-based observer cannot provide by itself because it never sees the job's body run. Two workable patterns:

1. Wrap the job: `pool.Submit(ctx, tracedJob{inner: job, tracer: tr, parent: trace.SpanContextFromContext(ctx)})` where `Run` starts a span (linking to the submitter's span context, since the job outlives the request context), sets `attempt` from `InfoFromContext`, and records the error. This gives per-attempt spans.
2. Use the observer for metrics only (OTel metrics instruments in the same shape as the Prometheus sketch), and let the job wrapper own tracing.

Because the job context is not derived from the submit context, trace context does not propagate automatically; propagate it explicitly as above. That is the same reason the pool does not propagate cancellation from the submit context.

## Structured logging

`Config.Logger` is a `*slog.Logger`, injected; there is no global logger. Default is a discarding logger.

| Level | Events |
| --- | --- |
| Info | pool started, shutdown initiated, queue no longer saturated, pool stopped (with final stats) |
| Debug | job submitted, rejected, started, succeeded, canceled |
| Warn | retry scheduled (with backoff and error), queue saturated (edge-triggered, once per episode), shutdown deadline reached |
| Error | terminal job failure, job panic (with stack), observer panic, classifier panic |

Debug call sites check `Logger.Enabled` first, so disabled debug logging costs a method call and no allocation. Job payloads are never logged: the pool does not know what is in a `Job`. It logs job IDs, worker IDs, attempt numbers, durations and error strings. Error strings are the job's own; do not put secrets in errors.

Normal per-job events are Debug specifically so that a production pool at Info does not emit a line per job.

## What to alert on

- Sustained non-zero rejection rate (`JobRejected` with `ErrQueueFull`): demand exceeds capacity.
- Queue depth pinned near capacity, or queue wait p99 approaching the SLO.
- Rising retry rate or `Failed` rate: a downstream is unhealthy; check for amplification.
- Any `Panicked`: a defect.
- Active workers equal to `Workers` for long periods: no headroom.
