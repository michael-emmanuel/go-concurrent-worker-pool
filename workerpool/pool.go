package workerpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// State is the lifecycle state of a Pool. Transitions only move forward.
type State int32

const (
	// Running: accepting submissions and executing jobs.
	Running State = iota + 1
	// Draining: no longer accepting submissions; queued and running jobs are
	// completing (or, after an abort, being canceled).
	Draining
	// Stopped: all workers have exited. Terminal.
	Stopped
)

// String returns a lower-case name for the state.
func (s State) String() string {
	switch s {
	case Running:
		return "running"
	case Draining:
		return "draining"
	case Stopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Stats is a point-in-time snapshot of pool activity. The counters are read
// individually, so the snapshot is not a single atomic view; counters are
// monotonic, gauges (QueueDepth, ActiveWorkers) are instantaneous.
type Stats struct {
	State         State
	Workers       int
	QueueCapacity int
	QueueDepth    int
	ActiveWorkers int

	// Submitted counts jobs accepted onto the queue.
	Submitted uint64
	// Rejected counts submissions that were refused for any reason: queue
	// full, pool closed, or the caller's context ended.
	Rejected uint64
	// Succeeded, Failed, Canceled and Panicked count terminal outcomes.
	Succeeded uint64
	Failed    uint64
	Canceled  uint64
	Panicked  uint64
	// Retries counts retry attempts scheduled (not attempts made).
	Retries uint64
}

// hooks are test seams for time and randomness. Production code uses the
// defaults set by newPool.
type hooks struct {
	sleep func(ctx context.Context, d time.Duration) error
	rand  func() float64
}

// item is a queued unit of work.
type item struct {
	id       string
	job      Job
	timeout  time.Duration
	enqueued time.Time
	ctx      context.Context         // job-scoped: derived from the pool's base context
	cancel   context.CancelCauseFunc // non-nil only for tracked tasks
	handle   *Handle                 // non-nil only for tracked tasks

	// gate is held by the submitter from before the item becomes visible to
	// workers until JobSubmitted has been reported. A worker takes and drops
	// it before doing anything else, which guarantees that JobSubmitted
	// happens-before JobStarted for the same job.
	gate sync.Mutex
}

// Pool is a bounded, fixed-size worker pool. Create one with New. All methods
// are safe for concurrent use.
type Pool struct {
	cfg Config
	obs Observer
	log *slog.Logger
	h   hooks

	queue chan *item

	// baseCtx is the parent of every job context. Cancelling it (abort) is
	// how running jobs, backoff sleeps and queued jobs are told to stop.
	baseCtx context.Context
	abort   context.CancelCauseFunc

	// mu guards state. Submitters take it for reading to check the state and
	// register with senders atomically; shutdown takes it for writing, which
	// is what makes "no new sender after Draining" precise.
	mu       sync.RWMutex
	state    State
	stopping chan struct{} // closed when the pool leaves Running
	done     chan struct{} // closed when the pool reaches Stopped

	senders sync.WaitGroup // submissions currently between registration and completion
	workers sync.WaitGroup

	seq       atomic.Uint64
	active    atomic.Int64
	saturated atomic.Bool

	submitted, rejected, succeeded, failed, canceled, panicked, retries atomic.Uint64
}

// New validates cfg, starts cfg.Workers worker goroutines and returns the pool.
// The caller must eventually call Shutdown or Close, otherwise the workers run
// for the life of the process.
func New(cfg Config) (*Pool, error) { return newPool(cfg, hooks{}) }

func newPool(cfg Config, h hooks) (*Pool, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.RetryPolicy = cfg.RetryPolicy.normalized()
	if cfg.Logger == nil {
		cfg.Logger = slog.New(discardHandler{})
	}
	if cfg.Observer == nil {
		cfg.Observer = NopObserver{}
	}
	if h.sleep == nil {
		h.sleep = sleepContext
	}
	if h.rand == nil {
		h.rand = rand.Float64
	}

	baseCtx, abort := context.WithCancelCause(context.Background())
	p := &Pool{
		cfg:      cfg,
		obs:      safeObserver{next: cfg.Observer, log: cfg.Logger},
		log:      cfg.Logger,
		h:        h,
		queue:    make(chan *item, cfg.QueueSize),
		baseCtx:  baseCtx,
		abort:    abort,
		state:    Running,
		stopping: make(chan struct{}),
		done:     make(chan struct{}),
	}
	p.workers.Add(cfg.Workers)
	for i := 1; i <= cfg.Workers; i++ {
		go p.worker(i)
	}
	p.log.Info("worker pool started",
		"workers", cfg.Workers, "queue_size", cfg.QueueSize,
		"max_attempts", cfg.RetryPolicy.MaxAttempts, "job_timeout", cfg.JobTimeout)
	return p, nil
}

// Submit places job on the queue, blocking while the queue is full. It returns
// nil once the job is accepted; from then on the pool guarantees the job
// reaches a terminal outcome.
//
// Submit returns early, without enqueuing, with:
//   - ctx's error if ctx ends while waiting (or was already done),
//   - ErrClosed if the pool is shut down, or shutdown begins while waiting,
//   - ErrNilJob if job is nil.
//
// ctx governs only the wait for queue space. It does not become the job's
// context: a request-scoped ctx can be used here without the job being
// canceled when the request ends. Use SubmitTask and Handle.Cancel to cancel
// an individual job.
func (p *Pool) Submit(ctx context.Context, job Job) error {
	_, err := p.submit(ctx, Task{Job: job}, true, false)
	return err
}

// TrySubmit places job on the queue without blocking. It returns ErrQueueFull
// if there is no free capacity, ErrClosed if the pool is shut down, or
// ErrNilJob. Use it when the caller can shed load (for example by replying
// HTTP 503) and must not wait.
func (p *Pool) TrySubmit(job Job) error {
	_, err := p.submit(context.Background(), Task{Job: job}, false, false)
	return err
}

// SubmitTask is Submit for a Task, returning a Handle for the accepted job.
// The Handle is nil when an error is returned.
func (p *Pool) SubmitTask(ctx context.Context, t Task) (*Handle, error) {
	return p.submit(ctx, t, true, true)
}

// TrySubmitTask is TrySubmit for a Task, returning a Handle for the accepted job.
func (p *Pool) TrySubmitTask(t Task) (*Handle, error) {
	return p.submit(context.Background(), t, false, true)
}

func (p *Pool) submit(ctx context.Context, t Task, block, track bool) (*Handle, error) {
	if t.Job == nil {
		return nil, ErrNilJob
	}
	it := &item{job: t.Job, id: t.ID, timeout: t.Timeout, ctx: p.baseCtx}
	if it.id == "" {
		it.id = "job-" + strconv.FormatUint(p.seq.Add(1), 10)
	}
	var h *Handle
	if track {
		it.ctx, it.cancel = context.WithCancelCause(p.baseCtx)
		h = &Handle{id: it.id, cancel: it.cancel, done: make(chan struct{})}
		it.handle = h
	}

	it.gate.Lock()
	defer it.gate.Unlock()
	if err := p.enqueue(ctx, it, block); err != nil {
		if it.cancel != nil {
			it.cancel(err)
		}
		p.rejected.Add(1)
		if p.debugOn() {
			p.log.Debug("job rejected", "job_id", it.id, "reason", err)
		}
		p.obs.JobRejected(Event{JobID: it.id, QueueDepth: len(p.queue), ActiveWorkers: int(p.active.Load()), Err: err})
		return nil, err
	}
	p.submitted.Add(1)
	if p.debugOn() {
		p.log.Debug("job submitted", "job_id", it.id)
	}
	p.obs.JobSubmitted(Event{JobID: it.id, QueueDepth: len(p.queue), ActiveWorkers: int(p.active.Load())})
	return h, nil
}

// enqueue is the only place that sends on p.queue. Its correctness rests on
// one invariant: the queue is closed only after every goroutine that
// registered in p.senders has returned, and no goroutine can register once the
// state has left Running.
func (p *Pool) enqueue(ctx context.Context, it *item, block bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.RLock()
	if p.state != Running {
		p.mu.RUnlock()
		return ErrClosed
	}
	p.senders.Add(1)
	p.mu.RUnlock()
	defer p.senders.Done()

	it.enqueued = time.Now()
	select {
	case p.queue <- it:
		return nil
	default:
	}

	// The queue is full (or, with QueueSize 0, no worker is ready).
	if p.saturated.CompareAndSwap(false, true) {
		p.log.Warn("queue saturated", "capacity", cap(p.queue), "workers", p.cfg.Workers)
	}
	if !block {
		return ErrQueueFull
	}
	select {
	case p.queue <- it:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stopping:
		return ErrClosed
	}
}

// worker is the loop run by each worker goroutine. It exits when the queue is
// closed and drained, which happens only during shutdown.
func (p *Pool) worker(id int) {
	defer p.workers.Done()
	for it := range p.queue {
		p.noteRecovered()
		// Barrier: wait for the submitter to finish reporting JobSubmitted.
		it.gate.Lock()
		it.gate.Unlock() //nolint:staticcheck // SA2001: the empty critical section is the point
		p.process(id, it)
	}
}

// debugOn reports whether Debug logging is enabled. Debug call sites check it
// first so that, when disabled, they do not pay to box their arguments.
func (p *Pool) debugOn() bool { return p.log.Enabled(context.Background(), slog.LevelDebug) }

func (p *Pool) noteRecovered() {
	if p.saturated.Load() && len(p.queue) <= cap(p.queue)/2 && p.saturated.CompareAndSwap(true, false) {
		p.log.Info("queue no longer saturated", "depth", len(p.queue), "capacity", cap(p.queue))
	}
}

func (p *Pool) process(workerID int, it *item) {
	waited := time.Since(it.enqueued)

	// A job canceled while queued, or queued when the pool was aborted, is
	// discarded without running.
	if it.ctx.Err() != nil {
		p.finish(0, it, Result{JobID: it.id, Outcome: Canceled, Err: cancelError(it.ctx, nil), QueueWait: waited})
		return
	}

	p.active.Add(1)
	defer p.active.Add(-1)
	if p.debugOn() {
		p.log.Debug("job started", "job_id", it.id, "worker", workerID, "queue_wait", waited)
	}
	p.obs.JobStarted(Event{JobID: it.id, WorkerID: workerID, Attempt: 1, QueueWait: waited,
		QueueDepth: len(p.queue), ActiveWorkers: int(p.active.Load())})

	p.finish(workerID, it, p.run(workerID, it, waited))
}

// run executes the job, applying the retry policy, and returns its terminal
// Result. Retries happen here, on the worker that owns the job.
func (p *Pool) run(workerID int, it *item, waited time.Duration) Result {
	pol := p.cfg.RetryPolicy
	start := time.Now()
	res := Result{JobID: it.id, QueueWait: waited}
	end := func(o Outcome, err error) Result {
		res.Outcome, res.Err, res.Duration = o, err, time.Since(start)
		return res
	}

	for attempt := 1; ; attempt++ {
		res.Attempts = attempt
		panicked, err := p.attempt(workerID, it, attempt)
		switch {
		case err == nil:
			return end(Succeeded, nil)
		case panicked:
			return end(Panicked, err) // a panic is a defect, not a transient fault: never retried
		case it.ctx.Err() != nil:
			return end(Canceled, cancelError(it.ctx, err))
		case !p.classify(pol, it.id, err):
			return end(Failed, err)
		case attempt >= pol.attempts():
			if pol.attempts() > 1 {
				err = fmt.Errorf("%w after %d attempts: %w", ErrRetriesExhausted, attempt, err)
			}
			return end(Failed, err)
		}

		delay := pol.backoff(attempt, p.h.rand())
		p.retries.Add(1)
		p.log.Warn("job attempt failed, will retry",
			"job_id", it.id, "worker", workerID, "attempt", attempt, "backoff", delay, "error", err)
		p.obs.JobRetried(Event{JobID: it.id, WorkerID: workerID, Attempt: attempt, Retries: attempt,
			Backoff: delay, Err: err, QueueDepth: len(p.queue), ActiveWorkers: int(p.active.Load())})
		if p.h.sleep(it.ctx, delay) != nil {
			return end(Canceled, cancelError(it.ctx, nil))
		}
	}
}

// attempt runs the job once. This is the panic boundary: a panic in Run is
// converted into a *PanicError here, so it can never unwind the worker.
func (p *Pool) attempt(workerID int, it *item, attempt int) (panicked bool, err error) {
	ctx := context.WithValue(it.ctx, infoKey{}, Info{JobID: it.id, WorkerID: workerID, Attempt: attempt})
	timeout := it.timeout
	if timeout <= 0 {
		timeout = p.cfg.JobTimeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			err = &PanicError{Value: r, Stack: debug.Stack(), JobID: it.id, WorkerID: workerID, Attempt: attempt}
		}
	}()
	return false, it.job.Run(ctx)
}

// classify calls the retry classifier, which is user code running on a worker
// goroutine outside the job's panic boundary. A panicking classifier is treated
// as "not retryable" and logged, so it cannot take the process down.
func (p *Pool) classify(pol RetryPolicy, jobID string, err error) (retry bool) {
	defer func() {
		if r := recover(); r != nil {
			retry = false
			p.log.Error("retry classifier panicked; treating error as not retryable",
				"job_id", jobID, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	return pol.Classify(err)
}

// cancelError builds the error for a canceled job from the context's
// cancellation cause and, if any, the error the job itself returned.
func cancelError(ctx context.Context, jobErr error) error {
	cause := context.Cause(ctx)
	if cause == nil {
		cause = context.Canceled
	}
	if jobErr == nil {
		return cause
	}
	return fmt.Errorf("%w (job returned: %w)", cause, jobErr)
}

// finish records the terminal outcome: counters, logs, observer, then the
// Handle. The Handle is resolved last so that a caller woken by Wait can rely
// on the observer having been notified.
func (p *Pool) finish(workerID int, it *item, res Result) {
	ev := Event{
		JobID: it.id, WorkerID: workerID, Attempt: res.Attempts, Retries: max(res.Attempts-1, 0),
		QueueDepth: len(p.queue), ActiveWorkers: int(p.active.Load()),
		QueueWait: res.QueueWait, Duration: res.Duration, Err: res.Err,
	}
	switch res.Outcome {
	case Succeeded:
		p.succeeded.Add(1)
		if p.debugOn() {
			p.log.Debug("job succeeded", "job_id", it.id, "attempts", res.Attempts, "duration", res.Duration)
		}
		p.obs.JobCompleted(ev)
	case Failed:
		p.failed.Add(1)
		p.log.Error("job failed", "job_id", it.id, "attempts", res.Attempts, "duration", res.Duration, "error", res.Err)
		p.obs.JobFailed(ev)
	case Canceled:
		p.canceled.Add(1)
		if p.debugOn() {
			p.log.Debug("job canceled", "job_id", it.id, "attempts", res.Attempts, "cause", res.Err)
		}
		p.obs.JobCanceled(ev)
	case Panicked:
		p.panicked.Add(1)
		var pe *PanicError
		if errors.As(res.Err, &pe) {
			p.log.Error("job panicked", "job_id", it.id, "worker", workerID, "panic", fmt.Sprint(pe.Value), "stack", string(pe.Stack))
		}
		p.obs.JobPanicked(ev)
	}
	if it.handle != nil {
		it.handle.resolve(res)
	}
	if it.cancel != nil {
		it.cancel(nil) // release the job context's resources
	}
}

// Shutdown gracefully stops the pool. It stops accepting submissions
// (blocked submitters return ErrClosed), lets every queued and running job
// finish, waits for the workers to exit and returns nil.
//
// If ctx ends before that completes, the pool is aborted: the contexts of
// running jobs and retry backoffs are canceled with cause ErrAborted, jobs
// still queued are discarded and reported as canceled, and Shutdown returns an
// error wrapping both ErrShutdownTimeout and ctx's error without waiting
// further. The workers exit as soon as their current jobs return; use Done or
// Close to wait for that. A job that ignores its context can delay this
// indefinitely, since goroutines cannot be killed.
//
// Shutdown is idempotent and safe to call concurrently. The first call owns
// the deadline and the abort decision; later calls simply wait for the pool to
// stop and return nil, or their own ctx's error if it ends first. Passing an
// already-canceled context requests an immediate abort.
func (p *Pool) Shutdown(ctx context.Context) error {
	first := p.beginDrain("shutdown")
	select {
	case <-p.done:
		return nil
	default:
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		if !first {
			return ctx.Err()
		}
		p.log.Warn("shutdown deadline reached, aborting pool", "queue_depth", len(p.queue), "active", p.active.Load())
		p.abort(ErrAborted)
		return fmt.Errorf("%w: %w", ErrShutdownTimeout, ctx.Err())
	}
}

// Close stops the pool immediately: like an expired Shutdown, it cancels
// running jobs and discards queued ones, but then waits for the workers to
// exit. It always returns nil and exists so Pool satisfies io.Closer. Prefer
// Shutdown with a deadline for planned termination.
func (p *Pool) Close() error {
	p.beginDrain("close")
	p.abort(ErrAborted)
	<-p.done
	return nil
}

// Done returns a channel that is closed once the pool is Stopped, meaning all
// workers have exited and no job is running.
func (p *Pool) Done() <-chan struct{} { return p.done }

// State returns the current lifecycle state.
func (p *Pool) State() State {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// beginDrain performs the Running -> Draining transition exactly once and
// reports whether this call performed it.
func (p *Pool) beginDrain(reason string) bool {
	p.mu.Lock()
	if p.state != Running {
		p.mu.Unlock()
		return false
	}
	p.state = Draining
	close(p.stopping) // releases submitters blocked on a full queue
	p.mu.Unlock()

	p.log.Info("shutdown initiated", "reason", reason, "queue_depth", len(p.queue), "active", p.active.Load())
	go p.finalize()
	return true
}

// finalize completes the Draining -> Stopped transition. The order matters:
// the queue may be closed only when no submission can still send on it.
func (p *Pool) finalize() {
	p.senders.Wait() // every registered sender has enqueued or given up
	close(p.queue)   // workers drain what is left, then their range loops end
	p.workers.Wait()

	p.mu.Lock()
	p.state = Stopped
	p.mu.Unlock()
	p.abort(context.Canceled) // release baseCtx; a no-op if already aborted
	close(p.done)
	p.log.Info("worker pool stopped", "stats", fmt.Sprintf("%+v", p.Stats()))
}

// Stats returns a snapshot of the pool's counters and gauges.
func (p *Pool) Stats() Stats {
	return Stats{
		State:         p.State(),
		Workers:       p.cfg.Workers,
		QueueCapacity: cap(p.queue),
		QueueDepth:    len(p.queue),
		ActiveWorkers: int(p.active.Load()),
		Submitted:     p.submitted.Load(),
		Rejected:      p.rejected.Load(),
		Succeeded:     p.succeeded.Load(),
		Failed:        p.failed.Load(),
		Canceled:      p.canceled.Load(),
		Panicked:      p.panicked.Load(),
		Retries:       p.retries.Load(),
	}
}
