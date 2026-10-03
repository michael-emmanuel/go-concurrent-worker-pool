package workerpool

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBasicExecution(t *testing.T) {
	p := newTestPool(t, Config{Workers: 2, QueueSize: 4})
	var wg sync.WaitGroup
	var ran atomic.Int32
	for i := 0; i < 4; i++ {
		wg.Add(1)
		if err := p.Submit(context.Background(), JobFunc(func(context.Context) error {
			defer wg.Done()
			ran.Add(1)
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 4 {
		t.Fatalf("ran %d jobs, want 4", ran.Load())
	}
	if s := p.Stats(); s.Submitted != 4 || s.Succeeded != 4 || s.State != Stopped {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestNilJobRejected(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1})
	if err := p.Submit(context.Background(), nil); !errors.Is(err, ErrNilJob) {
		t.Fatalf("got %v, want ErrNilJob", err)
	}
	if err := p.TrySubmit(nil); !errors.Is(err, ErrNilJob) {
		t.Fatalf("got %v, want ErrNilJob", err)
	}
}

// If N jobs each wait for all N to have started, the test can only pass when
// N workers really run concurrently.
func TestWorkersRunConcurrently(t *testing.T) {
	const n = 4
	p := newTestPool(t, Config{Workers: n, QueueSize: n})
	var barrier sync.WaitGroup
	barrier.Add(n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		if err := p.Submit(context.Background(), JobFunc(func(context.Context) error {
			defer wg.Done()
			barrier.Done()
			barrier.Wait()
			return nil
		})); err != nil {
			t.Fatal(err)
		}
	}
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	waitClosed(t, waitDone, "all workers to run concurrently")
}

func TestTrySubmitQueueFull(t *testing.T) {
	obs := &recObserver{}
	p := newTestPool(t, Config{Workers: 1, QueueSize: 2, Observer: obs})
	g := newGate(1)
	if err := p.TrySubmit(g.job()); err != nil {
		t.Fatal(err)
	}
	recv(t, g.started, "first job to start") // the worker is now busy
	for i := 0; i < 2; i++ {
		if err := p.TrySubmit(JobFunc(func(context.Context) error { return nil })); err != nil {
			t.Fatalf("queue slot %d: %v", i, err)
		}
	}
	if s := p.Stats(); s.QueueDepth != 2 || s.QueueCapacity != 2 || s.ActiveWorkers != 1 {
		t.Fatalf("unexpected stats: %+v", s)
	}
	err := p.TrySubmit(JobFunc(func(context.Context) error { return nil }))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("got %v, want ErrQueueFull", err)
	}
	if obs.count("rejected") != 1 || p.Stats().Rejected != 1 {
		t.Fatal("rejection not observed")
	}
	g.open()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s := p.Stats(); s.Succeeded != 3 {
		t.Fatalf("accepted jobs must all run: %+v", s)
	}
}

func TestSubmitBlocksUntilCapacity(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 1})
	g := newGate(1)
	_ = p.TrySubmit(g.job())
	recv(t, g.started, "worker busy")
	if err := p.TrySubmit(JobFunc(func(context.Context) error { return nil })); err != nil {
		t.Fatal(err) // fills the single slot
	}

	ran := make(chan struct{})
	returned := make(chan error, 1)
	go func() {
		returned <- p.Submit(context.Background(), JobFunc(func(context.Context) error { close(ran); return nil }))
	}()
	select {
	case err := <-returned:
		t.Fatalf("Submit returned %v while the queue was full", err)
	default:
	}
	g.open() // capacity frees up as the worker moves on
	if err := recv(t, returned, "blocked Submit to return"); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, ran, "unblocked job to run")
}

func TestSubmitContextCanceledWhileBlocked(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 0})
	g := newGate(1)
	_ = p.Submit(context.Background(), g.job())
	recv(t, g.started, "worker busy")

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() { returned <- p.Submit(ctx, JobFunc(func(context.Context) error { return nil })) }()
	cancel()
	if err := recv(t, returned, "Submit to observe cancellation"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	g.open()
}

func TestSubmitContextDeadline(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 0})
	g := newGate(1)
	_ = p.Submit(context.Background(), g.job())
	recv(t, g.started, "worker busy")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := p.Submit(ctx, JobFunc(func(context.Context) error { return nil }))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
	g.open()
}

func TestSubmitWithCanceledContextDoesNotEnqueue(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 10})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var ran atomic.Bool
	err := p.Submit(ctx, JobFunc(func(context.Context) error { ran.Store(true); return nil }))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	_ = p.Shutdown(context.Background())
	if ran.Load() {
		t.Fatal("job must not run when Submit fails")
	}
}

func TestSubmitContextDoesNotCancelJob(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 1})
	ctx, cancel := context.WithCancel(context.Background())
	seen := make(chan error, 1)
	release := make(chan struct{})
	if err := p.Submit(ctx, JobFunc(func(jc context.Context) error {
		<-release
		seen <- jc.Err()
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	cancel() // the submitter's ctx ends; the job must be unaffected
	close(release)
	if err := recv(t, seen, "job"); err != nil {
		t.Fatalf("job context was canceled by the submit context: %v", err)
	}
}

func TestSubmitUnblockedByShutdown(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 0})
	g := newGate(1)
	_ = p.Submit(context.Background(), g.job())
	recv(t, g.started, "worker busy")

	returned := make(chan error, 1)
	go func() {
		returned <- p.Submit(context.Background(), JobFunc(func(context.Context) error { return nil }))
	}()
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- p.Shutdown(context.Background()) }()
	if err := recv(t, returned, "blocked Submit"); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
	g.open()
	if err := recv(t, shutdownErr, "Shutdown"); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDrainsQueue(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 3})
	g := newGate(1)
	_ = p.TrySubmit(g.job())
	recv(t, g.started, "worker busy")
	var ran atomic.Int32
	for i := 0; i < 3; i++ {
		if err := p.TrySubmit(JobFunc(func(context.Context) error { ran.Add(1); return nil })); err != nil {
			t.Fatal(err)
		}
	}
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- p.Shutdown(context.Background()) }()
	g.open()
	if err := recv(t, shutdownErr, "Shutdown"); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 3 {
		t.Fatalf("queued jobs run = %d, want 3 (drain)", ran.Load())
	}
	if p.State() != Stopped {
		t.Fatalf("state %v, want stopped", p.State())
	}
}

func TestSubmitAfterShutdown(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 1})
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(context.Background(), JobFunc(func(context.Context) error { return nil })); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit: got %v", err)
	}
	if err := p.TrySubmit(JobFunc(func(context.Context) error { return nil })); !errors.Is(err, ErrClosed) {
		t.Fatalf("TrySubmit: got %v", err)
	}
}

func TestShutdownIdempotent(t *testing.T) {
	p := newTestPool(t, Config{Workers: 2, QueueSize: 2})
	for i := 0; i < 3; i++ {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, p.Done(), "Done")
}

func TestEmptyPoolStartsAndStops(t *testing.T) {
	p := newTestPool(t, Config{Workers: 3, QueueSize: 0})
	if p.State() != Running {
		t.Fatalf("state %v, want running", p.State())
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !isClosed(p.Done()) || p.State() != Stopped {
		t.Fatal("pool not stopped")
	}
}

func TestShutdownDeadlineAbortsPool(t *testing.T) {
	obs := &recObserver{}
	p := newTestPool(t, Config{Workers: 1, QueueSize: 2, Observer: obs})
	g := newGate(1)
	h, _ := p.TrySubmitTask(Task{Job: g.job()})
	recv(t, g.started, "worker busy")
	var queuedRan atomic.Int32
	var handles []*Handle
	for i := 0; i < 2; i++ {
		qh, err := p.TrySubmitTask(Task{Job: JobFunc(func(context.Context) error { queuedRan.Add(1); return nil })})
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, qh)
	}

	expired, cancel := context.WithCancel(context.Background())
	cancel() // an already-expired shutdown context: abort immediately
	err := p.Shutdown(expired)
	if !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want ErrShutdownTimeout wrapping context.Canceled", err)
	}
	waitClosed(t, p.Done(), "pool to stop after abort")

	res, _ := h.Wait(context.Background())
	if res.Outcome != Canceled || !errors.Is(res.Err, ErrAborted) {
		t.Fatalf("running job: %+v", res)
	}
	for _, qh := range handles {
		r, _ := qh.Wait(context.Background())
		if r.Outcome != Canceled || r.Attempts != 0 || !errors.Is(r.Err, ErrAborted) {
			t.Fatalf("queued job should be discarded as canceled: %+v", r)
		}
	}
	if queuedRan.Load() != 0 {
		t.Fatal("queued jobs must not run after abort")
	}
	if obs.count("canceled") != 3 {
		t.Fatalf("canceled events = %d, want 3", obs.count("canceled"))
	}
}

func TestCloseAbortsRunningJobs(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 1})
	g := newGate(1)
	h, _ := p.TrySubmitTask(Task{Job: g.job()})
	recv(t, g.started, "job running")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	r, ok := h.Result()
	if !ok || r.Outcome != Canceled {
		t.Fatalf("got %+v ok=%v", r, ok)
	}
}

func TestLaterShutdownCallerDoesNotAbort(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 0})
	g := newGate(1)
	// Blocking submit: with QueueSize 0 a non-blocking submit succeeds only if
	// the worker has already parked on the queue, which is a race at startup.
	h, err := p.SubmitTask(context.Background(), Task{Job: g.job()})
	if err != nil {
		t.Fatal(err)
	}
	recv(t, g.started, "job running")

	first := make(chan error, 1)
	go func() { first <- p.Shutdown(context.Background()) }()
	for p.State() == Running { // wait for the first call to own shutdown
		runtime.Gosched()
	}
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Shutdown(expired); !errors.Is(err, context.Canceled) || errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("second caller: got %v, want plain ctx error", err)
	}
	if isClosed(h.Done()) {
		t.Fatal("a later caller's expired context must not abort the pool")
	}
	g.open()
	if err := recv(t, first, "first Shutdown"); err != nil {
		t.Fatal(err)
	}
	if r, _ := h.Result(); r.Outcome != Succeeded {
		t.Fatalf("job should have completed normally: %+v", r)
	}
}

func TestRetryTransientThenSuccess(t *testing.T) {
	fs := &fakeSleeper{}
	obs := &recObserver{}
	p := newTestPoolHooks(t, Config{
		Workers: 1, Observer: obs,
		RetryPolicy: RetryPolicy{MaxAttempts: 5, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, Multiplier: 2, Jitter: 0.5},
	}, hooks{sleep: fs.sleep, rand: func() float64 { return 0 }})

	var calls int
	h, err := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	res, _ := h.Wait(context.Background())
	if res.Outcome != Succeeded || res.Attempts != 3 {
		t.Fatalf("got %+v", res)
	}
	if got := fs.durations(); len(got) != 2 || got[0] != 100*time.Millisecond || got[1] != 200*time.Millisecond {
		t.Fatalf("backoffs %v, want [100ms 200ms]", got)
	}
	if obs.count("retried") != 2 || obs.count("completed") != 1 || p.Stats().Retries != 2 {
		t.Fatalf("events: %v", obs.snapshot())
	}
}

func TestRetryExhaustion(t *testing.T) {
	fs := &fakeSleeper{}
	p := newTestPoolHooks(t, Config{Workers: 1, RetryPolicy: RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}},
		hooks{sleep: fs.sleep})
	boom := errors.New("still broken")
	var calls atomic.Int32
	h, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { calls.Add(1); return boom })})
	res, _ := h.Wait(context.Background())
	if res.Outcome != Failed || res.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("got %+v calls=%d", res, calls.Load())
	}
	if !errors.Is(res.Err, ErrRetriesExhausted) || !errors.Is(res.Err, boom) {
		t.Fatalf("error should wrap ErrRetriesExhausted and the last error: %v", res.Err)
	}
	if len(fs.durations()) != 2 {
		t.Fatalf("3 attempts mean 2 backoffs, got %v", fs.durations())
	}
}

func TestNonRetryableErrorNotRetried(t *testing.T) {
	fs := &fakeSleeper{}
	p := newTestPoolHooks(t, Config{Workers: 1, RetryPolicy: DefaultRetryPolicy()}, hooks{sleep: fs.sleep})
	var calls atomic.Int32
	bad := errors.New("invalid input")
	h, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { calls.Add(1); return Permanent(bad) })})
	res, _ := h.Wait(context.Background())
	if res.Outcome != Failed || calls.Load() != 1 || res.Attempts != 1 {
		t.Fatalf("got %+v calls=%d", res, calls.Load())
	}
	if errors.Is(res.Err, ErrRetriesExhausted) {
		t.Fatal("a permanent error is not an exhausted retry")
	}
	if !errors.Is(res.Err, bad) || len(fs.durations()) != 0 {
		t.Fatalf("err=%v sleeps=%v", res.Err, fs.durations())
	}
}

func TestNoRetryByDefault(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1})
	var calls atomic.Int32
	h, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { calls.Add(1); return errors.New("x") })})
	res, _ := h.Wait(context.Background())
	if calls.Load() != 1 || res.Outcome != Failed || errors.Is(res.Err, ErrRetriesExhausted) {
		t.Fatalf("got %+v calls=%d", res, calls.Load())
	}
}

func TestCancelDuringBackoff(t *testing.T) {
	sleeping := make(chan struct{})
	p := newTestPoolHooks(t, Config{Workers: 1, RetryPolicy: RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Hour}},
		hooks{sleep: func(ctx context.Context, d time.Duration) error {
			close(sleeping)
			<-ctx.Done() // a backoff that only ends by cancellation
			return ctx.Err()
		}})
	var calls atomic.Int32
	h, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { calls.Add(1); return errors.New("x") })})
	waitClosed(t, sleeping, "worker to enter backoff")
	h.Cancel()
	res, _ := h.Wait(context.Background())
	if res.Outcome != Canceled || !errors.Is(res.Err, ErrJobCanceled) || calls.Load() != 1 {
		t.Fatalf("got %+v calls=%d", res, calls.Load())
	}
}

func TestPanicIsRecovered(t *testing.T) {
	obs := &recObserver{}
	p := newTestPool(t, Config{Workers: 1, QueueSize: 1, Observer: obs, RetryPolicy: DefaultRetryPolicy()})
	var attempts atomic.Int32
	h, _ := p.SubmitTask(context.Background(), Task{ID: "boom", Job: JobFunc(func(context.Context) error {
		attempts.Add(1)
		panic("kaboom")
	})})
	res, _ := h.Wait(context.Background())
	if res.Outcome != Panicked || !errors.Is(res.Err, ErrPanic) {
		t.Fatalf("got %+v", res)
	}
	var pe *PanicError
	if !errors.As(res.Err, &pe) || pe.Value != "kaboom" || pe.JobID != "boom" || pe.WorkerID != 1 || pe.Attempt != 1 {
		t.Fatalf("bad PanicError: %+v", pe)
	}
	if !strings.Contains(string(pe.Stack), "TestPanicIsRecovered") {
		t.Fatalf("stack should identify the panic site:\n%s", pe.Stack)
	}
	if attempts.Load() != 1 {
		t.Fatal("panics must not be retried")
	}
	// The single worker must have survived.
	h2, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { return nil })})
	if r, _ := h2.Wait(context.Background()); r.Outcome != Succeeded {
		t.Fatalf("worker did not survive a panic: %+v", r)
	}
	if obs.count("panicked") != 1 || p.Stats().Panicked != 1 {
		t.Fatal("panic not observed")
	}
}

func TestPanicWithErrorValueUnwraps(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1})
	sentinel := errors.New("sentinel")
	h, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { panic(sentinel) })})
	res, _ := h.Wait(context.Background())
	if !errors.Is(res.Err, sentinel) || !errors.Is(res.Err, ErrPanic) {
		t.Fatalf("got %v", res.Err)
	}
}

func TestJobTimeoutPerAttempt(t *testing.T) {
	fs := &fakeSleeper{}
	p := newTestPoolHooks(t, Config{Workers: 1, JobTimeout: time.Millisecond,
		RetryPolicy: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}}, hooks{sleep: fs.sleep})
	var calls atomic.Int32
	h, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(ctx context.Context) error {
		calls.Add(1)
		<-ctx.Done()
		return ctx.Err()
	})})
	res, _ := h.Wait(context.Background())
	if res.Outcome != Failed || calls.Load() != 2 || !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("timeouts should fail the attempt and be retried: %+v calls=%d", res, calls.Load())
	}
}

func TestTaskTimeoutOverridesConfig(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, JobTimeout: time.Hour})
	h, _ := p.SubmitTask(context.Background(), Task{Timeout: time.Millisecond, Job: JobFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})})
	if res, _ := h.Wait(context.Background()); !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("got %+v", res)
	}
}

func TestCancelRunningJob(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1})
	g := newGate(1)
	h, _ := p.SubmitTask(context.Background(), Task{Job: g.job()})
	recv(t, g.started, "job running")
	h.Cancel()
	h.Cancel() // idempotent
	res, err := h.Wait(context.Background())
	if err != nil || res.Outcome != Canceled || !errors.Is(res.Err, ErrJobCanceled) || res.Attempts != 1 {
		t.Fatalf("got %+v err=%v", res, err)
	}
}

func TestCancelQueuedJobNeverRuns(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 1})
	g := newGate(1)
	_ = p.TrySubmit(g.job())
	recv(t, g.started, "worker busy")
	var ran atomic.Bool
	h, err := p.TrySubmitTask(Task{Job: JobFunc(func(context.Context) error { ran.Store(true); return nil })})
	if err != nil {
		t.Fatal(err)
	}
	h.Cancel()
	g.open()
	res, _ := h.Wait(context.Background())
	if res.Outcome != Canceled || res.Attempts != 0 || ran.Load() {
		t.Fatalf("got %+v ran=%v", res, ran.Load())
	}
}

func TestHandleWaitContext(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1})
	g := newGate(1)
	h, _ := p.SubmitTask(context.Background(), Task{Job: g.job()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if _, ok := h.Result(); ok {
		t.Fatal("job is still running")
	}
	g.open()
	if r, err := h.Wait(context.Background()); err != nil || r.Outcome != Succeeded {
		t.Fatalf("got %+v %v", r, err)
	}
}

func TestInfoFromContext(t *testing.T) {
	p := newTestPoolHooks(t, Config{Workers: 1, RetryPolicy: RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}},
		hooks{sleep: (&fakeSleeper{}).sleep})
	var attempts []int
	h, _ := p.SubmitTask(context.Background(), Task{ID: "abc", Job: JobFunc(func(ctx context.Context) error {
		info, ok := InfoFromContext(ctx)
		if !ok || info.JobID != "abc" || info.WorkerID != 1 {
			t.Errorf("bad info %+v ok=%v", info, ok)
		}
		attempts = append(attempts, info.Attempt)
		if len(attempts) < 3 {
			return errors.New("again")
		}
		return nil
	})})
	h.Wait(context.Background())
	if len(attempts) != 3 || attempts[0] != 1 || attempts[2] != 3 {
		t.Fatalf("attempts %v", attempts)
	}
	if _, ok := InfoFromContext(context.Background()); ok {
		t.Fatal("unexpected info on a foreign context")
	}
}

func TestObserverLifecycleOrder(t *testing.T) {
	obs := &recObserver{}
	p := newTestPool(t, Config{Workers: 1, Observer: obs})
	h, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { return nil })})
	h.Wait(context.Background())
	got := obs.snapshot()
	want := []string{"submitted", "started", "completed"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestPanickingObserverDoesNotKillWorker(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, Observer: panicObserver{}})
	for i := 0; i < 3; i++ {
		h, err := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { return nil })})
		if err != nil {
			t.Fatal(err)
		}
		if r, _ := h.Wait(context.Background()); r.Outcome != Succeeded {
			t.Fatalf("got %+v", r)
		}
	}
}

type panicObserver struct{ NopObserver }

func (panicObserver) JobStarted(Event)   { panic("observer bug") }
func (panicObserver) JobCompleted(Event) { panic("observer bug") }

func TestStatsCounters(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 1})
	h1, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { return errors.New("x") })})
	h1.Wait(context.Background())
	h2, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { return nil })})
	h2.Wait(context.Background())
	s := p.Stats()
	if s.Submitted != 2 || s.Failed != 1 || s.Succeeded != 1 || s.Workers != 1 || s.QueueCapacity != 1 || s.State != Running {
		t.Fatalf("stats %+v", s)
	}
}

func TestExplicitTaskIDAndGeneratedIDs(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 2})
	h1, _ := p.SubmitTask(context.Background(), Task{ID: "mine", Job: JobFunc(func(context.Context) error { return nil })})
	h2, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { return nil })})
	h3, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { return nil })})
	if h1.ID() != "mine" || h2.ID() == h3.ID() || !strings.HasPrefix(h2.ID(), "job-") {
		t.Fatalf("ids: %q %q %q", h1.ID(), h2.ID(), h3.ID())
	}
}

func TestStateStrings(t *testing.T) {
	if Running.String() != "running" || Draining.String() != "draining" || Stopped.String() != "stopped" || State(0).String() != "unknown" {
		t.Fatal("bad State strings")
	}
	if Succeeded.String() != "succeeded" || Failed.String() != "failed" || Canceled.String() != "canceled" || Panicked.String() != "panicked" || Outcome(0).String() != "unknown" {
		t.Fatal("bad Outcome strings")
	}
}

// TestNoGoroutineLeak checks that a pool that has served work, blocked
// submitters, retries and cancellation leaves no goroutines behind. Not run in
// parallel with other tests, so the goroutine count is meaningful.
func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		p, err := New(Config{Workers: 4, QueueSize: 2, RetryPolicy: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Microsecond}})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for j := 0; j < 8; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = p.Submit(context.Background(), JobFunc(func(context.Context) error { return errors.New("x") }))
			}()
		}
		wg.Wait()
		if err := p.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(watchdog)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if after := runtime.NumGoroutine(); after > before {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines: before=%d after=%d\n%s", before, after, buf[:runtime.Stack(buf, true)])
	}
}

func TestPanickingClassifierIsContained(t *testing.T) {
	p := newTestPoolHooks(t, Config{Workers: 1, RetryPolicy: RetryPolicy{
		MaxAttempts: 3, InitialBackoff: time.Millisecond,
		Retryable: func(error) bool { panic("classifier bug") },
	}}, hooks{sleep: (&fakeSleeper{}).sleep})
	var calls atomic.Int32
	h, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { calls.Add(1); return errors.New("x") })})
	res, _ := h.Wait(context.Background())
	if res.Outcome != Failed || calls.Load() != 1 {
		t.Fatalf("got %+v calls=%d", res, calls.Load())
	}
	h2, _ := p.SubmitTask(context.Background(), Task{Job: JobFunc(func(context.Context) error { return nil })})
	if r, _ := h2.Wait(context.Background()); r.Outcome != Succeeded {
		t.Fatal("worker must survive a panicking classifier")
	}
}
