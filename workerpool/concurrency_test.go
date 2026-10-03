package workerpool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestConcurrentProducers(t *testing.T) {
	const producers, perProducer = 32, 200
	p := newTestPool(t, Config{Workers: 4, QueueSize: 16})
	var executed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				if err := p.Submit(context.Background(), JobFunc(func(context.Context) error {
					executed.Add(1)
					return nil
				})); err != nil {
					t.Errorf("Submit: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := executed.Load(); got != producers*perProducer {
		t.Fatalf("executed %d, want %d", got, producers*perProducer)
	}
}

// Producers blocked on a full queue must all make progress once workers
// consume, and every accepted job must run exactly once.
func TestSaturationWithBlockedProducers(t *testing.T) {
	const producers = 16
	p := newTestPool(t, Config{Workers: 2, QueueSize: 1})
	g := newGate(2)
	for i := 0; i < 2; i++ {
		_ = p.Submit(context.Background(), g.job())
	}
	recv(t, g.started, "worker 1")
	recv(t, g.started, "worker 2")

	var ran atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Submit(context.Background(), JobFunc(func(context.Context) error { ran.Add(1); return nil })); err != nil {
				t.Errorf("Submit: %v", err)
			}
		}()
	}
	g.open()
	wg.Wait()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != producers {
		t.Fatalf("ran %d, want %d", ran.Load(), producers)
	}
}

// The central race: Submit versus Shutdown. Invariant: every job whose Submit
// returned nil runs exactly once, no submission ever panics (send on a closed
// channel), and after shutdown every Submit returns ErrClosed.
func TestSubmitRacingShutdown(t *testing.T) {
	for iter := 0; iter < 25; iter++ {
		p, err := New(Config{Workers: 3, QueueSize: 4})
		if err != nil {
			t.Fatal(err)
		}
		var accepted, executed atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					err := p.Submit(context.Background(), JobFunc(func(context.Context) error {
						executed.Add(1)
						return nil
					}))
					if err == nil {
						accepted.Add(1)
						continue
					}
					if !errors.Is(err, ErrClosed) {
						t.Errorf("unexpected error: %v", err)
					}
					return
				}
			}()
		}
		// Let some work happen before shutting down: wait for progress.
		for executed.Load() < 10 {
			runtime.Gosched()
		}
		if err := p.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if a, e := accepted.Load(), executed.Load(); a != e {
			t.Fatalf("iteration %d: accepted %d jobs but executed %d", iter, a, e)
		}
	}
}

func TestConcurrentShutdownCallers(t *testing.T) {
	p := newTestPool(t, Config{Workers: 4, QueueSize: 8})
	for i := 0; i < 8; i++ {
		_ = p.Submit(context.Background(), JobFunc(func(context.Context) error { return nil }))
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Shutdown(context.Background()); err != nil {
				t.Errorf("Shutdown: %v", err)
			}
		}()
	}
	wg.Wait()
	if p.State() != Stopped {
		t.Fatalf("state %v", p.State())
	}
}

// Jobs finishing while shutdown proceeds, with Close racing Shutdown.
func TestJobsCompletingDuringShutdown(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		p, _ := New(Config{Workers: 4, QueueSize: 64})
		var ran atomic.Int64
		accepted := 0
		for i := 0; i < 64; i++ {
			if p.TrySubmit(JobFunc(func(context.Context) error { ran.Add(1); return nil })) == nil {
				accepted++
			}
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = p.Shutdown(context.Background()) }()
		go func() { defer wg.Done(); _ = p.Stats(); _ = p.State() }()
		wg.Wait()
		<-p.Done()
		s := p.Stats()
		// Every accepted job reached exactly one terminal outcome.
		if s.Succeeded+s.Canceled != uint64(accepted) || uint64(ran.Load()) != s.Succeeded {
			t.Fatalf("accepted=%d ran=%d stats=%+v", accepted, ran.Load(), s)
		}
	}
}

// Abort racing with in-flight submissions: every accepted job still reaches a
// terminal outcome, so nothing is lost silently.
func TestAbortAccountsForEveryAcceptedJob(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		p, _ := New(Config{Workers: 2, QueueSize: 8})
		var wg sync.WaitGroup
		var accepted atomic.Int64
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					err := p.Submit(context.Background(), JobFunc(func(ctx context.Context) error {
						<-ctx.Done()
						return ctx.Err()
					}))
					if err != nil {
						return
					}
					accepted.Add(1)
				}
			}()
		}
		for p.Stats().Submitted < 10 {
			runtime.Gosched()
		}
		_ = p.Close()
		wg.Wait()
		s := p.Stats()
		if got := s.Succeeded + s.Failed + s.Canceled + s.Panicked; got != uint64(accepted.Load()) {
			t.Fatalf("accepted %d but %d terminal outcomes: %+v", accepted.Load(), got, s)
		}
	}
}

func TestLargeNumberOfJobs(t *testing.T) {
	n := 100_000
	if testing.Short() {
		n = 5_000
	}
	p := newTestPool(t, Config{Workers: 8, QueueSize: 128})
	var executed atomic.Int64
	for i := 0; i < n; i++ {
		if err := p.Submit(context.Background(), JobFunc(func(context.Context) error { executed.Add(1); return nil })); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if executed.Load() != int64(n) {
		t.Fatalf("executed %d, want %d", executed.Load(), n)
	}
}

func TestConcurrentTrySubmitNeverExceedsCapacity(t *testing.T) {
	const capacity = 5
	p := newTestPool(t, Config{Workers: 1, QueueSize: capacity})
	g := newGate(1)
	_ = p.TrySubmit(g.job())
	recv(t, g.started, "worker busy")

	var accepted, full atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := p.TrySubmit(JobFunc(func(context.Context) error { return nil })); {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, ErrQueueFull):
				full.Add(1)
			default:
				t.Errorf("unexpected: %v", err)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != capacity || full.Load() != 50-capacity {
		t.Fatalf("accepted=%d full=%d, want %d/%d", accepted.Load(), full.Load(), capacity, 50-capacity)
	}
	g.open()
}
