package workerpool

import (
	"context"
	"sync"
	"testing"
	"time"
)

// watchdog bounds how long a test waits for an event. It is a failure
// detector only: tests never rely on it elapsing for correctness.
const watchdog = 10 * time.Second

func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(watchdog):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(watchdog):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func newTestPool(t *testing.T, cfg Config) *Pool {
	t.Helper()
	return newTestPoolHooks(t, cfg, hooks{})
}

func newTestPoolHooks(t *testing.T, cfg Config, h hooks) *Pool {
	t.Helper()
	p, err := newPool(cfg, h)
	if err != nil {
		t.Fatalf("newPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// gate is a job body that reports when it starts and blocks until released.
type gate struct {
	started chan struct{}
	release chan struct{}
}

func newGate(n int) *gate {
	return &gate{started: make(chan struct{}, n), release: make(chan struct{})}
}

func (g *gate) job() Job {
	return JobFunc(func(ctx context.Context) error {
		g.started <- struct{}{}
		select {
		case <-g.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func (g *gate) open() { close(g.release) }

// fakeSleeper records requested backoffs and returns immediately.
type fakeSleeper struct {
	mu sync.Mutex
	ds []time.Duration
}

func (f *fakeSleeper) sleep(ctx context.Context, d time.Duration) error {
	f.mu.Lock()
	f.ds = append(f.ds, d)
	f.mu.Unlock()
	return ctx.Err()
}

func (f *fakeSleeper) durations() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.ds...)
}

// recObserver records event names and events.
type recObserver struct {
	mu     sync.Mutex
	names  []string
	events []Event
}

func (o *recObserver) rec(name string, e Event) {
	o.mu.Lock()
	o.names = append(o.names, name)
	o.events = append(o.events, e)
	o.mu.Unlock()
}
func (o *recObserver) JobSubmitted(e Event) { o.rec("submitted", e) }
func (o *recObserver) JobRejected(e Event)  { o.rec("rejected", e) }
func (o *recObserver) JobStarted(e Event)   { o.rec("started", e) }
func (o *recObserver) JobRetried(e Event)   { o.rec("retried", e) }
func (o *recObserver) JobCompleted(e Event) { o.rec("completed", e) }
func (o *recObserver) JobFailed(e Event)    { o.rec("failed", e) }
func (o *recObserver) JobCanceled(e Event)  { o.rec("canceled", e) }
func (o *recObserver) JobPanicked(e Event)  { o.rec("panicked", e) }

func (o *recObserver) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.names...)
}

func (o *recObserver) count(name string) int {
	n := 0
	for _, s := range o.snapshot() {
		if s == name {
			n++
		}
	}
	return n
}
