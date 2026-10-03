package workerpool

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSleepContext(t *testing.T) {
	if err := sleepContext(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepContext(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled context must end the sleep at once, got %v", err)
	}
	if err := sleepContext(context.Background(), 0); err != nil {
		t.Fatalf("zero duration: %v", err)
	}
}

func TestNopObserverAndDiscardHandler(t *testing.T) {
	var o Observer = NopObserver{}
	e := Event{}
	o.JobSubmitted(e)
	o.JobRejected(e)
	o.JobStarted(e)
	o.JobRetried(e)
	o.JobCompleted(e)
	o.JobFailed(e)
	o.JobCanceled(e)
	o.JobPanicked(e)
	h := discardHandler{}
	if h.Enabled(context.Background(), slog.LevelError) || h.WithAttrs(nil) != h || h.WithGroup("g") != h ||
		h.Handle(context.Background(), slog.Record{}) != nil {
		t.Fatal("discard handler misbehaves")
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func TestLoggingLevelsAndSaturationIsEdgeTriggered(t *testing.T) {
	buf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := newTestPoolHooks(t, Config{Workers: 1, QueueSize: 1, Logger: logger,
		RetryPolicy: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}},
		hooks{sleep: (&fakeSleeper{}).sleep})

	g := newGate(1)
	_ = p.TrySubmit(g.job())
	recv(t, g.started, "worker busy")
	_ = p.TrySubmit(JobFunc(func(context.Context) error { return nil }))
	for i := 0; i < 5; i++ { // five rejections, one warning
		if err := p.TrySubmit(JobFunc(func(context.Context) error { return nil })); !errors.Is(err, ErrQueueFull) {
			t.Fatal(err)
		}
	}
	g.open()
	h, _ := p.SubmitTask(context.Background(), Task{ID: "flaky", Job: JobFunc(func(context.Context) error { return errors.New("secret-payload-xyz") })})
	h.Wait(context.Background())
	pn, _ := p.SubmitTask(context.Background(), Task{ID: "panicky", Job: JobFunc(func(context.Context) error { panic("oops") })})
	pn.Wait(context.Background())
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-p.Done()

	out := buf.String()
	if n := strings.Count(out, "queue saturated"); n != 1 {
		t.Errorf("saturation warning logged %d times, want exactly 1\n%s", n, out)
	}
	for _, want := range []string{
		"level=INFO msg=\"worker pool started\"", "level=INFO msg=\"shutdown initiated\"", "level=INFO msg=\"worker pool stopped\"",
		"level=DEBUG msg=\"job submitted\"", "level=DEBUG msg=\"job started\"",
		"level=WARN msg=\"job attempt failed, will retry\"",
		"level=ERROR msg=\"job failed\"", "level=ERROR msg=\"job panicked\"",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %s", want)
		}
	}
}

func TestCancelErrorWithoutCause(t *testing.T) {
	if err := cancelError(context.Background(), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
