package workerpool

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string // substring; empty means valid
	}{
		{"valid", Config{Workers: 1, QueueSize: 0}, ""},
		{"zero workers", Config{}, "Workers must be > 0"},
		{"negative workers", Config{Workers: -1}, "Workers must be > 0"},
		{"negative queue", Config{Workers: 1, QueueSize: -1}, "QueueSize must be >= 0"},
		{"negative timeout", Config{Workers: 1, JobTimeout: -time.Second}, "JobTimeout"},
		{"negative attempts", Config{Workers: 1, RetryPolicy: RetryPolicy{MaxAttempts: -1}}, "MaxAttempts"},
		{"max below initial", Config{Workers: 1, RetryPolicy: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Second, MaxBackoff: time.Millisecond}}, "MaxBackoff"},
		{"multiplier below one", Config{Workers: 1, RetryPolicy: RetryPolicy{MaxAttempts: 2, Multiplier: 0.5}}, "Multiplier"},
		{"jitter too large", Config{Workers: 1, RetryPolicy: RetryPolicy{MaxAttempts: 2, Jitter: 1.5}}, "Jitter"},
		{"default retry policy", Config{Workers: 1, RetryPolicy: DefaultRetryPolicy()}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tc.wantErr)
			}
			if _, nerr := New(tc.cfg); nerr == nil {
				t.Fatal("New accepted an invalid config")
			}
		})
	}
}

func TestConfigReportsAllProblems(t *testing.T) {
	err := Config{Workers: 0, QueueSize: -1}.Validate()
	if err == nil || !strings.Contains(err.Error(), "Workers") || !strings.Contains(err.Error(), "QueueSize") {
		t.Fatalf("expected both problems reported, got %v", err)
	}
}

func TestBackoffProgressionAndCap(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 10, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, Multiplier: 2}
	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := p.backoff(i+1, 0); got != w*time.Millisecond {
			t.Errorf("retry %d: got %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
	if got := p.backoff(100000, 0); got != time.Second {
		t.Errorf("overflowing exponent must clamp to MaxBackoff, got %v", got)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Second, MaxBackoff: time.Second, Multiplier: 2, Jitter: 0.25}
	if got := p.backoff(1, 0); got != time.Second {
		t.Errorf("r=0 must give the full delay, got %v", got)
	}
	if got := p.backoff(1, 0.999999); got < 750*time.Millisecond || got > 751*time.Millisecond {
		t.Errorf("r~1 must give ~75%% of the delay, got %v", got)
	}
	for i := 0; i < 1000; i++ {
		got := p.Backoff(3)
		if got < 750*time.Millisecond || got > time.Second {
			t.Fatalf("jittered delay %v outside [750ms, 1s]", got)
		}
	}
}

func TestRetryPolicyDefaults(t *testing.T) {
	n := RetryPolicy{MaxAttempts: 3}.normalized()
	if n.InitialBackoff != 100*time.Millisecond || n.MaxBackoff != 30*time.Second || n.Multiplier != 2 {
		t.Fatalf("unexpected defaults: %+v", n)
	}
	if got := (RetryPolicy{}).normalized().MaxAttempts; got != 1 {
		t.Fatalf("zero policy must mean one attempt, got %d", got)
	}
}

func TestShouldRetry(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 3}
	boom := errors.New("boom")
	if !p.ShouldRetry(boom, 1) || !p.ShouldRetry(boom, 2) {
		t.Error("attempts 1 and 2 of 3 should be retried")
	}
	if p.ShouldRetry(boom, 3) {
		t.Error("attempt 3 of 3 must not be retried")
	}
	if p.ShouldRetry(Permanent(boom), 1) {
		t.Error("permanent errors must not be retried")
	}
	if p.ShouldRetry(fmt.Errorf("wrapped: %w", Permanent(boom)), 1) {
		t.Error("wrapped permanent errors must not be retried")
	}
	if (RetryPolicy{}).ShouldRetry(boom, 1) {
		t.Error("zero policy must not retry")
	}
	custom := RetryPolicy{MaxAttempts: 3, Retryable: func(err error) bool { return errors.Is(err, boom) }}
	if !custom.ShouldRetry(boom, 1) || custom.ShouldRetry(errors.New("other"), 1) {
		t.Error("custom classifier not honored")
	}
}

func TestPermanent(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	base := errors.New("bad input")
	err := Permanent(base)
	if !errors.Is(err, base) || !IsPermanent(err) || err.Error() != base.Error() {
		t.Fatal("Permanent must wrap transparently")
	}
	if IsPermanent(base) {
		t.Fatal("plain error is not permanent")
	}
}
