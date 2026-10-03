package workerpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Config describes a Pool. Construct it as a struct literal and pass it to New.
//
// Defaults for zero values:
//
//	Workers      no default; must be > 0 (sizing is a decision the caller must make)
//	QueueSize    0, meaning an unbuffered hand-off: no job waits, a submission
//	             succeeds only when a worker is ready to take it
//	RetryPolicy  a single attempt, no retries
//	JobTimeout   none
//	Observer     NopObserver
//	Logger       discards all output
type Config struct {
	// Workers is the number of worker goroutines. It must be at least 1.
	Workers int

	// QueueSize is the capacity of the bounded queue. It must be >= 0.
	QueueSize int

	// RetryPolicy configures retries. See RetryPolicy for the zero value.
	RetryPolicy RetryPolicy

	// JobTimeout, if positive, bounds each attempt of each job. A Task may
	// override it. The attempt's context is canceled when it expires; the job
	// must observe the context for the timeout to have any effect.
	JobTimeout time.Duration

	// Observer receives lifecycle events. Nil means NopObserver.
	Observer Observer

	// Logger receives structured log records: pool start/stop and shutdown at
	// Info, submissions and job pickup at Debug, retries and queue saturation
	// at Warn, terminal failures and panics at Error. Job payloads are never
	// logged. Nil means logs are discarded.
	Logger *slog.Logger
}

// Validate reports every problem with the configuration, joined into one error.
func (c Config) Validate() error {
	var errs []error
	if c.Workers <= 0 {
		errs = append(errs, fmt.Errorf("Workers must be > 0, got %d", c.Workers))
	}
	if c.QueueSize < 0 {
		errs = append(errs, fmt.Errorf("QueueSize must be >= 0, got %d", c.QueueSize))
	}
	if c.JobTimeout < 0 {
		errs = append(errs, fmt.Errorf("JobTimeout must be >= 0, got %v", c.JobTimeout))
	}
	if err := c.RetryPolicy.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("RetryPolicy: %w", err))
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("workerpool: invalid config: %w", errors.Join(errs...))
}

// discardHandler is a slog.Handler that drops everything.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }
