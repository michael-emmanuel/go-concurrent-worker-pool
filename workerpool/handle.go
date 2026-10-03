package workerpool

import "context"

// Handle tracks one job submitted with SubmitTask or TrySubmitTask. It lets
// the submitter cancel that specific job and wait for its Result. A Handle is
// safe for concurrent use.
type Handle struct {
	id     string
	cancel context.CancelCauseFunc
	done   chan struct{}
	res    Result // written once, before done is closed
}

// ID returns the job's identifier.
func (h *Handle) ID() string { return h.id }

// Cancel cancels the job's context with cause ErrJobCanceled. If the job is
// still queued it will not run; if it is running (or backing off between
// retries) it is up to the job to observe its context. Cancel is idempotent
// and has no effect once the job has finished.
func (h *Handle) Cancel() { h.cancel(ErrJobCanceled) }

// Done returns a channel that is closed when the job reaches a terminal
// outcome. The observer has already been notified by then.
func (h *Handle) Done() <-chan struct{} { return h.done }

// Result returns the terminal Result and true, or a zero Result and false if
// the job has not finished.
func (h *Handle) Result() (Result, bool) {
	select {
	case <-h.done:
		return h.res, true
	default:
		return Result{}, false
	}
}

// Wait blocks until the job finishes or ctx ends. It returns ctx's error if
// ctx ends first; the job is not affected by that.
func (h *Handle) Wait(ctx context.Context) (Result, error) {
	select {
	case <-h.done:
		return h.res, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func (h *Handle) resolve(r Result) {
	h.res = r
	close(h.done)
}
