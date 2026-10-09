// Command cancellation shows the places a context matters: a blocked Submit,
// a running job, a retry backoff, and a per-attempt timeout.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/michael-emmanuel/go-concurrent-worker-pool/workerpool"
)

func main() {
	pool, err := workerpool.New(workerpool.Config{
		Workers: 1, QueueSize: 0,
		RetryPolicy: workerpool.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Hour, MaxBackoff: time.Hour},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// 1. A long-running job that only ends when its context does.
	running := make(chan struct{})
	h, err := pool.SubmitTask(context.Background(), workerpool.Task{ID: "long", Job: workerpool.JobFunc(func(ctx context.Context) error {
		close(running)
		<-ctx.Done()
		return ctx.Err()
	})})
	if err != nil {
		log.Fatal(err)
	}
	<-running

	// 2. While that job occupies the only worker, a Submit with a deadline
	//    gives up rather than waiting indefinitely.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = pool.Submit(ctx, workerpool.JobFunc(func(context.Context) error { return nil }))
	fmt.Printf("blocked Submit: %v\n", err)

	// 3. Per-job cancellation of the running job through its Handle.
	h.Cancel()
	res, _ := h.Wait(context.Background())
	fmt.Printf("running job:    outcome=%s job-canceled-cause=%t\n", res.Outcome, errors.Is(res.Err, workerpool.ErrJobCanceled))

	// 4. Cancel during retry backoff: the 1h backoff is interrupted at once.
	failing, err := pool.SubmitTask(context.Background(), workerpool.Task{ID: "backoff", Job: workerpool.JobFunc(func(context.Context) error {
		return errors.New("transient")
	})})
	if err != nil {
		log.Fatal(err)
	}
	for pool.Stats().Retries == 0 { // wait until the worker has entered backoff
		time.Sleep(time.Millisecond)
	}
	failing.Cancel()
	res, _ = failing.Wait(context.Background())
	fmt.Printf("backoff job:    outcome=%s attempts=%d\n", res.Outcome, res.Attempts)

	// 5. Per-attempt timeout: the job's context expires by itself.
	timed, err := pool.SubmitTask(context.Background(), workerpool.Task{ID: "timeout", Timeout: 20 * time.Millisecond, Job: workerpool.JobFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return workerpool.Permanent(ctx.Err()) // do not retry a timeout in this demo
	})})
	if err != nil {
		log.Fatal(err)
	}
	res, _ = timed.Wait(context.Background())
	fmt.Printf("timed-out job:  outcome=%s deadline=%t\n", res.Outcome, errors.Is(res.Err, context.DeadlineExceeded))
}
