// Command backpressure shows what producers experience when the queue is full:
// TrySubmit sheds load with ErrQueueFull, and Submit with a deadline gives up
// instead of waiting forever.
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
	// 1 worker + queue of 2: at most 3 jobs can be in the pool at once.
	pool, err := workerpool.New(workerpool.Config{Workers: 1, QueueSize: 2})
	if err != nil {
		log.Fatal(err)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	blocker := workerpool.JobFunc(func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	})
	if err := pool.Submit(context.Background(), blocker); err != nil {
		log.Fatal(err)
	}
	<-started // the only worker is now busy

	quick := workerpool.JobFunc(func(context.Context) error { return nil })

	fmt.Println("-- TrySubmit: reject immediately when full")
	for i := 1; i <= 4; i++ {
		switch err := pool.TrySubmit(quick); {
		case err == nil:
			fmt.Printf("job %d accepted (queue depth %d/%d)\n", i, pool.Stats().QueueDepth, pool.Stats().QueueCapacity)
		case errors.Is(err, workerpool.ErrQueueFull):
			fmt.Printf("job %d rejected: queue full\n", i)
		default:
			log.Fatal(err)
		}
	}

	fmt.Println("-- Submit with a deadline: wait a bounded time for space")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = pool.Submit(ctx, quick)
	fmt.Printf("Submit returned: %v (deadline exceeded: %t)\n", err, errors.Is(err, context.DeadlineExceeded))

	fmt.Println("-- Release the worker; the queue drains")
	close(release)
	if err := pool.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	s := pool.Stats()
	fmt.Printf("submitted=%d rejected=%d succeeded=%d\n", s.Submitted, s.Rejected, s.Succeeded)
}
