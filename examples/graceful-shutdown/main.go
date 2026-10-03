// Command graceful-shutdown demonstrates both outcomes of Shutdown: a clean
// drain within the deadline, and an abort when the deadline is too short.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/example/concurrent-worker-pool/workerpool"
)

func slowJob(d time.Duration) workerpool.Job {
	return workerpool.JobFunc(func(ctx context.Context) error {
		select {
		case <-time.After(d):
			return nil
		case <-ctx.Done():
			return context.Cause(ctx) // cooperative: stop when told to
		}
	})
}

func run(name string, jobTime, deadline time.Duration) {
	fmt.Printf("== %s\n", name)
	pool, err := workerpool.New(workerpool.Config{Workers: 2, QueueSize: 4})
	if err != nil {
		log.Fatal(err)
	}
	var handles []*workerpool.Handle
	for i := 0; i < 6; i++ { // 2 running + 4 queued
		h, err := pool.SubmitTask(context.Background(), workerpool.Task{Job: slowJob(jobTime)})
		if err != nil {
			log.Fatal(err)
		}
		handles = append(handles, h)
	}

	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	err = pool.Shutdown(ctx)
	switch {
	case err == nil:
		fmt.Println("Shutdown: nil (drained)")
	case errors.Is(err, workerpool.ErrShutdownTimeout):
		fmt.Println("Shutdown: deadline exceeded, pool aborted")
	default:
		log.Fatal(err)
	}
	<-pool.Done() // wait for the workers to actually exit

	counts := map[workerpool.Outcome]int{}
	for _, h := range handles {
		res, _ := h.Wait(context.Background())
		counts[res.Outcome]++
	}
	fmt.Printf("outcomes: succeeded=%d canceled=%d final state=%s\n", counts[workerpool.Succeeded], counts[workerpool.Canceled], pool.State())
}

func main() {
	run("generous deadline: all 6 jobs drain", 10*time.Millisecond, 5*time.Second)
	run("tight deadline: running jobs canceled, queued jobs discarded", 5*time.Second, 50*time.Millisecond)
}
