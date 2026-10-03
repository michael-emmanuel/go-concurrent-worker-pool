// Command basic shows the smallest useful use of the pool: create it, submit
// jobs, wait for them, shut down.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/example/concurrent-worker-pool/workerpool"
)

func main() {
	pool, err := workerpool.New(workerpool.Config{Workers: 4, QueueSize: 16})
	if err != nil {
		log.Fatal(err)
	}

	var (
		mu      sync.Mutex
		squares = make(map[int]int)
	)
	for i := 1; i <= 10; i++ {
		err := pool.Submit(context.Background(), workerpool.JobFunc(func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			squares[i] = i * i
			return nil
		}))
		if err != nil {
			log.Fatalf("submit: %v", err)
		}
	}

	// Shutdown drains the queue and waits for every accepted job to finish.
	if err := pool.Shutdown(context.Background()); err != nil {
		log.Fatalf("shutdown: %v", err)
	}
	for i := 1; i <= 10; i++ {
		fmt.Printf("%d^2 = %d\n", i, squares[i])
	}
	s := pool.Stats()
	fmt.Printf("submitted=%d succeeded=%d\n", s.Submitted, s.Succeeded)
}
