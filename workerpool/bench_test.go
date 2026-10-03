package workerpool

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

func noop(context.Context) error { return nil }

// BenchmarkSubmit measures the cost of Submit plus execution of a no-op job
// through the pool, across worker counts and queue sizes. Each iteration is
// one job; the pool is drained before the timer stops so all work is counted.
func BenchmarkSubmit(b *testing.B) {
	for _, workers := range []int{1, 4, 16} {
		for _, queue := range []int{0, 16, 1024} {
			b.Run(fmt.Sprintf("workers=%d/queue=%d", workers, queue), func(b *testing.B) {
				p, _ := New(Config{Workers: workers, QueueSize: queue})
				job := JobFunc(noop)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = p.Submit(ctx, job)
				}
				_ = p.Shutdown(ctx)
			})
		}
	}
}

// BenchmarkSubmitParallel measures contention among many producers.
func BenchmarkSubmitParallel(b *testing.B) {
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			p, _ := New(Config{Workers: workers, QueueSize: 256})
			job := JobFunc(noop)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = p.Submit(ctx, job)
				}
			})
			_ = p.Shutdown(ctx)
		})
	}
}

// BenchmarkExecuteCPU runs a small fixed CPU-bound job, so the result reflects
// scheduling and synchronization overhead relative to real work.
func BenchmarkExecuteCPU(b *testing.B) {
	var sink atomic.Uint64
	work := JobFunc(func(context.Context) error {
		var x uint64 = 1
		for i := 0; i < 1000; i++ {
			x = x*6364136223846793005 + 1442695040888963407
		}
		sink.Add(x)
		return nil
	})
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			p, _ := New(Config{Workers: workers, QueueSize: 256})
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = p.Submit(ctx, work)
			}
			_ = p.Shutdown(ctx)
		})
	}
}

// BenchmarkTrySubmitFull measures the rejection fast path on a saturated pool.
func BenchmarkTrySubmitFull(b *testing.B) {
	p, _ := New(Config{Workers: 1, QueueSize: 1})
	block := make(chan struct{})
	started := make(chan struct{})
	_ = p.Submit(context.Background(), JobFunc(func(context.Context) error { close(started); <-block; return nil }))
	<-started
	_ = p.TrySubmit(JobFunc(noop))
	job := JobFunc(noop)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.TrySubmit(job)
	}
	b.StopTimer()
	close(block)
	_ = p.Shutdown(context.Background())
}
