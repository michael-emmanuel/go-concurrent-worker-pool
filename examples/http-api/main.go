// Command http-api is the production scenario: HTTP handlers accept requests
// and hand slow work to a bounded pool. A full queue becomes HTTP 503 with a
// Retry-After header instead of unbounded memory growth or unbounded latency,
// and SIGTERM triggers a graceful drain.
//
// By default it runs a self-contained demo: it starts the server on a local
// port, fires a burst of requests larger than the pool can absorb, then shuts
// down. With -serve it runs until interrupted.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/michael-emmanuel/go-concurrent-worker-pool/workerpool"
)

// callDownstream stands in for an external API. It fails transiently on every
// third call so that retries are visible.
func callDownstream(ctx context.Context, n int64) error {
	select {
	case <-time.After(30 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if n%3 == 0 {
		return errors.New("downstream: 503")
	}
	return nil
}

type server struct {
	pool  *workerpool.Pool
	calls atomic.Int64
}

func (s *server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	// TrySubmit, never Submit: a handler must not park a goroutine (and a
	// client connection) behind a saturated queue. Shed load instead.
	err := s.pool.TrySubmit(workerpool.JobFunc(func(ctx context.Context) error {
		return callDownstream(ctx, s.calls.Add(1))
	}))
	switch {
	case err == nil:
		w.WriteHeader(http.StatusAccepted)
	case errors.Is(err, workerpool.ErrQueueFull):
		w.Header().Set("Retry-After", "1")
		http.Error(w, "busy, retry later", http.StatusServiceUnavailable)
	case errors.Is(err, workerpool.ErrClosed):
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
	default:
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func main() {
	serve := flag.Bool("serve", false, "run until SIGINT/SIGTERM instead of the self-contained demo")
	addr := flag.String("addr", "127.0.0.1:0", "listen address")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	// Size workers to the downstream's capacity, not to the request rate.
	pool, err := workerpool.New(workerpool.Config{
		Workers:     4,
		QueueSize:   8,
		RetryPolicy: workerpool.DefaultRetryPolicy(),
		JobTimeout:  2 * time.Second,
		Logger:      logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	s := &server{pool: pool}
	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", s.handleEnqueue)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	fmt.Printf("listening on %s\n", ln.Addr())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	if *serve {
		<-stop
	} else {
		burst("http://" + ln.Addr().String() + "/enqueue")
	}

	// Deployment termination order: stop taking HTTP traffic first, so no new
	// TrySubmit calls arrive, then drain the pool within a deadline.
	fmt.Println("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := pool.Shutdown(ctx); err != nil {
		log.Printf("pool shutdown: %v", err)
	}
	st := pool.Stats()
	fmt.Printf("pool: submitted=%d rejected=%d succeeded=%d failed=%d retries=%d\n",
		st.Submitted, st.Rejected, st.Succeeded, st.Failed, st.Retries)
}

// burst sends 60 concurrent requests at a pool that can hold 4 running plus 8
// queued jobs, and reports how the service responded.
func burst(url string) {
	const n = 60
	var wg sync.WaitGroup
	var accepted, shed atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(url)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusAccepted:
				accepted.Add(1)
			case http.StatusServiceUnavailable:
				shed.Add(1)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("burst of %d requests: %d accepted (202), %d shed (503)\n", n, accepted.Load(), shed.Load())
}
