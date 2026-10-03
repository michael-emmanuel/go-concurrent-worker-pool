package workerpool

import (
	"context"
	"runtime"
	"testing"
)

// A job calling runtime.Goexit kills its worker and is never reported (documented
// limitation), but shutdown must still complete because the worker is accounted for.
func TestGoexitShutdownStillCompletes(t *testing.T) {
	p := newTestPool(t, Config{Workers: 1, QueueSize: 1})
	if err := p.Submit(context.Background(), JobFunc(func(context.Context) error { runtime.Goexit(); return nil })); err != nil {
		t.Fatal(err)
	}
	p.Shutdown(context.Background())
	waitClosed(t, p.Done(), "done")
}
