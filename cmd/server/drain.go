package main

import (
	"context"
	"net/http"
	"strings"
	"sync"
)

// requestDrain tracks handlers inside h2c, whose hijacked connections are not
// waited for by http.Server.Shutdown. Admission and draining share one lock.
type requestDrain struct {
	next    http.Handler
	mu      sync.Mutex
	closing bool
	active  int
	drained chan struct{}
}

func newRequestDrain(next http.Handler) *requestDrain {
	return &requestDrain{next: next, drained: make(chan struct{})}
}

func (d *requestDrain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			w.Header().Set("Content-Type", "application/grpc")
			w.Header().Set("Grpc-Status", "14") // UNAVAILABLE, trailers-only response.
			w.Header().Set("Grpc-Message", "server is shutting down")
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		}
		return
	}
	d.active++
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.active--
		if d.closing && d.active == 0 {
			close(d.drained)
		}
		d.mu.Unlock()
	}()
	d.next.ServeHTTP(w, r)
}

func (d *requestDrain) beginDrain() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing {
		return
	}
	d.closing = true
	if d.active == 0 {
		close(d.drained)
	}
}

func (d *requestDrain) wait(ctx context.Context) error {
	select {
	case <-d.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
