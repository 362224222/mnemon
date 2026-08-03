package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
)

type handlerTracker struct {
	mu       sync.Mutex
	active   int
	stopping bool
	drained  chan struct{}
}

func newHandlerTracker() *handlerTracker {
	return &handlerTracker{drained: make(chan struct{})}
}

func (tracker *handlerTracker) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !tracker.start() {
			http.Error(writer, "shutting down", http.StatusServiceUnavailable)
			return
		}
		defer tracker.finish()
		next.ServeHTTP(writer, request)
	})
}

func (tracker *handlerTracker) start() bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.stopping {
		return false
	}
	tracker.active++
	return true
}

func (tracker *handlerTracker) finish() {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.active--
	if tracker.stopping && tracker.active == 0 {
		close(tracker.drained)
	}
}

func (tracker *handlerTracker) stop() {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.stopping {
		return
	}
	tracker.stopping = true
	if tracker.active == 0 {
		close(tracker.drained)
	}
}

func (tracker *handlerTracker) wait(ctx context.Context) error {
	select {
	case <-tracker.drained:
		return nil
	case <-ctx.Done():
		return errors.New("active HTTP handlers did not stop within the shutdown budget")
	}
}

func (tracker *handlerTracker) idle() bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.active == 0
}
