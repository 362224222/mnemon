package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandlerTrackerRejectsNewWorkAndDrainsExistingWork(t *testing.T) {
	tracker := newHandlerTracker()
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	handler := tracker.wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	}))
	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		close(finished)
	}()
	<-started
	tracker.stop()

	rejected := httptest.NewRecorder()
	handler.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/", nil))
	if rejected.Code != http.StatusServiceUnavailable {
		t.Fatalf("handler after stop returned %d, want %d",
			rejected.Code, http.StatusServiceUnavailable)
	}
	close(release)
	<-finished
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tracker.wait(ctx); err != nil {
		t.Fatal(err)
	}
	if !tracker.idle() {
		t.Fatal("handler tracker did not become idle")
	}
}
