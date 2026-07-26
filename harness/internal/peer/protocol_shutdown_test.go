package peer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProtocolCloseContextBoundsBlockedHandlerDrainAndAllowsLaterJoin(t *testing.T) {
	tests := []struct {
		name  string
		open  func() protocolShutdownFixture
		owner error
	}{
		{name: "Channel dispatcher", open: newChannelDispatcherShutdownFixture,
			owner: ErrChannelDispatcher},
		{name: "Event server", open: newEventServerShutdownFixture, owner: ErrEventServer},
		{name: "Artifact server", open: newArtifactServerShutdownFixture,
			owner: ErrArtifactServer},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := test.open()
			handlerStarted := make(chan bool, 1)
			handlerRelease := make(chan struct{})
			handlerDone := make(chan struct{})
			go func() {
				admitted := fixture.begin()
				handlerStarted <- admitted
				if admitted {
					<-handlerRelease
					fixture.finish()
				}
				close(handlerDone)
			}()
			if !<-handlerStarted {
				t.Fatal("fixture did not admit the blocked handler")
			}
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			err := fixture.closeContext(shutdownCtx)
			cancel()
			if !errors.Is(err, test.owner) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("CloseContext() error = %v, want owner and deadline diagnostics", err)
			}
			select {
			case <-fixture.stopped:
			default:
				t.Fatal("CloseContext() did not broadcast handler cancellation before waiting")
			}
			if fixture.begin() {
				t.Fatal("CloseContext() admitted a handler after sealing")
			}

			close(handlerRelease)
			select {
			case <-handlerDone:
			case <-time.After(time.Second):
				t.Fatal("released handler did not join")
			}
			if err := fixture.close(); err != nil {
				t.Fatalf("Close() after handler release error = %v", err)
			}
		})
	}
}

func TestProtocolCloseContextHandlesNilAlreadyDrainedAndRepeatedCalls(t *testing.T) {
	tests := []struct {
		name  string
		open  func() protocolShutdownFixture
		owner error
	}{
		{name: "Channel dispatcher", open: newChannelDispatcherShutdownFixture,
			owner: ErrChannelDispatcher},
		{name: "Event server", open: newEventServerShutdownFixture, owner: ErrEventServer},
		{name: "Artifact server", open: newArtifactServerShutdownFixture,
			owner: ErrArtifactServer},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := test.open()
			if err := fixture.closeContext(nil); !errors.Is(err, test.owner) {
				t.Fatalf("CloseContext(nil) error = %v, want owner diagnostic", err)
			}
			select {
			case <-fixture.stopped:
				t.Fatal("nil shutdown context sealed the live owner")
			default:
			}
			if err := fixture.closeContext(context.Background()); err != nil {
				t.Fatalf("already-drained CloseContext() error = %v", err)
			}
			select {
			case <-fixture.stopped:
			default:
				t.Fatal("already-drained CloseContext() did not broadcast cancellation")
			}
			if err := fixture.closeContext(context.Background()); err != nil {
				t.Fatalf("repeated CloseContext() error = %v", err)
			}
		})
	}

	var dispatcher *ChannelDispatcher
	var events *EventServer
	var artifacts *ArtifactServer
	if err := dispatcher.CloseContext(nil); err != nil {
		t.Fatalf("nil Channel dispatcher CloseContext() error = %v", err)
	}
	if err := events.CloseContext(nil); err != nil {
		t.Fatalf("nil Event server CloseContext() error = %v", err)
	}
	if err := artifacts.CloseContext(nil); err != nil {
		t.Fatalf("nil Artifact server CloseContext() error = %v", err)
	}
}

type protocolShutdownFixture struct {
	begin        func() bool
	finish       func()
	closeContext func(context.Context) error
	close        func() error
	stopped      <-chan struct{}
}

func newChannelDispatcherShutdownFixture() protocolShutdownFixture {
	lifetime, cancel := context.WithCancel(context.Background())
	dispatcher := &ChannelDispatcher{ctx: lifetime, cancel: cancel}
	return protocolShutdownFixture{begin: dispatcher.begin, finish: dispatcher.finishHandler,
		closeContext: dispatcher.CloseContext, close: dispatcher.Close, stopped: lifetime.Done()}
}

func newEventServerShutdownFixture() protocolShutdownFixture {
	lifetime, cancel := context.WithCancel(context.Background())
	server := &EventServer{ctx: lifetime, cancel: cancel}
	return protocolShutdownFixture{begin: server.begin, finish: server.finishHandler,
		closeContext: server.CloseContext, close: server.Close, stopped: lifetime.Done()}
}

func newArtifactServerShutdownFixture() protocolShutdownFixture {
	lifetime, cancel := context.WithCancel(context.Background())
	server := &ArtifactServer{ctx: lifetime, cancel: cancel}
	return protocolShutdownFixture{begin: server.begin, finish: server.finishHandler,
		closeContext: server.CloseContext, close: server.Close, stopped: lifetime.Done()}
}
